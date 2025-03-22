import os
import json
import time
import numpy as np
import pandas as pd
import requests
from threading import Lock, Thread
from kafka import KafkaProducer
import signal
import sys

# Import from other modules
from config import (
    OANDA_API_KEY, 
    OANDA_ACCOUNT_ID,
    OANDA_STREAM_URL,
    INSTRUMENTS,
    CURRENCIES,
    BASE_CURRENCY,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MATRIX_UPDATE_INTERVAL_SECONDS,
    HEALTH_METRICS_INTERVAL_UPDATES,
    API_REQUEST_TIMEOUT,
    MAX_FAILURES,
    COOLDOWN_PERIOD,
    DATA_DIR,
    setup_logging
)
from matrix_builder import build_exchange_matrix, check_arbitrage

# Set up logging
logger = setup_logging("stream_matrix")

# Global state with locks for thread safety
live_rates = {}
rates_lock = Lock()

# Metrics for system health monitoring
metrics = {
    "last_update_time": None,
    "update_count": 0,
    "connection_attempts": 0,
    "start_time": time.time(),
    "last_matrix_time": 0,
    "successful_matrix_updates": 0,
    "arbitrage_opportunities": 0
}
metrics_lock = Lock()

# Kafka producer setup
kafka_available = False
producer = None

def initialize_kafka():
    """Initialize Kafka producer with proper error handling"""
    global kafka_available, producer
    
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            compression_type='gzip',  # Add compression for better performance
            acks='all',               # Wait for all replicas to acknowledge
            retries=5,                # Retry failed sends
            linger_ms=50              # Small delay to batch messages
        )
        kafka_available = True
        logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        
        # Test the connection with a simple message
        health_msg = {
            "type": "HEALTH",
            "timestamp": time.time(),
            "status": "STARTING"
        }
        producer.send(KAFKA_TOPIC, health_msg)
        producer.flush()  # Make sure the message is sent
        logger.info("Kafka test message sent successfully")
        
    except Exception as e:
        logger.warning(f"Failed to connect to Kafka: {e}")
        kafka_available = False

class CircuitBreaker:
    """Implements the circuit breaker pattern for API calls"""
    def __init__(self, max_failures=MAX_FAILURES, cooldown_period=COOLDOWN_PERIOD):
        self.max_failures = max_failures
        self.cooldown_period = cooldown_period
        self.failures = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN
        self.lock = Lock()
    
    def record_failure(self):
        with self.lock:
            self.failures += 1
            self.last_failure_time = time.time()
            
            if self.failures >= self.max_failures and self.state == "CLOSED":
                self.state = "OPEN"
                logger.warning(f"Circuit breaker OPEN after {self.failures} failures")
    
    def record_success(self):
        with self.lock:
            if self.state == "HALF-OPEN":
                self.state = "CLOSED"
                self.failures = 0
                logger.info("Circuit breaker reset to CLOSED after successful call")
            elif self.state == "CLOSED":
                self.failures = max(0, self.failures - 1)  # Gradually reduce failure count
    
    def can_execute(self):
        with self.lock:
            if self.state == "CLOSED":
                return True
            
            if self.state == "OPEN":
                # Check if cooldown period has elapsed
                if time.time() - self.last_failure_time > self.cooldown_period:
                    logger.info("Circuit breaker trying HALF-OPEN state after cooldown")
                    self.state = "HALF-OPEN"
                    return True
                return False
            
            # HALF-OPEN state allows one test request
            return True
    
    def get_state(self):
        with self.lock:
            return {
                "state": self.state,
                "failures": self.failures,
                "last_failure": self.last_failure_time
            }

# Create circuit breaker instance
circuit = CircuitBreaker()

def log_system_metrics():
    """Log system health metrics"""
    with metrics_lock:
        current_metrics = metrics.copy()
    
    uptime = time.time() - current_metrics["start_time"]
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    logger.info(f"System health metrics:")
    logger.info(f"Uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    logger.info(f"Total updates: {current_metrics['update_count']}")
    logger.info(f"Updates per minute: {current_metrics['update_count'] / (uptime / 60):.2f}")
    logger.info(f"Connection attempts: {current_metrics['connection_attempts']}")
    logger.info(f"Matrix updates: {current_metrics['successful_matrix_updates']}")
    logger.info(f"Arbitrage opportunities: {current_metrics['arbitrage_opportunities']}")
    logger.info(f"Circuit breaker: {circuit.get_state()['state']}")
    
    if current_metrics["last_update_time"]:
        time_since_update = time.time() - current_metrics["last_update_time"]
        logger.info(f"Time since last update: {time_since_update:.2f}s")
    
    # Send health metrics to Kafka if available
    if kafka_available:
        try:
            health_msg = {
                "type": "HEALTH",
                "timestamp": time.time(),
                "metrics": {
                    "uptime": uptime,
                    "updates": current_metrics["update_count"],
                    "updates_per_minute": current_metrics['update_count'] / (uptime / 60) if uptime > 0 else 0,
                    "connection_attempts": current_metrics["connection_attempts"],
                    "matrix_updates": current_metrics["successful_matrix_updates"],
                    "arbitrage_opportunities": current_metrics["arbitrage_opportunities"],
                    "circuit_breaker": circuit.get_state()
                }
            }
            producer.send(KAFKA_TOPIC, health_msg)
        except Exception as e:
            logger.error(f"Failed to send health metrics to Kafka: {e}")

def update_rate(price_info):
    """
    Parse a price tick from OANDA and update the live_rates dict.
    Thread-safe implementation.
    """
    instrument = price_info.get("instrument")
    if instrument is None:
        return
    
    # Get bid and ask prices from the tick
    try:
        bid = float(price_info['bids'][0]['price'])
        ask = float(price_info['asks'][0]['price'])
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error parsing price data: {e}")
        return

    # Compute mid-price
    mid_price = (bid + ask) / 2.0
    
    # Validate rate (sanity check)
    with rates_lock:
        if instrument in live_rates:
            previous_rate = live_rates[instrument]
            # Check for suspiciously large changes (e.g., more than 5%)
            if abs(mid_price - previous_rate) / previous_rate > 0.05:
                logger.warning(f"Suspicious rate change for {instrument}: {previous_rate} -> {mid_price} ({(mid_price - previous_rate) / previous_rate * 100:.2f}%)")
    
    # Use locks to safely update shared state
    with rates_lock:
        live_rates[instrument] = mid_price
    
    with metrics_lock:
        metrics["last_update_time"] = time.time()
        metrics["update_count"] += 1
        should_log = metrics["update_count"] % HEALTH_METRICS_INTERVAL_UPDATES == 0
    
    # Log tick data
    logger.info(f"Updated {instrument}: {mid_price} (bid: {bid}, ask: {ask})")
    
    # Send to Kafka if available
    if kafka_available:
        # Add timestamp and mid price to the data
        price_info["mid_price"] = mid_price
        price_info["local_timestamp"] = time.time()
        try:
            producer.send(KAFKA_TOPIC, price_info)
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
    
    # Log system metrics periodically
    if should_log:
        log_system_metrics()

def build_and_log_matrix():
    """Build the exchange rate matrix and log it"""
    # Safely copy the current rates to avoid locks during processing
    with rates_lock:
        current_rates = live_rates.copy()
    
    # Check if we have rates for all instruments
    missing_instruments = set(INSTRUMENTS) - set(k for k in current_rates.keys())
    if missing_instruments:
        logger.warning(f"Not all instruments updated yet. Missing: {missing_instruments}")
        return None
    
    try:
        # Build the matrix using rates relative to USD
        matrix = build_exchange_matrix(current_rates, CURRENCIES, base_currency=BASE_CURRENCY)
        
        # Check for arbitrage opportunities
        opportunities = check_arbitrage(matrix)
        
        with metrics_lock:
            metrics["successful_matrix_updates"] += 1
            metrics["arbitrage_opportunities"] += len(opportunities)
        
        logger.info("Updated Exchange Rate Log Matrix:")
        logger.info("\n" + str(matrix))
        
        # Log arbitrage opportunities
        if opportunities:
            logger.info(f"Found {len(opportunities)} arbitrage opportunities")
            for opp in opportunities:
                logger.info(f"  {opp['path']}: {opp['profit_percent']:.4f}%")
        
        # Send matrix to Kafka if available
        if kafka_available:
            try:
                matrix_dict = {
                    "type": "MATRIX",
                    "timestamp": time.time(),
                    "matrix": matrix.to_dict(),
                    "arbitrage_opportunities": opportunities
                }
                producer.send(KAFKA_TOPIC, matrix_dict)
            except Exception as e:
                logger.error(f"Failed to send matrix to Kafka: {e}")
        
        # Save matrix snapshot periodically (every 100 updates)
        with metrics_lock:
            if metrics["successful_matrix_updates"] % 100 == 0:
                save_matrix_snapshot(matrix)
        
        return matrix
        
    except Exception as e:
        logger.error(f"Error building exchange rate matrix: {e}")
        return None

def save_matrix_snapshot(matrix, directory=DATA_DIR):
    """Save the current exchange rate matrix to disk"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    filename = f"exchange_matrix_{timestamp}.csv"
    filepath = os.path.join(directory, filename)
    
    try:
        matrix.to_csv(filepath)
        logger.info(f"Matrix snapshot saved to {filepath}")
    except Exception as e:
        logger.error(f"Failed to save matrix snapshot: {e}")
    
    return filepath

def stream_prices():
    """
    Non-recursive implementation of price streaming with exponential backoff
    for reconnection attempts and circuit breaker pattern.
    """
    if not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        logger.error("OANDA_API_KEY and OANDA_ACCOUNT_ID must be set in environment")
        return

    instruments_query = ",".join(INSTRUMENTS)
    stream_url = f"{OANDA_STREAM_URL}/accounts/{OANDA_ACCOUNT_ID}/pricing/stream?instruments={instruments_query}"
    headers = {"Authorization": f"Bearer {OANDA_API_KEY}"}
    
    reconnect_delay = 1  # Initial delay in seconds
    max_reconnect_delay = 300  # Maximum delay (5 minutes)
    
    while True:
        # Check if we should execute based on circuit breaker
        if not circuit.can_execute():
            logger.warning("Circuit breaker open, waiting before reconnecting")
            time.sleep(60)  # Wait a minute before checking again
            continue
        
        with metrics_lock:
            metrics["connection_attempts"] += 1
            connection_attempt = metrics["connection_attempts"]
            
        logger.info(f"Connecting to OANDA streaming endpoint (attempt {connection_attempt})...")
        
        try:
            with requests.get(
                stream_url, 
                headers=headers, 
                stream=True, 
                timeout=API_REQUEST_TIMEOUT
            ) as response:
                if response.status_code != 200:
                    error_msg = f"Error connecting to stream: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    circuit.record_failure()
                    raise requests.RequestException(error_msg)
                    
                logger.info("Connected. Streaming tick data now...")
                circuit.record_success()
                reconnect_delay = 1  # Reset delay on successful connection
                
                last_matrix_time = 0
                
                for line in response.iter_lines():
                    if line:
                        try:
                            data = json.loads(line.decode("utf-8"))
                            
                            # Process heartbeats to keep connection alive
                            if data.get("type") == "HEARTBEAT":
                                logger.debug(f"Received heartbeat at {data.get('time')}")
                                continue
                            
                            # Process price ticks
                            if data.get("type") == "PRICE":
                                update_rate(data)
                                
                                # Update matrix periodically
                                current_time = time.time()
                                if current_time - last_matrix_time >= MATRIX_UPDATE_INTERVAL_SECONDS:
                                    build_and_log_matrix()
                                    last_matrix_time = current_time
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Received non-JSON line: {e}")
                        
        except requests.RequestException as e:
            logger.error(f"Stream error: {e}")
            circuit.record_failure()
            
            # Implement exponential backoff with jitter
            jitter = 0.1 * reconnect_delay * (np.random.random() - 0.5)
            actual_delay = min(reconnect_delay + jitter, max_reconnect_delay)
            
            logger.info(f"Reconnecting in {actual_delay:.2f} seconds...")
            time.sleep(actual_delay)
            
            # Increase delay for next time, but cap it
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

def handle_shutdown(signum, frame):
    """Handle graceful shutdown"""
    logger.info("Shutdown signal received, exiting...")
    
    if kafka_available and producer:
        # Send final status message
        try:
            shutdown_msg = {
                "type": "SYSTEM",
                "timestamp": time.time(),
                "status": "SHUTDOWN",
                "reason": "User requested shutdown"
            }
            producer.send(KAFKA_TOPIC, shutdown_msg)
            producer.flush()  # Make sure all messages are sent
            producer.close(timeout=5)
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error during Kafka shutdown: {e}")
    
    # Log final metrics
    log_system_metrics()
    
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    # Print startup banner
    logger.info("=" * 80)
    logger.info("Starting FX rate streaming service")
    logger.info(f"Monitoring {len(INSTRUMENTS)} instruments: {', '.join(INSTRUMENTS)}")
    logger.info(f"Using {len(CURRENCIES)} currencies: {', '.join(CURRENCIES)}")
    logger.info(f"Base currency: {BASE_CURRENCY}")
    logger.info("=" * 80)
    
    # Initialize Kafka
    initialize_kafka()
    
    try:
        # Start streaming process
        stream_prices()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down")
        handle_shutdown(None, None)
    except Exception as e:
        logger.critical(f"Unhandled exception: {e}", exc_info=True)
        if kafka_available and producer:
            try:
                error_msg = {
                    "type": "SYSTEM",
                    "timestamp": time.time(),
                    "status": "ERROR",
                    "error": str(e)
                }
                producer.send(KAFKA_TOPIC, error_msg)
                producer.flush()
            except:
                pass
        sys.exit(1)