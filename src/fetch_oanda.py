import os
import numpy as np
import requests
import pandas as pd
import logging
import time
from kafka import KafkaProducer
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('fetch_oanda')

# Kafka producer setup
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "oanda-ticks")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    kafka_available = True
    logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.warning(f"Failed to connect to Kafka: {e}")
    kafka_available = False

def fetch_oanda_rates(instruments, max_retries=3, retry_delay=2):
    """
    Fetches mid-prices for a list of currency pairs from OANDA's REST API.
    Returns a dict { 'USD_EUR': 0.90, ... } of effective rates.
    
    Parameters:
    -----------
    instruments : list
        List of instrument names, e.g. ["EUR_USD", "USD_JPY"]
    max_retries : int
        Maximum number of retry attempts on failure
    retry_delay : int
        Initial delay between retries in seconds (doubles with each retry)
    
    Returns:
    --------
    dict
        Dictionary mapping instrument names to mid-prices
    """
    OANDA_API_KEY = os.environ.get("OANDA_API_KEY")
    OANDA_ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID")
    OANDA_URL = "https://api-fxpractice.oanda.com/v3"

    # Verify required environment variables
    if not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        logger.error("OANDA_API_KEY and OANDA_ACCOUNT_ID must be set in environment")
        return {}

    headers = {
        "Authorization": f"Bearer {OANDA_API_KEY}"
    }

    # Build the instruments query, e.g. "EUR_USD,USD_JPY,GBP_USD"
    inst_str = ",".join(instruments)
    endpoint = f"/accounts/{OANDA_ACCOUNT_ID}/pricing?instruments={inst_str}"
    
    current_retry = 0
    current_delay = retry_delay
    
    while current_retry <= max_retries:
        try:
            logger.info(f"Fetching rates for {len(instruments)} instruments")
            resp = requests.get(OANDA_URL + endpoint, headers=headers, timeout=10)
            resp.raise_for_status()
            
            data = resp.json()
            prices = data.get("prices", [])
            
            if not prices:
                logger.warning("API returned no prices")
                
            rate_dict = {}
            for p in prices:
                instrument = p["instrument"]  # e.g. "EUR_USD"
                # Use mid price
                bid = float(p['bids'][0]['price'])
                ask = float(p['asks'][0]['price'])
                mid_price = (bid + ask) / 2
                rate_dict[instrument] = mid_price
                
                # Log to Kafka if available
                if kafka_available:
                    # Add timestamp to the price data
                    p["timestamp"] = time.time()
                    p["mid_price"] = mid_price
                    producer.send(KAFKA_TOPIC, p)
            
            # If we have Kafka, also send a consolidated message with all rates
            if kafka_available and rate_dict:
                consolidated_msg = {
                    "type": "CONSOLIDATED_RATES",
                    "timestamp": time.time(),
                    "rates": rate_dict
                }
                producer.send(KAFKA_TOPIC, consolidated_msg)
                
            logger.info(f"Successfully fetched {len(rate_dict)} rates")
            return rate_dict
            
        except requests.exceptions.RequestException as e:
            current_retry += 1
            logger.error(f"API request error (attempt {current_retry}/{max_retries}): {e}")
            
            if current_retry > max_retries:
                logger.error("Max retries exceeded, giving up")
                return {}
                
            logger.info(f"Retrying in {current_delay} seconds")
            time.sleep(current_delay)
            current_delay *= 2  # Exponential backoff
            
        except (ValueError, KeyError, IndexError) as e:
            logger.error(f"Data parsing error: {e}")
            return {}

# Function to fetch rates and build matrix in one go
def fetch_and_build_matrix(instruments, currencies, base_currency="USD"):
    """
    Fetches rates and builds a log exchange rate matrix.
    Also sends the matrix to Kafka if available.
    """
    from matrix_builder import build_exchange_matrix
    
    rate_dict = fetch_oanda_rates(instruments)
    
    if not rate_dict:
        logger.warning("No rates fetched, cannot build matrix")
        return None
        
    matrix = build_exchange_matrix(rate_dict, currencies, base_currency)
    
    # Send matrix to Kafka if available
    if kafka_available:
        matrix_dict = {
            "type": "MATRIX",
            "timestamp": time.time(),
            "matrix": matrix.to_dict()
        }
        producer.send(KAFKA_TOPIC, matrix_dict)
        
    return matrix

if __name__ == "__main__":
    # Example usage
    instruments = ["EUR_USD", "USD_JPY", "GBP_USD", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"]
    rates = fetch_oanda_rates(instruments)
    print("Fetched rates:", rates)