import os
import logging
from logging.handlers import RotatingFileHandler
import json

# Directory configuration
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
LOG_DIR = os.path.join(ROOT_DIR, "logs")

# Update paths and settings for Docker environment
if os.environ.get("RUNNING_IN_DOCKER", "false").lower() == "true":
    # In Docker, our working directory is /app
    ROOT_DIR = "/app"
    DATA_DIR = "/app/data"
    LOG_DIR = "/app/logs"
    
    # Update Kafka settings for Docker environment
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    # Set host for dashboard to allow external access
    DASHBOARD_HOST = "0.0.0.0"

# Ensure directories exist
for directory in [DATA_DIR, LOG_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# OANDA API Configuration
OANDA_API_KEY = os.environ.get("OANDA_API_KEY")
OANDA_ACCOUNT_ID = os.environ.get("OANDA_ACCOUNT_ID")
OANDA_REST_URL = "https://api-fxpractice.oanda.com/v3"
OANDA_STREAM_URL = "https://stream-fxpractice.oanda.com/v3"

# Forex Instruments and Currencies
INSTRUMENTS = ["EUR_USD", "USD_JPY", "GBP_USD", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"]
CURRENCIES = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "NZD"]
BASE_CURRENCY = "USD"

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "oanda-ticks")

# Timing Parameters
SCHEDULER_INTERVAL_MINUTES = 1
MATRIX_UPDATE_INTERVAL_SECONDS = 10
HEALTH_METRICS_INTERVAL_UPDATES = 100

# API Parameters
API_REQUEST_TIMEOUT = 10  # seconds
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 2  # seconds

# Circuit Breaker Parameters
MAX_FAILURES = 5
COOLDOWN_PERIOD = 300  # seconds (5 minutes)

# Dashboard Configuration
DASHBOARD_HOST = "0.0.0.0"
DASHBOARD_PORT = 8050
DASHBOARD_DEBUG = False

# Setup logging
def setup_logging(name, level=logging.INFO):
    """Configure logging with rotating file handler and console output"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Clear any existing handlers
    if logger.handlers:
        logger.handlers = []
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # Create file handler
    log_file = os.path.join(LOG_DIR, f"{name}.log")
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setLevel(level)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger

# Function to save configuration for reference
def save_current_config():
    """Save current configuration to a JSON file for reference"""
    config_dict = {k: v for k, v in globals().items() 
                  if k.isupper() and not k.startswith('_')}
    
    # Convert non-serializable objects to strings
    for k, v in config_dict.items():
        if not isinstance(v, (str, int, float, bool, list, dict, type(None))):
            config_dict[k] = str(v)
    
    config_file = os.path.join(DATA_DIR, "current_config.json")
    with open(config_file, 'w') as f:
        json.dump(config_dict, f, indent=2)
    
    return config_file

# Validate required configuration
def validate_config():
    """Validate that required configuration parameters are set"""
    missing = []
    
    if not OANDA_API_KEY:
        missing.append("OANDA_API_KEY")
    if not OANDA_ACCOUNT_ID:
        missing.append("OANDA_ACCOUNT_ID")
    
    if missing:
        raise ValueError(f"Missing required configuration: {', '.join(missing)}")
    
    return True