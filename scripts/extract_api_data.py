import os
import json
import requests
import logging
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
LOG_DIR = BASE_DIR / "logs"

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging setup
LOG_FILE = LOG_DIR / "extract_api_data.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# Function to fetch data from API
def extract_crypto_data() -> list:
    # Get values from .env file
    api_base_url = os.getenv("API_BASE_URL")
    vs_currency = os.getenv("VS_CURRENCY", "usd")
    per_page = int(os.getenv("PER_PAGE", "50"))
    page = int(os.getenv("PAGE", "1"))

    if not api_base_url:
        logger.error("API_BASE_URL is not set in the .env file.")
        raise ValueError("API_BASE_URL is required")
    
    endpoint = f"{api_base_url}/coins/markets"

    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false"
    }

    max_retries = 5

    for attempt in range(max_retries):
        try:
            response = requests.get(endpoint, params=params)
            
            if response.status_code == 429:
                wait_time = 2 ** attempt
                print(f"Rate limited. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            time.sleep(2)

    raise Exception("Failed to fetch data after retries")

# Function to save raw data to file
def save_raw_json(data:list) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_DATA_DIR / f"crypto_data_{timestamp}.json"

    try:
        with open(file_path,"w", encoding ="utf-8") as f:
            json.dump(data,f, indent=2)
        
        logger.info(f"Raw data saved to {file_path}")
        return file_path
    
    except Exception as e:
        logger.error(f"Error saving raw data to file: {e}")
        raise

if __name__ == "__main__":
    try:
        crypto_data = extract_crypto_data()
        saved_file = save_raw_json(crypto_data)
        logger.info(f"Data extraction and saving completed successfully. File: {saved_file}")
    except Exception as e:
        logger.error(f"An error occurred during the extraction process: {e}")
    


