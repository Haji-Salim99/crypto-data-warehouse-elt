import os
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from extract_api_data import extract_crypto_data, save_raw_json


load_dotenv()

# Base project paths
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging setup
LOG_FILE = LOG_DIR / "load_raw_to_postgres.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

# Function to create a database engine
def get_db_engine():
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_host, db_name, db_user, db_password]):
        logger.error("Missing one or more DB environment variables.")
        raise ValueError("Database connection details are missing in the .env file.")

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )

    logger.info("Creating PostgreSQL database engine.")
    return create_engine(db_url)

# Function to load raw data into PostgreSQL
def load_raw_data():
    try:
        logger.info("Starting raw data load process.")

        engine = get_db_engine()

        crypto_data = extract_crypto_data()
        logger.info("Extracted %s records from API.", len(crypto_data))

        saved_file = save_raw_json(crypto_data)
        logger.info("Raw JSON backup saved at: %s", saved_file)

        snapshot_timestamp = datetime.now()

        insert_sql = text("""
            INSERT INTO raw_crypto_market_data (
                coin_id,
                symbol,
                coin_name,
                current_price,
                market_cap,
                market_cap_rank,
                total_volume,
                high_24h,
                low_24h,
                price_change_24h,
                price_change_percentage_24h,
                snapshot_timestamp
            )
            VALUES (
                :coin_id,
                :symbol,
                :coin_name,
                :current_price,
                :market_cap,
                :market_cap_rank,
                :total_volume,
                :high_24h,
                :low_24h,
                :price_change_24h,
                :price_change_percentage_24h,
                :snapshot_timestamp
            )
        """)

        rows_to_insert = []

        for coin in crypto_data:
            row = {
                "coin_id": coin.get("id"),
                "symbol": coin.get("symbol"),
                "coin_name": coin.get("name"),
                "current_price": coin.get("current_price"),
                "market_cap": coin.get("market_cap"),
                "market_cap_rank": coin.get("market_cap_rank"),
                "total_volume": coin.get("total_volume"),
                "high_24h": coin.get("high_24h"),
                "low_24h": coin.get("low_24h"),
                "price_change_24h": coin.get("price_change_24h"),
                "price_change_percentage_24h": coin.get("price_change_percentage_24h"),
                "snapshot_timestamp": snapshot_timestamp,
            }
            rows_to_insert.append(row)

        logger.info("Prepared %s rows for insertion.", len(rows_to_insert))

        with engine.begin() as connection:
            connection.execute(insert_sql, rows_to_insert)

        logger.info("Successfully loaded %s rows into raw_crypto_market_data.", len(rows_to_insert))

    except Exception as e:
        logger.exception("Raw data loading failed: %s", e)
        raise


if __name__ == "__main__":
    load_raw_data()