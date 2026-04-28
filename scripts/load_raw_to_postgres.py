import os
from datetime import datetime
from sqlalchemy import create_engine, text

from extract_api_data import extract_crypto_data, save_raw_json
from utils import get_logger

logger = get_logger("load_raw")


# CREATE DB ENGINE

def get_db_engine():
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_host, db_name, db_user, db_password]):
        raise ValueError("Missing DB environment variables")

    db_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )

    logger.info("Creating PostgreSQL engine")
    return create_engine(db_url)


# LOAD RAW DATA

def load_raw_data():
    try:
        logger.info("Starting raw data load process")

        engine = get_db_engine()

        # Extract
        crypto_data = extract_crypto_data()
        logger.info(f"Extracted {len(crypto_data)} records")

        # Save JSON backup
        saved_file = save_raw_json(crypto_data)
        logger.info(f"Raw JSON saved at: {saved_file}")

        # Prepare data
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

        rows = []

        for coin in crypto_data:
            rows.append({
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
                "snapshot_timestamp": snapshot_timestamp
            })

        logger.info(f"Prepared {len(rows)} rows for insertion")

        # Insert
        with engine.begin() as conn:
            conn.execute(insert_sql, rows)

        logger.info(f"Inserted {len(rows)} rows into raw table")

    except Exception as e:
        logger.exception(f"Raw load failed: {e}")
        raise


if __name__ == "__main__":
    load_raw_data()