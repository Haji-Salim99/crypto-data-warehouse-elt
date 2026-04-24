CREATE TABLE IF NOT EXISTS raw_crypto_market_data(
    id SERIAL PRIMARY KEY,
    coin_id TEXT,
    symbol TEXT,
    coin_name TEXT,
    current_price NUMERIC,
    market_cap NUMERIC,
    market_cap_rank INTEGER,
    total_volume NUMERIC,
    high_24h NUMERIC,
    low_24h NUMERIC,
    price_change_24h NUMERIC,
    price_change_percentage_24h NUMERIC,
    snapshot_timestamp timestamp,
    ingestion_timestamp timestamp DEFAULT CURRENT_TIMESTAMP
)