CREATE TABLE IF NOT EXISTS fact_crypto_snapshot (
    fact_id SERIAL PRIMARY KEY,
    coin_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    category_key INTEGER NOT NULL,
    snapshot_timestamp TIMESTAMP NOT NULL,
    current_price NUMERIC,
    market_cap NUMERIC,
    market_cap_rank INTEGER,
    total_volume NUMERIC,
    high_24h NUMERIC,
    low_24h NUMERIC,
    price_change_24h NUMERIC,
    price_change_percentage_24h NUMERIC,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_coin
        FOREIGN KEY (coin_key) REFERENCES dim_coin (coin_key),

    CONSTRAINT fk_date
        FOREIGN KEY (date_key) REFERENCES dim_date (date_key),

    CONSTRAINT fk_category
        FOREIGN KEY (category_key) REFERENCES dim_market_cap_category (category_key)
    
);