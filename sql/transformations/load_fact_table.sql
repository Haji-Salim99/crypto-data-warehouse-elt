INSERT INTO fact_crypto_snapshot (
    coin_key,
    date_key,
    category_key,
    snapshot_timestamp,
    current_price,
    market_cap,
    market_cap_rank,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h
)
SELECT
    dc.coin_key,
    dd.date_key,
    dmc.category_key,
    r.snapshot_timestamp,
    r.current_price,
    r.market_cap,
    r.market_cap_rank,
    r.total_volume,
    r.high_24h,
    r.low_24h,
    r.price_change_24h,
    r.price_change_percentage_24h
FROM raw_crypto_market_data r
JOIN dim_coin dc
    ON r.coin_id = dc.coin_id
JOIN dim_date dd
    ON DATE(r.snapshot_timestamp) = dd.full_date
JOIN dim_market_cap_category dmc
    ON dmc.market_cap_category = CASE
        WHEN r.market_cap > 10000000000 THEN 'Large Cap'
        WHEN r.market_cap BETWEEN 1000000000 AND 10000000000 THEN 'Mid Cap'
        ELSE 'Small Cap'
    END
    
ON CONFLICT (coin_key, snapshot_timestamp) DO NOTHING;