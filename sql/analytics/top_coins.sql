SELECT d.coin_name, f.market_cap
    FROM fact_crypto_snapshot f
    JOIN dim_coin d ON f.coin_key = d.coin_key
    WHERE f.snapshot_timestamp = (
        SELECT MAX(snapshot_timestamp)
        FROM fact_crypto_snapshot
    )
    ORDER BY f.market_cap DESC
    LIMIT 10;