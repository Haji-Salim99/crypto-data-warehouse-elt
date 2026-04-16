INSERT INTO dim_date (date_key, full_date, day, month, month_name, year)
SELECT DISTINCT
    TO_CHAR(snapshot_timestamp, 'YYYYMMDD')::INTEGER AS date_key,
    DATE(snapshot_timestamp) AS full_date,
    EXTRACT(DAY FROM snapshot_timestamp) AS day,
    EXTRACT(MONTH FROM snapshot_timestamp) AS month,
    TO_CHAR(snapshot_timestamp, 'Month') AS month_name,
    EXTRACT(YEAR FROM snapshot_timestamp) AS year
FROM raw_crypto_market_data
WHERE snapshot_timestamp IS NOT NULL
ON CONFLICT (date_key) DO NOTHING;