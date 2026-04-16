
INSERT INTO dim_coin (coin_id, coin_name, symbol) 
SELECT DISTINCT coin_id, coin_name, symbol
FROM raw_crypto_market_data
WHERE coin_id IS NOT NULL
ON CONFLICT (coin_id) DO NOTHING;
