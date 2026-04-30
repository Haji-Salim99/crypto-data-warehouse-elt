SELECT dmc.market_cap_category, SUM(f.market_cap) AS total_market_cap
FROM fact_crypto_snapshot f
JOIN dim_market_cap_category dmc 
ON f.category_key = dmc.category_key
GROUP BY dmc.market_cap_category;