SELECT dc.coin_name, dd.full_date, AVG(f.current_price) AS avg_price
FROM fact_crypto_snapshot f
JOIN dim_coin dc ON f.coin_key = dc.coin_key
JOIN dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.full_date, dc.coin_name
ORDER BY dd.full_date