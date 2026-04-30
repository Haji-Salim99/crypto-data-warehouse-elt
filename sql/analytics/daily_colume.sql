SELECT dd.full_date, SUM(f.total_volume) AS total_volume
FROM fact_crypto_snapshot f
JOIN dim_date dd ON f.date_key = dd.date_key
GROUP BY dd.full_date
ORDER BY dd.full_date;