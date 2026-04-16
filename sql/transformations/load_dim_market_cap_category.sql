INSERT INTO dim_market_cap_category (market_cap_category)
VALUES
('Large Cap'),
('Mid Cap'),
('Small Cap')
ON CONFLICT (market_cap_category) DO NOTHING;