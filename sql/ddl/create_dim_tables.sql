CREATE TABLE IF NOT EXISTS dim_coin (
    coin_key SERIAL PRIMARY KEY,
    coin_id TEXT UNIQUE,
    coin_name TEXT,
    symbol TEXT
);


CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE,
    day INTEGER,
    month INTEGER,
    month_name TEXT,
    year INTEGER
);


CREATE TABLE IF NOT EXISTS dim_market_cap_category (
    category_key SERIAL PRIMARY KEY,
    market_cap_category TEXT UNIQUE
);
