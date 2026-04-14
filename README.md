# Crypto Data Warehouse ELT

A data engineering project for building a cryptocurrency data warehouse using PostgreSQL and ELT principles.

## Project Goal

This project focuses on moving from ETL to ELT by first loading raw cryptocurrency market data into PostgreSQL, then transforming it later inside the database for analytics and warehouse modeling.

## Current Progress

### Completed so far
- Created a new project structure for the data warehouse project
- Set up PostgreSQL project database: `crypto_dw`
- Created the raw table: `raw_crypto_market_data`
- Built Python extraction script to fetch crypto market data from the CoinGecko API
- Saved raw API responses as timestamped JSON files
- Built Python loading script to insert raw data into PostgreSQL
- Added logging to both terminal and log files
- Configured environment variables using `.env`

## Current Architecture

CoinGecko API
    ↓
Python Extraction
    ↓
Raw JSON Backup
    ↓
PostgreSQL Raw Table

## Tech Stack
Python
PostgreSQL
SQLAlchemy
Requests
python-dotenv
Logging
VS Code
## Project Structure
---

```
crypto-data-warehouse-elt/
│
├── dags/
│   └── crypto_elt_dag.py
│
├── scripts/
│   ├── extract_api_data.py
│   ├── load_raw_to_postgres.py
│   └── run_pipeline.py
│
├── sql/
│   ├── ddl/
│   │   ├── create_raw_table.sql
│   │   ├── create_dim_tables.sql
│   │   └── create_fact_table.sql
│   │
│   ├── transformations/
│   │   ├── staging.sql
│   │   ├── load_dim_coin.sql
│   │   ├── load_dim_date.sql
│   │   ├── load_dim_category.sql
│   │   └── load_fact_table.sql
│   │
│   └── analytics/
│       ├── top_coins.sql
│       ├── price_trend.sql
│       └── volatility.sql
│
├── data/
│   └── raw/
│
├── logs/
├── requirements.txt
├── .env
├── .gitignore
└── README.md

```

### Raw Table

The first layer of the pipeline stores raw API data in PostgreSQL.

Table: raw_crypto_market_data

Key columns:
coin_id
symbol
coin_name
current_price
market_cap
market_cap_rank
total_volume
high_24h
low_24h
price_change_24h
price_change_percentage_24h
snapshot_timestamp
ingestion_timestamp
### How to Run
1. Install dependencies
pip install -r requirements.txt
2. Add environment variables:
3. Create.env file
4. API_BASE_URL=https://api.coingecko.com/api/v3
5. VS_CURRENCY=usd
6. PER_PAGE=50
7. PAGE=1
8. DB_HOST=localhost
9. DB_PORT=5432
10. DB_NAME=crypto_dw
11. DB_USER=postgres
12. DB_PASSWORD=your_password
13. Create the raw table
14. Run the SQL in: sql/ddl/create_raw_table.sql
15. Run the raw loading pipeline : python scripts/load_raw_to_postgres.py

### Next Steps
Create dimension tables
Create fact table
Transform raw data inside PostgreSQL
Build star schema
Write analytics queries
Connect warehouse to Power BI
Add orchestration with Airflow
Learning Focus

### This project is helping me practice:
ELT pipeline design
raw data ingestion
PostgreSQL-based warehouse design
SQL transformations
data modeling with fact and dimension tables
production-style project structure