import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Base project paths
BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql" / "transformations"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

def run_sql(cursor, filename):
    with open(SQL_DIR / filename, "r") as f:
        cursor.execute(f.read())

def load_dimensions():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    run_sql(cursor, "load_dim_coin.sql")
    run_sql(cursor, "load_dim_date.sql")
    run_sql(cursor, "load_dim_market_cap_category.sql")

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_dimensions()