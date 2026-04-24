import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
# Base project paths
BASE_DIR = Path(__file__).resolve().parent.parent
DDL_DIR = BASE_DIR / "sql" / "ddl"

# Database configuration from environment variables
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

# Function to execute SQL files
def run_sql(cursor, filename):
    with open(DDL_DIR / filename, "r") as f:
        cursor.execute(f.read())

# Function to create tables in PostgreSQL
def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    run_sql(cursor, "create_raw_table.sql")
    run_sql(cursor, "create_dim_tables.sql")
    run_sql(cursor, "create_fact_table.sql")

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()