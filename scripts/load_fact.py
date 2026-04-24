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

# Function to execute SQL files
def load_fact():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    with open(SQL_DIR / "load_fact_table.sql", "r") as f:
        cursor.execute(f.read())

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_fact()