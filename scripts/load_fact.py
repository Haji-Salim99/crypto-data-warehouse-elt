from utils import get_logger, DB_CONFIG
import psycopg2
from pathlib import Path

logger = get_logger("load_fact")

SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "transformations"

def load_fact():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    path = SQL_DIR / "load_fact_table.sql"
    logger.info(f"Running {path}")

    with open(path) as f:
        cursor.execute(f.read())

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_fact()