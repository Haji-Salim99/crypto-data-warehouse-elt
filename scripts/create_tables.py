from utils import get_logger, DB_CONFIG
import psycopg2
from pathlib import Path

logger = get_logger("create_tables")

DDL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ddl"

def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for file in [
        "create_raw_table.sql",
        "create_dim_tables.sql",
        "create_fact_table.sql"
    ]:
        path = DDL_DIR / file
        logger.info(f"Running {path}")
        with open(path) as f:
            cursor.execute(f.read())

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()