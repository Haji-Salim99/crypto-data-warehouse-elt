from utils import get_logger, DB_CONFIG
import psycopg2
from pathlib import Path

logger = get_logger("load_dimensions")

SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "transformations"

def load_dimensions():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for file in [
        "load_dim_coin.sql",
        "load_dim_date.sql",
        "load_dim_market_cap_category.sql"
    ]:
        path = SQL_DIR / file
        logger.info(f"Running {path}")
        with open(path) as f:
            cursor.execute(f.read())

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_dimensions()