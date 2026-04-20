import psycopg2
import logging
import time
import os
from dotenv import load_dotenv
from pathlib import Path
import subprocess

load_dotenv()

# Base project paths
BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql" / "transformations"
SCRIPT_DIR = BASE_DIR / "scripts"
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / "elt_pipeline.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Database connection parameters
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}

# Function to run a Python script
def run_python_script(script_name):
    script_path = SCRIPT_DIR / script_name
    if not script_path.exists():
        logger.error(f"Script {script_name} not found at {script_path}")
        raise FileNotFoundError(f"{script_name} not found")
    
    result = subprocess.run(["python", str(script_path)], capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error running {script_name}: {result.stderr}")
        raise Exception(f"{script_name} failed")

    logger.info(f"{script_name} completed successfully")


    # Function to run SQL files
def run_sql_file(cursor, file_name):
        sql_path = SQL_DIR / file_name
        logger.info(f"Running SQL file: {file_name}")
        with open(sql_path, "r") as file:
            sql = file.read()
            cursor.execute(sql)

    # Load dimension tables
def load_dimensions(cursor):
        logger.info("Loading dimension tables...")

        dim_files = [
            "load_dim_coin.sql",
            "load_dim_date.sql",
            "load_dim_market_cap_category.sql"
        ]

        for file in dim_files:
            run_sql_file(cursor,file)

        logger.info("Dimension tables loaded successfully.")


    # Load fact table
def load_fact(cursor):
        logger.info("Loading fact table...")

        run_sql_file(cursor, "load_fact_table.sql")

        logger.info("Fact table loaded successfully.")


    # Main function to run the ELT pipeline
def run_pipeline():
        try:
            logger.info("===== STARTING ELT PIPELINE =====")

            # RAW Data Extraction and Load
            run_python_script("load_raw_to_postgres.py")

            # Connect to the database
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Load dimension tables
            try:
                load_dimensions(cursor)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Error loading dimension tables: {e}")
                raise

            # Load fact table
            try:
                load_fact(cursor)
                conn.commit()
            except Exception as e:
                    conn.rollback()
                    logger.error(f"Error loading fact table: {e}")
                    raise
            
            cursor.close()
            conn.close()

            logger.info("===== ELT PIPELINE COMPLETED SUCCESSFULLY =====")

        except Exception as e:
                logger.error(f"ELT pipeline failed: {e}")
                raise

if __name__ == "__main__":
    start_time = time.time()

    run_pipeline()

    end_time = time.time()
    print(f"Pipeline finished in {end_time - start_time:.2f} seconds")

