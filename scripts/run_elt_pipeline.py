import traceback
import psycopg2
import logging
import time
import os
from dotenv import load_dotenv
from pathlib import Path
import subprocess

load_dotenv()


# PATH CONFIG

BASE_DIR = Path(__file__).resolve().parent.parent
SQL_DIR = BASE_DIR / "sql" / "transformations"
DDL_DIR = BASE_DIR / "sql" / "ddl"
SCRIPT_DIR = BASE_DIR / "scripts"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / "elt_pipeline.log"


# LOGGING SETUP

logger = logging.getLogger("elt_pipeline")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(log_file)
console_handler = logging.StreamHandler()

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# DB CONFIG

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")
}


# WAIT FOR DB

def wait_for_db(max_retries=10, delay=3):
    logger.info("Waiting for PostgreSQL to be ready...")

    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            logger.info("PostgreSQL is ready!")
            return
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
            time.sleep(delay)

    raise Exception("Database not ready after retries")


# RUN PYTHON SCRIPT

def run_python_script(script_name):
    script_path = SCRIPT_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"{script_name} not found at {script_path}")

    logger.info(f"Running script: {script_name}")

    result = subprocess.run(
        ["python", "-u", str(script_path)],
        capture_output=True,
        text=True
    )

    logger.info(f"[{script_name}] STDOUT:\n{result.stdout}")
    logger.error(f"[{script_name}] STDERR:\n{result.stderr}")

    if result.returncode != 0:
        raise Exception(f"{script_name} failed with return code {result.returncode}")


# RUN SQL FILE

def run_sql_file(cursor, file_name, directory):
    sql_path = directory / file_name

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    logger.info(f"Running SQL file: {sql_path}")

    with open(sql_path, "r") as file:
        cursor.execute(file.read())



# LOAD DIMENSIONS

def load_dimensions(cursor):
    logger.info("Loading dimension tables...")

    for file in [
        "load_dim_coin.sql",
        "load_dim_date.sql",
        "load_dim_market_cap_category.sql"
    ]:
        run_sql_file(cursor, file, SQL_DIR)


# LOAD FACT

def load_fact(cursor):
    logger.info("Loading fact table...")
    run_sql_file(cursor, "load_fact_table.sql", SQL_DIR)


# PIPELINE

def run_pipeline():
    try:
        logger.info("===== STARTING ELT PIPELINE =====")

        wait_for_db()


        # RAW LOAD
        run_python_script("load_raw_to_postgres.py")

        # TRANSFORMATIONS
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        load_dimensions(cursor)
        conn.commit()

        load_fact(cursor)
        conn.commit()

        cursor.close()
        conn.close()

        logger.info("===== PIPELINE COMPLETED SUCCESSFULLY =====")

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        logger.error(traceback.format_exc())
        raise


# ENTRY

if __name__ == "__main__":
    start = time.time()
    run_pipeline()
    print(f"Pipeline finished in {time.time() - start:.2f}s")