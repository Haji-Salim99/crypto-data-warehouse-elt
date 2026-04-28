import logging
import os
import time
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()


# PATHS

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


# LOGGER FACTORY

def get_logger(name: str):
    logger = logging.getLogger(name)

    
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )

    file_handler = logging.FileHandler(LOG_DIR / "elt_pipeline.log")
    console_handler = logging.StreamHandler()

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


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
    logger = get_logger("wait_for_db")

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


# RUN SQL FILE

def run_sql_file(cursor, file_path):
    logger = get_logger("sql_runner")

    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    logger.info(f"Running SQL file: {file_path}")

    with open(file_path, "r") as f:
        cursor.execute(f.read())