# scripts/wait_for_db.py
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://etl_user:etl_pass@db:5432/etl_demo"
)
retries = 10
delay = 3


def wait():
    engine = create_engine(DATABASE_URL)
    for i in range(retries):
        try:
            conn = engine.connect()
            conn.close()
            print("DB ready!")
            return 0
        except OperationalError as e:
            print(f"DB not ready, retrying ({i+1}/{retries})...: {e}")
            time.sleep(delay)
    raise SystemExit("DB not available after retries")


if __name__ == "__main__":
    wait()
