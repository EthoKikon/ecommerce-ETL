# Keeps loading logic seperate to easily change destination.
# src/load.py
import time

import pandas as pd
from sqlalchemy import create_engine

from src.config import DATABASE_URL
from src.utils import get_logger

logger = get_logger("load")


def load_to_postgres(parquet_path, table_name="books", if_exists="replace"):
    if DATABASE_URL is None:
        raise ValueError("DATABASE_URL not set in environment")
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    # read parquet
    df = pd.read_parquet(parquet_path)
    # try with simple retry for DB readiness
    for attempt in range(3):
        try:
            df.to_sql(table_name, engine, if_exists=if_exists, index=False)
            logger.info(f"Loaded {len(df)} rows into {table_name}")
            return
        except Exception as e:
            logger.warning(f"Load attempt {attempt+1} failed: {e}")
            time.sleep(2)
    raise RuntimeError("Failed to load to Postgres after retries")
