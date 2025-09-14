# Easy orchestration and visual monitorying; easy replacement for cron.
# src/flow.py
from prefect import flow, task

from src.extract import run_extract
from src.load import load_to_postgres
from src.transform import run_transform


@task
def extract_task(pages):
    return run_extract(pages=pages, out_csv="raw_books.csv")


@task
def transform_task(raw_csv):
    return run_transform(raw_csv, out_parquet="outputs/books_clean.parquet")


@task
def load_task(parquet_path):
    load_to_postgres(parquet_path)


@flow
def etl_flow(pages: int = 1):
    raw = extract_task(pages)
    cleaned = transform_task(raw)
    load_task(cleaned)


if __name__ == "__main__":
    etl_flow(1)
