# single-commnad reproducible run; easy to call from Docker CMD / CI
# src/cli.py
import argparse

from src.extract import run_extract
from src.load import load_to_postgres
from src.transform import run_transform
from src.utils import get_logger

logger = get_logger("cli")


def run(pages=1):
    raw_csv = run_extract(pages=pages, out_csv="raw_books.csv")
    parquet = run_transform(raw_csv, out_parquet="outputs/books_clean.parquet")
    load_to_postgres(parquet)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--pages", type=int, default=1)
    args = p.parse_args()
    run(args.pages)
