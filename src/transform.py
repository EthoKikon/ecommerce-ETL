# Gaurantees Schema and reproducibility; pandera helps catch bad data before loading
# src/transform.py
import re

import numpy as np
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

from src.config import OUTPUT_DIR
from src.utils import get_logger

logger = get_logger("transform")

schema = DataFrameSchema(
    {
        "title": Column(str, Check.str_length(1, 512)),
        "price": Column(float, Check.ge(0)),
        "availability": Column(str),
        "rating": Column(str),
        "link": Column(str, Check.str_length(1, 1024)),
    }
)

# regex pattern moved to its own name to avoid long in-line expressions
_PRICE_PATTERN = r"[-+]?\d[\d,]*\.?\d*"
_PRICE_RE = re.compile(_PRICE_PATTERN)


def clean_price(x):
    """
    Robust parsing of price-like strings to float.
    Examples handled: "£51.77", "Â51.77", "51,777.50", "  £0.99 "
    Returns numpy.nan if no valid number found.
    """
    if pd.isna(x):
        return np.nan
    s = str(x)

    # remove common encoding artefacts and whitespace
    s = s.replace("\xa0", "").replace("Â", "").strip()

    # find numeric substring
    m = _PRICE_RE.search(s)
    if not m:
        logger.warning("Could not parse numeric value from price: %r", x)
        return np.nan

    num_str = m.group(0).replace(",", "")  # remove thousand separators
    try:
        return float(num_str)
    except Exception as e:
        logger.warning("Float conversion failed for %r: %s", num_str, e)
        return np.nan


def run_transform(raw_csv, out_parquet=None):
    df = pd.read_csv(raw_csv)
    # basic cleaning & type coercions
    df["title"] = df["title"].astype(str).str.strip()
    df["price"] = df["price"].apply(clean_price).astype(float)
    df["availability"] = (
        df["availability"].astype(str).str.replace("\n", " ").str.strip()
    )
    df["rating"] = df["rating"].astype(str)
    df["link"] = df["link"].astype(str)

    # validate with pandera (lazy: see all issues)
    try:
        df = schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        logger.error("Schema validation failed. Sample errors:\n%s", err)
        bad_idx = getattr(err, "failure_cases", None)
        if bad_idx is not None:
            logger.info("See err.failure_cases for details.")
        raise

    out_parquet = out_parquet or (OUTPUT_DIR / "books_clean.parquet")
    df.to_parquet(out_parquet, index=False)
    logger.info(
        "Wrote cleaned parquet %s (%d rows)",
        out_parquet,
        len(df),
    )
    return str(out_parquet)
