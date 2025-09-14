# automated checks to avoid regressions.
# tests/test_transform.py
import pandas as pd

from src.transform import clean_price, run_transform


def test_clean_price():
    assert clean_price("£12.50") == 12.5
    assert clean_price("  £0.99 ") == 0.99


def test_transform_roundtrip(tmp_path):
    df = pd.DataFrame(
        [
            {
                "title": "Test",
                "price": "£1.00",
                "availability": "In stock",
                "rating": "Three",
                "link": "x",
            }
        ]
    )
    raw = tmp_path / "raw.csv"
    df.to_csv(raw, index=False)
    out = tmp_path / "out.parquet"
    run_transform(str(raw), out_parquet=str(out))
    res = pd.read_parquet(out)
    assert res.shape[0] == 1
