# src/extract.py
import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.utils import get_logger

logger = get_logger("extract")
BASE = "http://books.toscrape.com/"


def extract_books_page(page_url):
    r = requests.get(page_url, timeout=10)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    books = []
    for b in soup.select("article.product_pod"):
        title = b.h3.a["title"]
        price = b.select_one("p.price_color").text.strip()
        availability = b.select_one("p.instock.availability").text.strip()
        rating = b.p["class"][1] if len(b.p.get("class", [])) > 1 else "Zero"
        link = BASE + b.h3.a["href"].replace("../../", "catalogue/")
        books.append(
            {
                "title": title,
                "price": price,
                "availability": availability,
                "rating": rating,
                "link": link,
            }
        )
    return books


def run_extract(pages=1, out_csv="raw_books.csv"):
    all_books = []
    for i in range(1, pages + 1):
        url = BASE + f"catalogue/page-{i}.html" if i > 1 else BASE
        logger.info(f"Fetching {url}")
        all_books.extend(extract_books_page(url))
    df = pd.DataFrame(all_books)
    df.to_csv(out_csv, index=False)
    logger.info(f"Saved raw CSV {out_csv} ({len(df)} rows)")
    return out_csv
