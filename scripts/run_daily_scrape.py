#!/usr/bin/env python3
"""Daily review scraper for IRIP.

Reads scripts/product_catalog.json, scrapes Amazon.in (3 pages) and
Flipkart (2 pages) for each product, filters to the last N days, deduplicates
in-memory, and writes scraped_reviews.csv for import via /reviews/import-csv.

Run locally:
    cd <repo-root>
    pip install requests beautifulsoup4 lxml
    python scripts/run_daily_scrape.py
    python scripts/run_daily_scrape.py --days-back 14

Run in GitHub Actions:
    python scripts/run_daily_scrape.py --days-back "$DAYS_BACK"
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# ── Resolve paths relative to this script's location ─────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent          # <repo>/scripts/
REPO_ROOT   = SCRIPT_DIR.parent                       # <repo>/
BACKEND_DIR = REPO_ROOT / "irip_mvp_starter" / "backend"  # FastAPI backend

# Add backend to PYTHONPATH so we can import the scrapers directly
sys.path.insert(0, str(BACKEND_DIR))

# ── Output file is always written to repo root (GitHub Actions working dir) ──
OUTPUT_CSV  = REPO_ROOT / "scraped_reviews.csv"
CATALOG_JSON = SCRIPT_DIR / "product_catalog.json"

# ── CSV fields expected by /reviews/import-csv ───────────────────────────────
CSV_FIELDS = [
    "review_id",
    "source",
    "product_id",
    "product_name",
    "brand",
    "review_date",
    "rating",
    "title",
    "raw_text",
    "verified_purchase",
    "helpful_votes",
    "price_band",
    "marketplace",
]


# ─────────────────────────────────────────────────────────────────────────────

def load_catalog() -> list[dict]:
    """Load and validate scripts/product_catalog.json."""
    if not CATALOG_JSON.exists():
        print(f"ERROR: Catalog file not found at {CATALOG_JSON}", file=sys.stderr)
        sys.exit(1)

    with CATALOG_JSON.open(encoding="utf-8") as f:
        raw: list[dict] = json.load(f)

    products = []
    for item in raw:
        if "_comment" in item and len(item) <= 2:
            continue  # skip comment-only objects
        # Require product_id and product_name at minimum
        if not item.get("product_id") or not item.get("product_name"):
            continue
        products.append(item)

    return products


def _scrape_amazon(product: dict, days_back: int) -> list[dict]:
    """Import AmazonReviewScraper and scrape recent reviews."""
    asin = product.get("amazon_asin", "TODO")
    if not asin or asin.upper() == "TODO":
        print(f"  [Amazon] {product['product_name']}: ASIN not set — skipping")
        return []

    if not product.get("scrape_amazon", True):
        print(f"  [Amazon] {product['product_name']}: scrape_amazon=false — skipping")
        return []

    try:
        from app.scrapers.amazon_scraper import AmazonReviewScraper
    except ImportError as e:
        print(f"  [Amazon] Import failed: {e}", file=sys.stderr)
        return []

    print(f"  [Amazon] Scraping {product['product_name']} (ASIN={asin}, days_back={days_back})...")
    try:
        scraper = AmazonReviewScraper(delay_seconds=2)
        reviews = scraper.scrape_recent_only(
            asin=asin,
            product_id=product["product_id"],
            product_name=product["product_name"],
            brand=product.get("brand", ""),
            days_back=days_back,
        )
        print(f"  [Amazon] → {len(reviews)} reviews")
        return reviews
    except Exception as exc:
        print(f"  [Amazon] Error: {exc}", file=sys.stderr)
        return []


def _scrape_flipkart(product: dict, days_back: int) -> list[dict]:
    """Import FlipkartReviewScraper and scrape recent reviews."""
    url = product.get("flipkart_url", "TODO")
    if not url or url.upper() == "TODO":
        print(f"  [Flipkart] {product['product_name']}: URL not set — skipping")
        return []

    if not product.get("scrape_flipkart", True):
        print(f"  [Flipkart] {product['product_name']}: scrape_flipkart=false — skipping")
        return []

    # Basic sanity-check: must be a Flipkart product URL containing /p/
    if "/p/" not in url:
        print(f"  [Flipkart] {product['product_name']}: URL has no /p/ — skipping ({url})")
        return []

    try:
        from app.scrapers.flipkart_scraper import FlipkartReviewScraper
    except ImportError as e:
        print(f"  [Flipkart] Import failed: {e}", file=sys.stderr)
        return []

    print(f"  [Flipkart] Scraping {product['product_name']} (days_back={days_back})...")
    try:
        scraper = FlipkartReviewScraper(delay_seconds=2)
        reviews = scraper.scrape_recent_only(
            product_url=url,
            product_id=product["product_id"],
            product_name=product["product_name"],
            brand=product.get("brand", ""),
            days_back=days_back,
        )
        print(f"  [Flipkart] → {len(reviews)} reviews")
        return reviews
    except Exception as exc:
        print(f"  [Flipkart] Error: {exc}", file=sys.stderr)
        return []


def deduplicate(reviews: list[dict]) -> list[dict]:
    """Remove duplicates by review_id (in-memory; no DB needed)."""
    seen: set[str] = set()
    unique: list[dict] = []
    for review in reviews:
        rid = review.get("review_id", "")
        if not rid or rid in seen:
            continue
        seen.add(rid)
        unique.append(review)
    return unique


def to_csv_row(review: dict, price_band: str) -> dict:
    """Map a scraper review dict to the /reviews/import-csv column schema."""
    return {
        "review_id":        review.get("review_id", ""),
        "source":           review.get("source", ""),
        "product_id":       review.get("product_id", ""),
        "product_name":     review.get("product_name", ""),
        "brand":            review.get("brand", ""),
        "review_date":      review.get("review_date", ""),
        "rating":           review.get("rating", ""),
        "title":            review.get("title", ""),
        "raw_text":         review.get("raw_text", ""),
        "verified_purchase": "TRUE" if review.get("verified_purchase") else "FALSE",
        "helpful_votes":    review.get("helpful_votes", 0),
        "price_band":       review.get("price_band") or price_band,
        "marketplace":      review.get("marketplace", review.get("source", "")),
    }


def write_csv(rows: list[dict], path: Path) -> None:
    """Write review rows to CSV at *path*."""
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="IRIP daily review scraper")
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Number of days of reviews to scrape (default: 7)",
    )
    args = parser.parse_args()
    days_back: int = max(1, min(args.days_back, 30))

    cutoff = (date.today() - timedelta(days=days_back)).isoformat()
    print(f"IRIP Daily Scrape — {date.today().isoformat()}")
    print(f"Collecting reviews since {cutoff} ({days_back} days back)")
    print(f"Backend module path : {BACKEND_DIR}")
    print(f"Output CSV          : {OUTPUT_CSV}")
    print()

    catalog = load_catalog()
    print(f"Loaded {len(catalog)} product(s) from {CATALOG_JSON.name}")
    print()

    all_reviews: list[dict] = []
    products_scraped = 0

    for product in catalog:
        name = product.get("product_name", product["product_id"])
        price_band = product.get("price_band", "10000-35000")

        print(f"── {name} ──")

        # Amazon
        amazon_reviews = _scrape_amazon(product, days_back)
        for r in amazon_reviews:
            all_reviews.append(to_csv_row(r, price_band))

        # Small delay between Amazon and Flipkart for the same product
        if amazon_reviews:
            time.sleep(2)

        # Flipkart
        flipkart_reviews = _scrape_flipkart(product, days_back)
        for r in flipkart_reviews:
            all_reviews.append(to_csv_row(r, price_band))

        if amazon_reviews or flipkart_reviews:
            products_scraped += 1

        print()

        # Polite delay between products
        time.sleep(3)

    # Deduplicate the combined list by review_id
    total_before = len(all_reviews)
    unique_reviews = deduplicate(all_reviews)
    duplicates_removed = total_before - len(unique_reviews)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("═" * 50)
    print(f"  Products scraped   : {products_scraped} / {len(catalog)}")
    print(f"  Total reviews raw  : {total_before}")
    print(f"  Duplicates removed : {duplicates_removed}")
    print(f"  Unique reviews     : {len(unique_reviews)}")
    print("═" * 50)

    # ── Write CSV ─────────────────────────────────────────────────────────────
    if unique_reviews:
        write_csv(unique_reviews, OUTPUT_CSV)
        print(f"Wrote {len(unique_reviews)} review(s) → {OUTPUT_CSV}")
    else:
        # Write an empty CSV (header only) so the upload-artifact step
        # finds the file and doesn't emit a warning.
        write_csv([], OUTPUT_CSV)
        print("No new reviews found today — wrote empty CSV.")

    print("Done.")


if __name__ == "__main__":
    main()
