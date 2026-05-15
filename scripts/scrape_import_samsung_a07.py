#!/usr/bin/env python3
"""Scrape Flipkart reviews for Samsung Galaxy A07 5G and import into IRIP DB.

Run from the repo root:
    python scripts/scrape_import_samsung_a07.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = REPO_ROOT / "irip_mvp_starter" / "backend"

sys.path.insert(0, str(BACKEND_DIR))
os.chdir(BACKEND_DIR)

PRODUCT_ID = "samsung_galaxy_a07_5g"
PRODUCT_NAME = "Samsung Galaxy A07 5G"
BRAND = "Samsung"
FLIPKART_URL = (
    "https://www.flipkart.com/samsung-galaxy-a07-5g-light-violet-128-gb"
    "/p/itm375542879d8d6?pid=MOBHJXB2YNDNYWWQ"
)
DB_PATH = BACKEND_DIR / "data" / "irip_mvp.db"
CSV_PATH = BACKEND_DIR / "data" / "samsung_galaxy_a07_5g_reviews.csv"


def step1_upsert_product() -> None:
    print("\n-- Step 1: Upsert product into DB --")
    from app.db.repository import ReviewRepository

    repo = ReviewRepository(DB_PATH)
    repo.upsert_product(
        product_id=PRODUCT_ID,
        product_name=PRODUCT_NAME,
        brand=BRAND,
        price_band="10000-15000",
        own_brand=False,
        parent_company="Samsung",
        marketplace="flipkart",
        marketplace_product_url=FLIPKART_URL,
    )
    print(f"  Upserted product: {PRODUCT_ID}")


def step2_scrape_reviews() -> int:
    print("\n-- Step 2: Playwright scraper --")
    from app.collectors.flipkart_review_collector import FlipkartReviewCollector

    collector = FlipkartReviewCollector(
        max_reviews=50,
        max_pages=10,
        headful=False,
        debug_dir=BACKEND_DIR / "data" / "scrape_debug",
    )

    reviews = collector.collect(
        product_id=PRODUCT_ID,
        product_name=PRODUCT_NAME,
        brand=BRAND,
        marketplace_url=FLIPKART_URL,
    )

    print(f"  Collected {len(reviews)} reviews")

    collector.write_csv(reviews, CSV_PATH)
    print(f"  Wrote CSV -> {CSV_PATH}")
    return len(reviews)


def step3_import_and_analyse() -> None:
    print("\n-- Step 3: Import CSV + sentiment analysis --")
    from app.db.repository import ReviewRepository
    from app.pipeline.review_analyzer import ReviewAnalyzer
    from app.services.import_service import ReviewImportService

    repo = ReviewRepository(DB_PATH)
    analyzer = ReviewAnalyzer()
    importer = ReviewImportService(repository=repo, analyzer=analyzer)

    csv_text = CSV_PATH.read_text(encoding="utf-8-sig")
    result = importer.import_csv_text(csv_text, discovered_via="playwright_collector")

    print(f"  Imported      : {result.imported_count}")
    print(f"  Skipped (dup) : {result.skipped_duplicate_count}")
    print(f"  Failed        : {result.failed_count}")
    if result.errors:
        for err in result.errors[:5]:
            print(f"    row {err.row_number}: {err.reason}")


def main() -> None:
    print("IRIP - Samsung Galaxy A07 5G scrape + import")
    print(f"DB   : {DB_PATH}")
    print(f"CSV  : {CSV_PATH}")

    step1_upsert_product()
    count = step2_scrape_reviews()

    if count == 0:
        print("\n[warn] No reviews collected — skipping import.")
        print("       Check data/scrape_debug/ for screenshots/HTML.")
        sys.exit(1)

    step3_import_and_analyse()
    print("\nDone.")


if __name__ == "__main__":
    main()
