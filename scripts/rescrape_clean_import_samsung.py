#!/usr/bin/env python3
"""Scrape, clean, and import Samsung Galaxy A07 5G reviews into IRIP.

Steps:
  1. Playwright scrape (up to 100 reviews, 20 pages)
  2. ReviewCleaner pass (removes junk, metadata-only, too-short)
  3. Import cleaned reviews to local DB (runs ReviewAnalyzer sentiment)
  4. POST cleaned CSV to Render production backend

Run from repo root:
    python scripts/rescrape_clean_import_samsung.py
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
BACKEND_DIR = REPO_ROOT / "irip_mvp_starter" / "backend"

sys.path.insert(0, str(BACKEND_DIR))
os.chdir(BACKEND_DIR)

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

PRODUCT_ID = "samsung_galaxy_a07_5g"
PRODUCT_NAME = "Samsung Galaxy A07 5G"
BRAND = "Samsung"
FLIPKART_URL = (
    "https://www.flipkart.com/samsung-galaxy-a07-5g-light-violet-128-gb"
    "/p/itm375542879d8d6?pid=MOBHJXB2YNDNYWWQ"
)
DB_PATH = BACKEND_DIR / "data" / "irip_mvp.db"
RAW_CSV = BACKEND_DIR / "data" / "samsung_galaxy_a07_5g_reviews_raw.csv"
CLEAN_CSV = BACKEND_DIR / "data" / "samsung_galaxy_a07_5g_reviews_clean.csv"
RENDER_URL = "https://irip-api.onrender.com/reviews/import-csv"
PIPELINE_KEY = "Luvey_AI_2026!"


def step1_scrape() -> int:
    print("\n-- Step 1: Playwright scrape (max 100 reviews) --")
    from app.collectors.flipkart_review_collector import FlipkartReviewCollector

    collector = FlipkartReviewCollector(
        max_reviews=100,
        max_pages=20,
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
    collector.write_csv(reviews, RAW_CSV)
    print(f"  Wrote raw CSV -> {RAW_CSV.name}")
    return len(reviews)


def step2_clean() -> int:
    print("\n-- Step 2: ReviewCleaner --")
    from app.collectors.review_cleaner import ReviewCleaner

    cleaner = ReviewCleaner()
    result = cleaner.clean_csv_path(
        input_path=RAW_CSV,
        output_path=CLEAN_CSV,
        report_path=BACKEND_DIR / "data" / "samsung_galaxy_a07_5g_clean_report.json",
    )
    print(f"  Input  : {result.input_count}")
    print(f"  Clean  : {result.clean_count}")
    print(f"  Removed: {result.removed_count}")
    print(f"  Dupes  : {result.duplicate_count}")
    return result.clean_count


def step3_import_local() -> None:
    print("\n-- Step 3: Local DB import + sentiment analysis --")
    from app.db.repository import ReviewRepository
    from app.pipeline.review_analyzer import ReviewAnalyzer
    from app.services.import_service import ReviewImportService

    repo = ReviewRepository(DB_PATH)
    # Ensure product exists
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

    analyzer = ReviewAnalyzer()
    importer = ReviewImportService(repository=repo, analyzer=analyzer)

    csv_text = CLEAN_CSV.read_text(encoding="utf-8-sig")
    result = importer.import_csv_text(csv_text, discovered_via="playwright_clean")

    print(f"  Imported  : {result.imported_count}")
    print(f"  Skipped   : {result.skipped_duplicate_count}")
    print(f"  Failed    : {result.failed_count}")
    for err in result.errors[:5]:
        print(f"    row {err.row_number}: {err.reason}")


def step4_post_to_render() -> None:
    print("\n-- Step 4: POST cleaned CSV to Render --")
    import requests

    print("  Waking Render...")
    try:
        health = requests.get("https://irip-api.onrender.com/health", timeout=30)
        print(f"  Health: {health.status_code}")
    except Exception as e:
        print(f"  Health check failed: {e}")

    time.sleep(5)

    with open(CLEAN_CSV, "rb") as f:
        try:
            resp = requests.post(
                RENDER_URL,
                headers={"X-Pipeline-Key": PIPELINE_KEY},
                files={"file": ("samsung_clean.csv", f, "text/csv")},
                timeout=300,
            )
            print(f"  Status: {resp.status_code}")
            print(f"  Response: {resp.text[:400]}")
        except Exception as e:
            print(f"  POST failed: {e}")


def main() -> None:
    print("IRIP - Samsung Galaxy A07 5G: Rescrape + Clean + Import")
    print(f"  DB    : {DB_PATH}")
    print(f"  Raw   : {RAW_CSV.name}")
    print(f"  Clean : {CLEAN_CSV.name}")

    count = step1_scrape()
    if count == 0:
        print("\n[warn] No reviews scraped. Check scrape_debug/ for screenshots.")
        sys.exit(1)

    clean_count = step2_clean()
    if clean_count == 0:
        print("\n[warn] All reviews rejected by cleaner. Check raw CSV quality.")
        sys.exit(1)

    step3_import_local()
    step4_post_to_render()

    print("\nDone. Updated CSV files:")
    print(f"  {RAW_CSV}")
    print(f"  {CLEAN_CSV}")


if __name__ == "__main__":
    main()
