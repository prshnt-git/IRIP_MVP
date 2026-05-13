#!/usr/bin/env python3
"""Daily review scraper for IRIP.

Scraping strategy (2026 hybrid):
  1. Discovery-Scraper Bridge: fetches the live product catalog from the
     Render backend (GET /products) so newly discovered ASINs/URLs are picked
     up automatically without editing the static JSON.
  2. ScraperAPI residential proxy as primary; direct requests as fallback.
     On Azure/GitHub Actions IPs are datacenter-blocked — ScraperAPI is
     required for non-zero results in CI.
  3. Static scripts/product_catalog.json supplements the live catalog for
     amazon_asin / flipkart_url fields not stored in the products DB table.

Run locally:
    pip install requests beautifulsoup4 lxml
    python scripts/run_daily_scrape.py
    python scripts/run_daily_scrape.py --days-back 14

Run in GitHub Actions:
    SCRAPERAPI_KEY=<key> BACKEND_API_URL=https://irip-api.onrender.com \
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
SCRIPT_DIR  = Path(__file__).resolve().parent           # <repo>/scripts/
REPO_ROOT   = SCRIPT_DIR.parent                         # <repo>/
BACKEND_DIR = REPO_ROOT / "irip_mvp_starter" / "backend"

# Add backend to PYTHONPATH so we can import the scrapers directly
sys.path.insert(0, str(BACKEND_DIR))

# ── Configuration from environment ───────────────────────────────────────────
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")             # comma-sep for key pool
BACKEND_API_URL = os.getenv(
    "BACKEND_API_URL", "https://irip-api.onrender.com"
)

# ── Output files ──────────────────────────────────────────────────────────────
OUTPUT_CSV     = REPO_ROOT / "scraped_reviews.csv"
SUMMARY_JSON   = REPO_ROOT / "scrape_summary.json"
CATALOG_JSON   = SCRIPT_DIR / "product_catalog.json"

# ── CSV fields expected by /reviews/import-csv ───────────────────────────────
CSV_FIELDS = [
    "review_id", "source", "product_id", "product_name", "brand",
    "review_date", "rating", "title", "raw_text", "verified_purchase",
    "helpful_votes", "price_band", "marketplace",
]


# ─────────────────────────────────────────────────────────────────────────────
# Catalog loading
# ─────────────────────────────────────────────────────────────────────────────

def load_static_catalog() -> list[dict]:
    """Load and validate scripts/product_catalog.json."""
    if not CATALOG_JSON.exists():
        print(f"WARNING: Static catalog not found at {CATALOG_JSON}", file=sys.stderr)
        return []

    with CATALOG_JSON.open(encoding="utf-8") as f:
        raw: list[dict] = json.load(f)

    products = []
    for item in raw:
        if "_comment" in item and len(item) <= 2:
            continue
        if not item.get("product_id") or not item.get("product_name"):
            continue
        products.append(item)

    return products


def fetch_live_catalog(api_base: str) -> list[dict]:
    """Fetch products from the live backend API (GET /products).

    Returns a list of product dicts normalised to match the static catalog
    format.  Returns [] on any error — the bridge is opportunistic; falling
    back to the static JSON is always safe.
    """
    import requests as _requests  # lazy import — only needed here

    try:
        resp = _requests.get(f"{api_base.rstrip('/')}/products", timeout=20)
        if resp.status_code != 200:
            print(
                f"[Bridge] GET /products returned HTTP {resp.status_code} — "
                "using static catalog only",
                file=sys.stderr,
            )
            return []

        data = resp.json()
        api_products: list[dict] = (
            data if isinstance(data, list) else data.get("products", [])
        )

        result: list[dict] = []
        for p in api_products:
            pid = p.get("product_id") or p.get("id") or ""
            name = p.get("product_name") or p.get("name") or ""
            if not pid or not name:
                continue

            # Detect marketplace from stored URL
            mp_url: str = p.get("marketplace_product_url") or ""
            mp_id: str = p.get("marketplace_product_id") or ""
            marketplace: str = p.get("marketplace") or ""

            is_amazon   = "amazon.in" in mp_url or marketplace.lower() == "amazon"
            is_flipkart = "flipkart.com" in mp_url or marketplace.lower() == "flipkart"

            result.append({
                "product_id":    pid,
                "product_name":  name,
                "brand":         p.get("brand") or "",
                "price_band":    p.get("price_band") or "10000-35000",
                "is_own_brand":  bool(p.get("own_brand") or p.get("is_own_brand")),
                # Best-effort ASIN / URL from DB fields
                "amazon_asin":   mp_id if is_amazon else "",
                "amazon_url":    mp_url if is_amazon else "",
                "flipkart_url":  mp_url if is_flipkart else "",
                "scrape_amazon":   is_amazon and bool(mp_id),
                "scrape_flipkart": is_flipkart and bool(mp_url),
            })

        return result

    except Exception as exc:
        print(f"[Bridge] Could not fetch live catalog: {exc}", file=sys.stderr)
        return []


def merge_catalogs(static: list[dict], live: list[dict]) -> list[dict]:
    """Merge live API catalog with the static JSON.

    Rules:
    - Static JSON always wins for amazon_asin / flipkart_url (curated).
    - Products in the live API but not in the static JSON are appended
      (newly discovered products that haven't been manually curated yet).
    """
    static_by_id = {p["product_id"]: p for p in static}

    merged = list(static)
    added = 0

    for lp in live:
        pid = lp.get("product_id", "")
        if not pid or pid in static_by_id:
            continue
        merged.append(lp)
        added += 1

    if added:
        print(f"[Bridge] Added {added} new product(s) from live API")
    elif live:
        print(f"[Bridge] Live catalog: {len(live)} product(s) — all already in static JSON")

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Scraping helpers
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_amazon(
    product: dict,
    days_back: int,
    pool: Any,
) -> list[dict]:
    """Import AmazonReviewScraper and scrape recent reviews."""
    asin = product.get("amazon_asin", "")
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

    mode = "ScraperAPI" if pool else "direct"
    print(
        f"  [Amazon/{mode}] Scraping {product['product_name']} "
        f"(ASIN={asin}, days_back={days_back})..."
    )
    try:
        scraper = AmazonReviewScraper(delay_seconds=2, scraperapi_pool=pool)
        reviews = scraper.scrape_recent_only(
            asin=asin,
            product_id=product["product_id"],
            product_name=product["product_name"],
            brand=product.get("brand", ""),
            days_back=days_back,
        )
        print(f"  [Amazon/{mode}] → {len(reviews)} reviews")
        return reviews
    except Exception as exc:
        print(f"  [Amazon] Error: {exc}", file=sys.stderr)
        return []


def _scrape_flipkart(
    product: dict,
    days_back: int,
    pool: Any,
) -> list[dict]:
    """Import FlipkartReviewScraper and scrape recent reviews."""
    url = product.get("flipkart_url", "")
    if not url or url.upper() == "TODO":
        print(f"  [Flipkart] {product['product_name']}: URL not set — skipping")
        return []

    if not product.get("scrape_flipkart", True):
        print(f"  [Flipkart] {product['product_name']}: scrape_flipkart=false — skipping")
        return []

    if "/p/" not in url:
        print(f"  [Flipkart] {product['product_name']}: URL has no /p/ — skipping ({url})")
        return []

    try:
        from app.scrapers.flipkart_scraper import FlipkartReviewScraper
    except ImportError as e:
        print(f"  [Flipkart] Import failed: {e}", file=sys.stderr)
        return []

    mode = "ScraperAPI" if pool else "direct"
    print(
        f"  [Flipkart/{mode}] Scraping {product['product_name']} "
        f"(days_back={days_back})..."
    )
    try:
        scraper = FlipkartReviewScraper(delay_seconds=2, scraperapi_pool=pool)
        reviews = scraper.scrape_recent_only(
            product_url=url,
            product_id=product["product_id"],
            product_name=product["product_name"],
            brand=product.get("brand", ""),
            days_back=days_back,
        )
        print(f"  [Flipkart/{mode}] → {len(reviews)} reviews")
        return reviews
    except Exception as exc:
        print(f"  [Flipkart] Error: {exc}", file=sys.stderr)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Post-processing
# ─────────────────────────────────────────────────────────────────────────────

def deduplicate(reviews: list[dict]) -> list[dict]:
    """Remove in-memory duplicates by review_id."""
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


def write_summary(
    summary: dict,
    path: Path,
) -> None:
    """Write scrape_summary.json artifact for CI visibility."""
    with path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# Type alias for the pool (avoids import at module level)
# ─────────────────────────────────────────────────────────────────────────────

from typing import Any  # noqa: E402  (standard lib, safe here)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

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
    print(f"Backend API URL     : {BACKEND_API_URL}")
    print(f"Output CSV          : {OUTPUT_CSV}")
    print(f"ScraperAPI key      : {'SET ✓' if SCRAPERAPI_KEY else 'NOT SET — direct requests only'}")
    print()

    # ── ScraperAPI key pool ───────────────────────────────────────────────────
    pool: Any = None
    if SCRAPERAPI_KEY:
        try:
            from app.scrapers.proxy import ApiKeyPool
            pool = ApiKeyPool.from_env(SCRAPERAPI_KEY)
            key_count = len(SCRAPERAPI_KEY.split(","))
            print(f"[ScraperAPI] Pool initialised: {key_count} key(s), 900 req/day limit")
        except Exception as exc:
            print(f"[ScraperAPI] Could not init pool: {exc} — falling back to direct", file=sys.stderr)
    print()

    # ── Discovery-Scraper Bridge ──────────────────────────────────────────────
    static_catalog = load_static_catalog()
    print(f"Static catalog      : {len(static_catalog)} product(s) from {CATALOG_JSON.name}")

    live_catalog = fetch_live_catalog(BACKEND_API_URL)
    print(f"Live API catalog    : {len(live_catalog)} product(s) from {BACKEND_API_URL}")

    catalog = merge_catalogs(static_catalog, live_catalog)
    print(f"Merged catalog      : {len(catalog)} product(s) total")
    print()

    # ── Scrape loop ───────────────────────────────────────────────────────────
    all_reviews: list[dict] = []
    products_scraped = 0
    per_product_summary: dict[str, dict[str, int]] = {}

    for product in catalog:
        name = product.get("product_name", product["product_id"])
        price_band = product.get("price_band", "10000-35000")
        pid = product["product_id"]

        print(f"── {name} ──")

        amazon_reviews = _scrape_amazon(product, days_back, pool)
        for r in amazon_reviews:
            all_reviews.append(to_csv_row(r, price_band))

        if amazon_reviews:
            time.sleep(2)

        flipkart_reviews = _scrape_flipkart(product, days_back, pool)
        for r in flipkart_reviews:
            all_reviews.append(to_csv_row(r, price_band))

        per_product_summary[pid] = {
            "amazon": len(amazon_reviews),
            "flipkart": len(flipkart_reviews),
        }

        if amazon_reviews or flipkart_reviews:
            products_scraped += 1

        print()
        time.sleep(3)

    # ── Deduplicate ───────────────────────────────────────────────────────────
    total_before = len(all_reviews)
    unique_reviews = deduplicate(all_reviews)
    duplicates_removed = total_before - len(unique_reviews)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("═" * 50)
    print(f"  Products scraped   : {products_scraped} / {len(catalog)}")
    print(f"  Total reviews raw  : {total_before}")
    print(f"  Duplicates removed : {duplicates_removed}")
    print(f"  Unique reviews     : {len(unique_reviews)}")
    if pool:
        print(f"  ScraperAPI calls   : {pool.daily_calls_used}")
    print("═" * 50)

    # ── Write CSV ─────────────────────────────────────────────────────────────
    if unique_reviews:
        write_csv(unique_reviews, OUTPUT_CSV)
        print(f"Wrote {len(unique_reviews)} review(s) → {OUTPUT_CSV}")
    else:
        write_csv([], OUTPUT_CSV)
        print("No new reviews found today — wrote empty CSV.")

    # ── Write scrape_summary.json ─────────────────────────────────────────────
    summary = {
        "scrape_date":       date.today().isoformat(),
        "days_back":         days_back,
        "scraperapi_active": pool is not None,
        "scraperapi_calls":  pool.daily_calls_used if pool else 0,
        "products":          per_product_summary,
        "totals": {
            "amazon":            sum(v["amazon"] for v in per_product_summary.values()),
            "flipkart":          sum(v["flipkart"] for v in per_product_summary.values()),
            "raw":               total_before,
            "duplicates_removed": duplicates_removed,
            "unique":            len(unique_reviews),
        },
    }
    write_summary(summary, SUMMARY_JSON)
    print(f"Wrote summary       → {SUMMARY_JSON}")
    print("Done.")


if __name__ == "__main__":
    main()
