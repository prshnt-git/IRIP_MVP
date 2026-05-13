"""Scrape-log deduplication for IRIP review acquisition.

Hash strategy: SHA256 of "{product_id}|{source}|{review_date}|{raw_text[:80].lower().strip()}"
This is intentionally loose — the first 80 chars plus date catches the same review
re-scraped on different days while allowing distinct reviews that share a short prefix
to pass through.

Usage in a scraper loop:
    from app.services.dedup import generate_review_hash, is_duplicate, mark_scraped

    h = generate_review_hash(product_id, source, review_date, raw_text)
    if is_duplicate(db_path, h):
        continue
    # … import and analyse the review …
    mark_scraped(db_path, h, product_id, source)
"""
from __future__ import annotations

import hashlib

from app.db.database import connect


def generate_review_hash(
    product_id: str,
    source: str,
    review_date: str,
    raw_text: str,
) -> str:
    """Return SHA256 hex digest used as the scrape-log primary key."""
    key = f"{product_id}|{source}|{review_date}|{raw_text[:80].lower().strip()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def is_duplicate(db_path: str, review_hash: str) -> bool:
    """Return True if this hash already exists in scrape_log (i.e. already processed)."""
    with connect(db_path) as connection:
        row = connection.execute(
            "SELECT 1 FROM scrape_log WHERE review_hash = ? LIMIT 1",
            (review_hash,),
        ).fetchone()
    return row is not None


def mark_scraped(
    db_path: str,
    review_hash: str,
    product_id: str,
    source: str,
) -> None:
    """Record this hash in scrape_log so future runs skip it.

    INSERT OR IGNORE means calling this twice for the same hash is safe.
    """
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO scrape_log (review_hash, product_id, source, scraped_at)
            VALUES (?, ?, ?, datetime('now'))
            """,
            (review_hash, product_id, source),
        )


def get_stats(db_path: str) -> dict[str, int]:
    """Return aggregate counts for the Trust tab dedup-stats card.

    Returns:
        total_seen:    total rows ever written to scrape_log
        scraped_today: rows whose scraped_at date matches today (UTC)
    """
    with connect(db_path) as connection:
        row = connection.execute(
            """
            SELECT
                COUNT(*)                                                        AS total_seen,
                COUNT(CASE WHEN date(scraped_at) = date('now') THEN 1 END)     AS scraped_today
            FROM scrape_log
            """
        ).fetchone()

    if row is None:
        return {"total_seen": 0, "scraped_today": 0}

    return {
        "total_seen": int(row["total_seen"]),
        "scraped_today": int(row["scraped_today"]),
    }
