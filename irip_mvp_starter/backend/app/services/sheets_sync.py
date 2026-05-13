"""Google Sheets sync service for IRIP.

Syncs SQLite data to a Google Spreadsheet in four tabs so stakeholders can
view live data without needing database access.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SETUP INSTRUCTIONS (one-time, ~10 minutes)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 1 — Create a Google Cloud project & Service Account
  a. Go to https://console.cloud.google.com
  b. Create a new project (e.g. "IRIP Sheets Sync") or use an existing one.
  c. APIs & Services → Library → search "Google Sheets API" → Enable.
  d. APIs & Services → Library → search "Google Drive API" → Enable.
  e. APIs & Services → Credentials → Create Credentials → Service Account.
     Name: irip-sheets-sync   Role: Editor
  f. Click the new service account → Keys → Add Key → Create New Key → JSON.
  g. Download the JSON file.  Keep it secret — it contains a private key.

STEP 2 — Share the Spreadsheet with the service account
  a. Create a new Google Spreadsheet (or open an existing one).
  b. From the URL, copy the Spreadsheet ID:
       https://docs.google.com/spreadsheets/d/<SPREADSHEET_ID>/edit
  c. In the Spreadsheet → Share → paste the service account email address.
       It looks like: irip-sheets-sync@your-project.iam.gserviceaccount.com
  d. Grant "Editor" access → Share.

STEP 3 — Add environment variables to Render
  Render Dashboard → your backend service → Environment → Add variables:

  Variable name                Value
  ─────────────────────────────────────────────────────────
  GOOGLE_SHEETS_CREDENTIALS    (entire content of the JSON key file — paste as-is)
  GOOGLE_SHEETS_SPREADSHEET_ID (the ID from Step 2b)

STEP 4 — Test
  curl -X POST https://irip-api.onrender.com/sheets/sync \
       -H "X-Pipeline-Key: <your PIPELINE_SECRET_KEY>"

  Open the Spreadsheet. You should see four tabs: Product Catalog,
  Raw Reviews, Processed Sentiment, Daily Summary.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import gspread

from app.db.database import connect

# IST = UTC + 5:30
_IST = timezone(timedelta(hours=5, minutes=30))


def _now_ist() -> str:
    return datetime.now(_IST).strftime("%Y-%m-%d %H:%M IST")


def _bool_label(value: Any) -> str:
    """Convert SQLite 0/1 / truthy / "true" strings to Yes/No."""
    if isinstance(value, str):
        return "Yes" if value.lower() in ("1", "true", "yes") else "No"
    return "Yes" if value else "No"


def _safe(value: Any) -> str:
    """Coerce any value to a string safe for Sheets (None → '')."""
    if value is None:
        return ""
    return str(value)


def _segment_from_price_band(price_band: str | None) -> str:
    """Map price band string to a human-readable segment label."""
    mapping = {
        "10000-15000": "Budget",
        "15000-20000": "Lower Mid-range",
        "20000-25000": "Mid-range",
        "25000-35000": "Upper Mid-range",
        "10000-35000": "Budget–Mid-range",
        "Entry": "Entry",
        "Mid-range": "Mid-range",
    }
    return mapping.get(price_band or "", price_band or "")


class SheetsSyncService:
    """Syncs IRIP SQLite data to a Google Spreadsheet.

    Authentication is done via a Google Service Account whose full JSON
    key is stored in the GOOGLE_SHEETS_CREDENTIALS environment variable.
    """

    def __init__(self, spreadsheet_id: str) -> None:
        """Authenticate and open the target spreadsheet.

        Args:
            spreadsheet_id: The ID from the spreadsheet URL
                (docs.google.com/spreadsheets/d/<ID>/edit).

        Raises:
            ValueError: If GOOGLE_SHEETS_CREDENTIALS env var is not set.
            gspread.exceptions.SpreadsheetNotFound: If the spreadsheet does
                not exist or is not shared with the service account.
        """
        credentials_raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
        if not credentials_raw:
            raise ValueError(
                "GOOGLE_SHEETS_CREDENTIALS environment variable is not set. "
                "See the setup instructions at the top of sheets_sync.py."
            )

        credentials_dict = json.loads(credentials_raw)
        gc = gspread.service_account_from_dict(credentials_dict)
        self._spreadsheet = gc.open_by_key(spreadsheet_id)

    # ----------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------

    def _get_or_create_worksheet(
        self, tab_name: str, rows: int = 5000, cols: int = 30
    ) -> gspread.Worksheet:
        """Return the worksheet with *tab_name*, creating it if absent."""
        try:
            return self._spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            return self._spreadsheet.add_worksheet(
                title=tab_name, rows=rows, cols=cols
            )

    def _write_tab(
        self,
        tab_name: str,
        headers: list[str],
        rows: list[list[str]],
    ) -> None:
        """Clear a worksheet and write timestamp + headers + data rows.

        Layout:
          Row 1: "Last updated: YYYY-MM-DD HH:MM IST"
          Row 2: column headers
          Row 3+: data rows

        Uses batch_update so the entire write is one API call regardless
        of the number of rows (avoids per-cell quota exhaustion).
        """
        ws = self._get_or_create_worksheet(tab_name)
        ws.clear()

        timestamp_row = [f"Last updated: {_now_ist()}"]
        all_values: list[list[str]] = [timestamp_row, headers] + rows

        # batch_update expects a list of {range, values} dicts.
        # A single range covering A1 → the last cell is the most efficient.
        if all_values:
            ws.batch_update(
                [{"range": "A1", "values": all_values}],
                value_input_option="USER_ENTERED",
            )

    # ----------------------------------------------------------
    # Public sync methods
    # ----------------------------------------------------------

    def sync_product_catalog(self, db_path: str) -> int:
        """Write all products from the SQLite products table.

        Writes to the "Product Catalog" tab with columns:
          product_id, product_name, brand, price_band, own_brand, launch_date,
          current_price, segment, competitor_product_ids

        Returns:
            Number of data rows written.
        """
        with connect(db_path) as conn:
            products = conn.execute(
                """
                SELECT
                    p.product_id,
                    p.product_name,
                    p.brand,
                    p.price_band,
                    p.own_brand,
                    p.launch_period,
                    p.marketplace_product_url,
                    p.comparison_group
                FROM products p
                ORDER BY p.brand, p.product_name
                """
            ).fetchall()

            # Aggregate competitor_product_ids per product
            mappings = conn.execute(
                """
                SELECT product_id, GROUP_CONCAT(competitor_product_id, '; ') AS competitor_ids
                FROM competitor_mappings
                GROUP BY product_id
                """
            ).fetchall()

        comp_map: dict[str, str] = {
            row["product_id"]: row["competitor_ids"] for row in mappings
        }

        headers = [
            "product_id", "product_name", "brand", "price_band", "own_brand",
            "launch_date", "current_price", "segment", "competitor_product_ids",
        ]

        data_rows: list[list[str]] = []
        for p in products:
            price_band = _safe(p["price_band"])
            data_rows.append([
                _safe(p["product_id"]),
                _safe(p["product_name"]),
                _safe(p["brand"]),
                price_band,
                _bool_label(p["own_brand"]),
                _safe(p["launch_period"]),
                "",                                        # current_price: not in SQLite products table
                _segment_from_price_band(price_band),
                comp_map.get(p["product_id"], ""),
            ])

        self._write_tab("Product Catalog", headers, data_rows)
        return len(data_rows)

    def sync_raw_reviews(self, db_path: str, days_back: int = 30) -> int:
        """Write recent reviews from reviews_raw to the "Raw Reviews" tab.

        Columns: review_date, product_name, brand, source, rating,
                 raw_text, verified_purchase

        Returns:
            Number of data rows written.
        """
        cutoff = (datetime.now(_IST) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        with connect(db_path) as conn:
            reviews = conn.execute(
                """
                SELECT
                    r.review_date,
                    r.product_name,
                    p.brand,
                    r.source,
                    r.rating,
                    r.raw_text,
                    r.verified_purchase
                FROM reviews_raw r
                LEFT JOIN products p ON p.product_id = r.product_id
                WHERE r.duplicate_status = 'canonical'
                  AND r.review_date >= ?
                ORDER BY r.review_date DESC, r.product_name
                """,
                (cutoff,),
            ).fetchall()

        headers = [
            "review_date", "product_name", "brand", "source",
            "rating", "raw_text", "verified_purchase",
        ]

        data_rows: list[list[str]] = [
            [
                _safe(r["review_date"]),
                _safe(r["product_name"]),
                _safe(r["brand"]),
                _safe(r["source"]),
                _safe(r["rating"]),
                _safe(r["raw_text"]),
                _bool_label(r["verified_purchase"]),
            ]
            for r in reviews
        ]

        self._write_tab("Raw Reviews", headers, data_rows)
        return len(data_rows)

    def sync_processed_sentiment(self, db_path: str, days_back: int = 30) -> int:
        """Write aspect-level sentiment data to the "Processed Sentiment" tab.

        Joins: aspect_sentiments ← reviews_raw ← reviews_processed

        Columns: review_date, product_name, brand, source, rating, sentiment,
                 main_aspect, key_phrase, confidence, is_hinglish

        Returns:
            Number of data rows written.
        """
        cutoff = (datetime.now(_IST) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        with connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT
                    r.review_date,
                    r.product_name,
                    p.brand,
                    r.source,
                    r.rating,
                    a.sentiment,
                    a.aspect        AS main_aspect,
                    a.evidence_span AS key_phrase,
                    a.confidence,
                    rp.language_profile_json
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                LEFT JOIN products p ON p.product_id = a.product_id
                LEFT JOIN reviews_processed rp ON rp.review_id = a.review_id
                WHERE r.review_date >= ?
                  AND r.duplicate_status = 'canonical'
                ORDER BY r.review_date DESC, r.product_name, a.aspect
                """,
                (cutoff,),
            ).fetchall()

        headers = [
            "review_date", "product_name", "brand", "source", "rating",
            "sentiment", "main_aspect", "key_phrase", "confidence", "is_hinglish",
        ]

        data_rows: list[list[str]] = []
        for row in rows:
            # Extract is_hinglish from the language_profile_json field
            is_hinglish = "No"
            lang_json = row["language_profile_json"]
            if lang_json:
                try:
                    lang = json.loads(lang_json)
                    is_hinglish = "Yes" if lang.get("is_hinglish") else "No"
                except (json.JSONDecodeError, TypeError):
                    pass

            data_rows.append([
                _safe(row["review_date"]),
                _safe(row["product_name"]),
                _safe(row["brand"]),
                _safe(row["source"]),
                _safe(row["rating"]),
                _safe(row["sentiment"]),
                _safe(row["main_aspect"]),
                _safe(row["key_phrase"]),
                _safe(row["confidence"]),
                is_hinglish,
            ])

        self._write_tab("Processed Sentiment", headers, data_rows)
        return len(data_rows)

    def sync_daily_summary(self, db_path: str, days_back: int = 30) -> int:
        """Aggregate per-product, per-day metrics to the "Daily Summary" tab.

        Aggregates: avg_rating, total_reviews, positive_pct, negative_pct,
                    neutral_pct, top_complaint (aspect), top_praise (aspect)

        Returns:
            Number of data rows written (one per product-day combination).
        """
        cutoff = (datetime.now(_IST) - timedelta(days=days_back)).strftime("%Y-%m-%d")

        with connect(db_path) as conn:
            # Daily review counts and avg rating
            daily_stats = conn.execute(
                """
                SELECT
                    r.review_date,
                    r.product_id,
                    r.product_name,
                    p.brand,
                    ROUND(AVG(r.rating), 2) AS avg_rating,
                    COUNT(r.review_id)      AS total_reviews
                FROM reviews_raw r
                LEFT JOIN products p ON p.product_id = r.product_id
                WHERE r.review_date >= ?
                  AND r.duplicate_status = 'canonical'
                GROUP BY r.review_date, r.product_id
                ORDER BY r.review_date DESC, r.product_name
                """,
                (cutoff,),
            ).fetchall()

            # Sentiment counts per product-day
            sentiment_counts = conn.execute(
                """
                SELECT
                    r.review_date,
                    a.product_id,
                    a.sentiment,
                    COUNT(*) AS cnt
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE r.review_date >= ?
                  AND r.duplicate_status = 'canonical'
                GROUP BY r.review_date, a.product_id, a.sentiment
                """,
                (cutoff,),
            ).fetchall()

            # Top negative aspect per product-day
            top_complaints = conn.execute(
                """
                SELECT
                    r.review_date,
                    a.product_id,
                    a.aspect,
                    COUNT(*) AS cnt
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE r.review_date >= ?
                  AND r.duplicate_status = 'canonical'
                  AND a.sentiment = 'negative'
                GROUP BY r.review_date, a.product_id, a.aspect
                ORDER BY r.review_date, a.product_id, cnt DESC
                """,
                (cutoff,),
            ).fetchall()

            # Top positive aspect per product-day
            top_praises = conn.execute(
                """
                SELECT
                    r.review_date,
                    a.product_id,
                    a.aspect,
                    COUNT(*) AS cnt
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE r.review_date >= ?
                  AND r.duplicate_status = 'canonical'
                  AND a.sentiment = 'positive'
                GROUP BY r.review_date, a.product_id, a.aspect
                ORDER BY r.review_date, a.product_id, cnt DESC
                """,
                (cutoff,),
            ).fetchall()

        # Build sentiment distribution lookup: (date, product_id) → {sentiment: count}
        sent_lookup: dict[tuple[str, str], dict[str, int]] = {}
        for row in sentiment_counts:
            key = (row["review_date"], row["product_id"])
            if key not in sent_lookup:
                sent_lookup[key] = {}
            sent_lookup[key][row["sentiment"]] = row["cnt"]

        # Top complaint per (date, product_id) — take first row since ordered by cnt DESC
        complaint_lookup: dict[tuple[str, str], str] = {}
        for row in top_complaints:
            key = (row["review_date"], row["product_id"])
            if key not in complaint_lookup:
                complaint_lookup[key] = row["aspect"]

        praise_lookup: dict[tuple[str, str], str] = {}
        for row in top_praises:
            key = (row["review_date"], row["product_id"])
            if key not in praise_lookup:
                praise_lookup[key] = row["aspect"]

        headers = [
            "review_date", "product_name", "brand", "total_reviews", "avg_rating",
            "positive_pct", "negative_pct", "neutral_pct",
            "top_complaint", "top_praise",
        ]

        data_rows: list[list[str]] = []
        for stat in daily_stats:
            date_str = _safe(stat["review_date"])
            pid = _safe(stat["product_id"])
            key = (date_str, pid)

            sentiments = sent_lookup.get(key, {})
            total_aspects = sum(sentiments.values()) or 1
            pos_pct = round(sentiments.get("positive", 0) / total_aspects * 100, 1)
            neg_pct = round(sentiments.get("negative", 0) / total_aspects * 100, 1)
            neu_pct = round(sentiments.get("neutral", 0) / total_aspects * 100, 1)

            data_rows.append([
                date_str,
                _safe(stat["product_name"]),
                _safe(stat["brand"]),
                _safe(stat["total_reviews"]),
                _safe(stat["avg_rating"]),
                f"{pos_pct}%",
                f"{neg_pct}%",
                f"{neu_pct}%",
                complaint_lookup.get(key, ""),
                praise_lookup.get(key, ""),
            ])

        self._write_tab("Daily Summary", headers, data_rows)
        return len(data_rows)

    # ----------------------------------------------------------
    # Orchestration
    # ----------------------------------------------------------

    def sync_all(self, db_path: str, days_back: int = 30) -> dict[str, Any]:
        """Run all four sync methods and return a summary.

        Errors in individual syncs are caught and collected rather than
        aborting the entire run — a Sheets quota error on one tab should
        not prevent the others from being written.

        Returns:
            {
                "catalog":     <rows written: int>,
                "raw_reviews": <rows written: int>,
                "sentiment":   <rows written: int>,
                "summary":     <rows written: int>,
                "errors":      <list of error strings>,
            }
        """
        result: dict[str, Any] = {
            "catalog": 0,
            "raw_reviews": 0,
            "sentiment": 0,
            "summary": 0,
            "errors": [],
        }

        steps: list[tuple[str, Any]] = [
            ("catalog",     lambda: self.sync_product_catalog(db_path)),
            ("raw_reviews", lambda: self.sync_raw_reviews(db_path, days_back)),
            ("sentiment",   lambda: self.sync_processed_sentiment(db_path, days_back)),
            ("summary",     lambda: self.sync_daily_summary(db_path, days_back)),
        ]

        for key, fn in steps:
            try:
                result[key] = fn()
            except Exception as exc:
                result["errors"].append(f"{key}: {exc}")

        return result
