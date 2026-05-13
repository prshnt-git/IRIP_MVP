#!/usr/bin/env python3
"""
Import demo data into the live Render database.

Usage:
    python scripts/import_to_render.py

Requires no third-party packages — uses requests if installed, urllib otherwise.
"""

import csv
import io
import json
import sys
from pathlib import Path

BASE_URL = "https://irip-api.onrender.com"
DATA_DIR = Path(__file__).parent.parent / "backend" / "data"
REVIEWS_CSV = DATA_DIR / "final_recent_balanced_review_feed.csv"
CATALOG_JSON = DATA_DIR / "product_catalog.json"

BOUNDARY = "----IripFormBoundary7f3a9b2e4c1d"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _build_multipart_body(field_name: str, filename: str, content: bytes, content_type: str) -> bytes:
    header = (
        f"--{BOUNDARY}\r\n"
        f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
        f"Content-Type: {content_type}\r\n\r\n"
    ).encode("utf-8")
    footer = f"\r\n--{BOUNDARY}--\r\n".encode("utf-8")
    return header + content + footer


def post_multipart_urllib(url: str, field_name: str, filename: str, content: bytes, content_type: str = "text/csv") -> tuple[int, str]:
    from urllib.error import HTTPError
    from urllib.request import Request, urlopen

    body = _build_multipart_body(field_name, filename, content, content_type)
    req = Request(
        url,
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={BOUNDARY}"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def post_multipart(url: str, field_name: str, filename: str, content: bytes, content_type: str = "text/csv") -> tuple[int, str]:
    try:
        import requests
        files = {field_name: (filename, content, content_type)}
        resp = requests.post(url, files=files, timeout=120)
        return resp.status_code, resp.text
    except ImportError:
        return post_multipart_urllib(url, field_name, filename, content, content_type)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def json_list_to_csv_bytes(records: list[dict]) -> bytes:
    if not records:
        return b""
    fields = list(records[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue().encode("utf-8")


def extract_imported_count(body: str) -> int | None:
    try:
        data = json.loads(body)
        for key in ("imported_count", "imported", "reviews_imported"):
            if key in data:
                return int(data[key])
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None


# ---------------------------------------------------------------------------
# Task 1a — Reviews CSV
# ---------------------------------------------------------------------------

def import_reviews() -> None:
    print(f"\n{'='*60}")
    print(f"STEP 1 — Import reviews")
    print(f"  File : {REVIEWS_CSV}")

    if not REVIEWS_CSV.exists():
        print(f"  ERROR: File not found: {REVIEWS_CSV}")
        return

    content = REVIEWS_CSV.read_bytes()
    print(f"  Size : {len(content):,} bytes")

    url = f"{BASE_URL}/reviews/import-csv"
    print(f"  POST : {url}")
    status, body = post_multipart(url, "file", REVIEWS_CSV.name, content)
    print(f"  HTTP : {status}")
    print(f"  Body : {body[:600]}")

    imported = extract_imported_count(body)
    if imported is not None and imported == 0:
        print("\n  0 reviews imported via primary endpoint.")
        print("  Retrying with /data/reviews/import-csv-normalized ...")
        url2 = f"{BASE_URL}/data/reviews/import-csv-normalized"
        print(f"  POST : {url2}")
        status2, body2 = post_multipart(url2, "file", REVIEWS_CSV.name, content)
        print(f"  HTTP : {status2}")
        print(f"  Body : {body2[:600]}")


# ---------------------------------------------------------------------------
# Task 1b — Product catalog JSON → CSV → upload
# ---------------------------------------------------------------------------

def import_catalog() -> None:
    print(f"\n{'='*60}")
    print(f"STEP 2 — Import product catalog")
    print(f"  File : {CATALOG_JSON}")

    if not CATALOG_JSON.exists():
        print(f"  ERROR: File not found: {CATALOG_JSON}")
        return

    raw = json.loads(CATALOG_JSON.read_text(encoding="utf-8"))
    products: list[dict] = raw if isinstance(raw, list) else [raw]
    print(f"  Products in JSON : {len(products)}")

    csv_bytes = json_list_to_csv_bytes(products)
    print(f"  Converted to CSV : {len(csv_bytes):,} bytes")

    url = f"{BASE_URL}/products/catalog/import-csv"
    print(f"  POST : {url}")
    status, body = post_multipart(url, "file", "product_catalog.csv", csv_bytes)
    print(f"  HTTP : {status}")
    print(f"  Body : {body[:600]}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Target backend : {BASE_URL}")
    import_reviews()
    import_catalog()
    print(f"\n{'='*60}")
    print("Done.")
