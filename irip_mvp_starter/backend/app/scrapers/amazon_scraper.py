"""Amazon.in review scraper for IRIP.

Runs in GitHub Actions, NOT on Render. Designed for:
  - Polite scraping (configurable delay between pages)
  - Resilient parsing (return [] on any page error, never raise)
  - Recent-only mode for daily automation (days_back=7)
  - Connection reuse via requests.Session

Typical daily-run usage:
    scraper = AmazonReviewScraper(delay_seconds=2)
    reviews = scraper.scrape_recent_only(
        asin="B0EXAMPLE",
        product_id="infinix_hot_50_pro",
        product_name="Infinix Hot 50 Pro",
        brand="Infinix",
        days_back=7,
    )
    # Pass `reviews` to ReviewImportService or dedup pipeline.
"""
from __future__ import annotations

import re
import time
from datetime import date, timedelta
from typing import Any

import requests
from bs4 import BeautifulSoup, Tag

# ============================================================
# Module-level helpers
# ============================================================

_MONTH_MAP: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_amazon_date(date_text: str) -> str:
    """Convert Amazon India date string to YYYY-MM-DD.

    Handles: "Reviewed in India on 15 March 2025"
    Returns "" on any parse failure so the caller can decide what to do.
    """
    match = re.search(
        r"on\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
        date_text,
        re.IGNORECASE,
    )
    if not match:
        return ""
    day_str, month_str, year_str = match.groups()
    month_num = _MONTH_MAP.get(month_str.lower())
    if not month_num:
        return ""
    try:
        return date(int(year_str), month_num, int(day_str)).isoformat()
    except ValueError:
        return ""


def _parse_helpful_votes(helpful_text: str) -> int:
    """Extract vote count from Amazon helpful-vote text.

    "42 people found this helpful"  → 42
    "One person found this helpful" → 1
    anything else / empty           → 0
    """
    if not helpful_text:
        return 0
    text = helpful_text.strip()
    if text.lower().startswith("one"):
        return 1
    match = re.search(r"(\d[\d,]*)", text)
    if match:
        return int(match.group(1).replace(",", ""))
    return 0


def _text(element: Tag | None, sep: str = " ") -> str:
    """Safe get_text with strip; returns '' if element is None."""
    if element is None:
        return ""
    return element.get_text(separator=sep, strip=True)


# ============================================================
# Scraper class
# ============================================================


class AmazonReviewScraper:
    """Polite Amazon.in review scraper.

    One instance per scraping job — reuses a requests.Session for
    connection pooling across pages of the same product.
    """

    _HEADERS: dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Connection": "keep-alive",
    }

    def __init__(self, delay_seconds: float = 2) -> None:
        self.delay_seconds = delay_seconds
        self._session = requests.Session()
        self._session.headers.update(self._HEADERS)

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def get_reviews_page(self, asin: str, page: int = 1) -> list[dict[str, Any]]:
        """Fetch and parse one page of reviews for *asin*.

        Args:
            asin: Amazon Standard Identification Number (e.g. "B0CX1234AB").
            page: 1-based page number.

        Returns:
            List of review dicts. Empty list on any network or parse error —
            this method never raises.
        """
        url = (
            f"https://www.amazon.in/product-reviews/{asin}"
            f"?pageNumber={page}&sortBy=recent"
        )
        try:
            response = self._session.get(url, timeout=15)
        except Exception:
            return []

        if response.status_code != 200:
            return []

        content = response.text
        if "captcha" in content.lower() or 'id="captchacharacters"' in content:
            return []

        try:
            soup = BeautifulSoup(response.content, "lxml")
        except Exception:
            return []

        containers = soup.find_all("div", {"data-hook": "review"})
        if not containers:
            return []

        reviews: list[dict[str, Any]] = []
        for container in containers:
            parsed = self._parse_review_container(container)
            if parsed is not None:
                reviews.append(parsed)

        return reviews

    def scrape_product(
        self,
        asin: str,
        product_id: str,
        product_name: str,
        brand: str,
        max_pages: int = 3,
    ) -> list[dict[str, Any]]:
        """Scrape up to *max_pages* pages and enrich each review with product metadata.

        Sleeps ``delay_seconds`` between pages. Stops early if a page returns no
        reviews (end of review list detected).

        Returns:
            All reviews across all pages, enriched with source/product fields.
        """
        all_reviews: list[dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            page_reviews = self.get_reviews_page(asin, page)

            for review in page_reviews:
                review.update(
                    {
                        "source": "amazon",
                        "marketplace": "amazon_in",
                        "product_id": product_id,
                        "product_name": product_name,
                        "brand": brand,
                        "price_band": "10000-35000",
                    }
                )
            all_reviews.extend(page_reviews)

            if not page_reviews:
                break

            if page < max_pages:
                time.sleep(self.delay_seconds)

        return all_reviews

    def scrape_recent_only(
        self,
        asin: str,
        product_id: str,
        product_name: str,
        brand: str,
        days_back: int = 7,
    ) -> list[dict[str, Any]]:
        """Scrape up to 5 pages and return only reviews from the last *days_back* days.

        Ideal for daily GitHub Actions runs — day 1 returns many reviews,
        subsequent runs return only the fresh handful posted since last run.

        Reviews with an unparseable date are excluded (safer than including stale data).
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()  # YYYY-MM-DD

        all_reviews = self.scrape_product(
            asin=asin,
            product_id=product_id,
            product_name=product_name,
            brand=brand,
            max_pages=5,
        )

        return [
            r for r in all_reviews
            if r.get("review_date", "") >= cutoff and r.get("review_date", "")
        ]

    # ----------------------------------------------------------
    # Private parsing helpers
    # ----------------------------------------------------------

    def _parse_review_container(
        self, container: Tag
    ) -> dict[str, Any] | None:
        """Extract all fields from a single ``div[data-hook="review"]`` element.

        Returns None if the review body is empty (delivery-only complaint, bot
        placeholder, etc.) or if any unrecoverable parse error occurs.
        """
        try:
            # ── review_id ────────────────────────────────────────────────
            review_id: str = container.get("id", "")  # type: ignore[assignment]

            # ── rating ───────────────────────────────────────────────────
            # Try the direct span first, then fall back to the <i> wrapper.
            rating: float | None = None
            rating_elem: Tag | None = container.find(  # type: ignore[assignment]
                "span", {"data-hook": "review-star-rating"}
            )
            if rating_elem is None:
                i_elem: Tag | None = container.find(  # type: ignore[assignment]
                    "i", {"data-hook": "review-star-rating"}
                )
                if i_elem is not None:
                    rating_elem = i_elem.find("span")  # type: ignore[assignment]
            if rating_elem is not None:
                m = re.search(r"(\d+(?:\.\d+)?)", _text(rating_elem))
                if m:
                    rating = float(m.group(1))

            # ── title ────────────────────────────────────────────────────
            # The title span often wraps star icons inside child spans;
            # grab the last non-empty child span to skip rating text.
            title = ""
            title_elem: Tag | None = container.find(  # type: ignore[assignment]
                "span", {"data-hook": "review-title"}
            )
            if title_elem is not None:
                child_texts = [
                    s.get_text(strip=True)
                    for s in title_elem.find_all("span")
                    if s.get_text(strip=True)
                ]
                title = child_texts[-1] if child_texts else _text(title_elem)

            # ── raw_text ─────────────────────────────────────────────────
            body_elem: Tag | None = container.find(  # type: ignore[assignment]
                "span", {"data-hook": "review-body"}
            )
            raw_text = _text(body_elem)
            if not raw_text:
                return None  # skip empty / unloaded reviews

            # ── review_date ──────────────────────────────────────────────
            date_elem: Tag | None = container.find(  # type: ignore[assignment]
                "span", {"data-hook": "review-date"}
            )
            review_date = _parse_amazon_date(_text(date_elem)) if date_elem else ""

            # ── verified_purchase ────────────────────────────────────────
            verified_elem = container.find("span", {"data-hook": "avp-badge"})
            verified_purchase: bool = verified_elem is not None

            # ── helpful_votes ────────────────────────────────────────────
            helpful_elem: Tag | None = container.find(  # type: ignore[assignment]
                "span", {"data-hook": "helpful-vote-statement"}
            )
            helpful_votes = _parse_helpful_votes(_text(helpful_elem))

            return {
                "review_id": review_id,
                "rating": rating,
                "title": title,
                "raw_text": raw_text,
                "review_date": review_date,
                "verified_purchase": verified_purchase,
                "helpful_votes": helpful_votes,
            }

        except Exception:
            return None
