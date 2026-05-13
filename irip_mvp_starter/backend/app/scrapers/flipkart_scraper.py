"""Flipkart review scraper for IRIP.

Runs in GitHub Actions, NOT on Render. Key differences from the Amazon scraper:
  - Session seeded with homepage GET in __init__ to capture initial cookies
  - No stable id attribute on review elements → synthesise review_id from text hash
  - CSS class names change frequently → each field uses a cascade of fallback selectors
  - Relative dates ("2 months ago") parsed alongside absolute ("15 March, 2025")
  - Flipkart 429/503 blocks are common → _get_with_retry with exponential backoff

Typical daily-run usage:
    scraper = FlipkartReviewScraper(delay_seconds=2)
    reviews = scraper.scrape_recent_only(
        product_url="https://www.flipkart.com/infinix-hot-50-pro/p/itmf7c3cd1e84593",
        product_id="infinix_hot_50_pro",
        product_name="Infinix Hot 50 Pro",
        brand="Infinix",
        days_back=30,
    )
"""
from __future__ import annotations

import hashlib
import re
import time
from datetime import date, timedelta
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

# ============================================================
# Module-level helpers
# ============================================================

_MONTH_MAP: dict[str, int] = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_relative_date(text: str) -> str:
    """Convert relative Flipkart date strings to YYYY-MM-DD.

    Handles:
      "2 months ago"  → today − 60 days
      "1 month ago"   → today − 30 days
      "3 weeks ago"   → today − 21 days
      "5 days ago"    → today − 5 days
      "yesterday"     → today − 1 day
      "today" / "just now" / "a few hours ago" → today
    Returns "" if no relative pattern is detected.
    """
    lower = text.lower().strip()
    today = date.today()

    # Immediate / same-day
    if any(x in lower for x in ("just now", "today", "hour ago", "hours ago",
                                  "minute ago", "minutes ago", "second")):
        return today.isoformat()

    if "yesterday" in lower:
        return (today - timedelta(days=1)).isoformat()

    # "N days ago"
    m = re.search(r"(\d+)\s+day", lower)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()

    # "a day ago"
    if re.search(r"\ba\s+day", lower):
        return (today - timedelta(days=1)).isoformat()

    # "N weeks ago"
    m = re.search(r"(\d+)\s+week", lower)
    if m:
        return (today - timedelta(weeks=int(m.group(1)))).isoformat()

    # "a week ago"
    if re.search(r"\ba\s+week", lower):
        return (today - timedelta(weeks=1)).isoformat()

    # "N months ago"
    m = re.search(r"(\d+)\s+month", lower)
    if m:
        return (today - timedelta(days=30 * int(m.group(1)))).isoformat()

    # "a month ago"
    if re.search(r"\ba\s+month", lower):
        return (today - timedelta(days=30)).isoformat()

    # "N years ago"
    m = re.search(r"(\d+)\s+year", lower)
    if m:
        return (today - timedelta(days=365 * int(m.group(1)))).isoformat()

    return ""


def _parse_flipkart_date(date_text: str) -> str:
    """Convert Flipkart date strings to YYYY-MM-DD.

    Tries relative parsing first (Flipkart often shows "2 months ago"),
    then falls back to absolute patterns:
      "15 Mar, 2025"   → "2025-03-15"
      "15th March 2025"→ "2025-03-15"
      "Mar 2025"       → "2025-03-01"  (day unknown, defaults to 1st)
    Returns "" on any parse failure.
    """
    text = date_text.strip()
    lower = text.lower()

    # ── Relative date (highest priority on Flipkart) ─────────────────────────
    if "ago" in lower or "yesterday" in lower or "today" in lower or "hour" in lower:
        result = _parse_relative_date(lower)
        if result:
            return result

    # ── Pattern A: day + month name + year ───────────────────────────────────
    # Matches "15 Mar, 2025" / "3rd April, 2024" / "15 March 2025"
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(\d{4})",
        text,
    )
    if m:
        day_str, month_str, year_str = m.groups()
        month_num = _MONTH_MAP.get(month_str.lower()[:3])
        if month_num:
            try:
                return date(int(year_str), month_num, int(day_str)).isoformat()
            except ValueError:
                pass

    # ── Pattern B: month name + year only ────────────────────────────────────
    # Matches "Mar 2025" / "March 2025"
    m = re.search(r"([A-Za-z]+)\s+(\d{4})", text)
    if m:
        month_str, year_str = m.groups()
        month_num = _MONTH_MAP.get(month_str.lower()[:3])
        if month_num:
            try:
                return date(int(year_str), month_num, 1).isoformat()
            except ValueError:
                pass

    return ""


def _stable_hash(text: str) -> str:
    """Return a 16-char hex digest of *text* — stable across Python processes."""
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:16]


def _text(element: Tag | None, sep: str = " ") -> str:
    """Safe get_text; returns '' if element is None."""
    if element is None:
        return ""
    return element.get_text(separator=sep, strip=True)


def _find_first(
    container: Tag,
    selectors: list[tuple[str, dict[str, Any]]],
) -> Tag | None:
    """Try each (tag, attrs) selector in order; return the first match or None."""
    for tag, attrs in selectors:
        el = container.find(tag, attrs)  # type: ignore[arg-type]
        if el is not None:
            return el  # type: ignore[return-value]
    return None


# ============================================================
# Selector tables  (CSS classes change; multiple fallbacks per field)
# ============================================================

# Each entry is (html_tag, {attr: value_or_compiled_regex}).
# Specific class names listed first (exact match = fastest), regex fallbacks last.

_RATING_SELECTORS: list[tuple[str, dict[str, Any]]] = [
    ("div", {"class": "_3LWZlK"}),                       # primary (2022-2024)
    ("div", {"class": "X_4Vge"}),                         # alternate layout
    ("span", {"class": re.compile(r"_1l_f")}),            # partial class match
    ("div", {"class": re.compile(r"LWZlK")}),             # regex fallback (handles prefix variants)
    ("div", {"class": re.compile(r"1lRcqv")}),            # 2023+ variant
    ("span", {"class": re.compile(r"XQDdHH")}),           # 2024+ variant
    ("span", {"class": re.compile(r"rating|star", re.I)}),
]

_TITLE_SELECTORS: list[tuple[str, dict[str, Any]]] = [
    ("p", {"class": "_2-N8zT"}),                          # primary (2022-2024)
    ("p", {"class": "YU7NEW"}),                            # 2024+ variant
    ("p", {"class": "z9E0IG"}),                            # alternate
    ("p", {"class": re.compile(r"2-N8zT|YU7NEW|z9E0IG")}),
    ("p", {"class": re.compile(r"title|head", re.I)}),
]

_BODY_SELECTORS: list[tuple[str, dict[str, Any]]] = [
    ("div", {"class": "t-ZTKy"}),                         # primary (2022-2024)
    ("div", {"class": "ZmyHeo"}),                          # alternate
    ("div", {"class": "qwjRop"}),                          # 2024+ variant
    ("div", {"class": re.compile(r"t-ZTKy|ZmyHeo|qwjRop")}),
    ("div", {"class": re.compile(r"review.?body|review.?text", re.I)}),
]

_DATE_SELECTORS: list[tuple[str, dict[str, Any]]] = [
    ("p", {"class": "_2sc7ZR"}),                          # primary (2022-2024)
    ("p", {"class": "_2NsTMU"}),                           # 2023+ variant
    ("span", {"class": "igWgfA"}),                         # 2024+ variant
    ("p", {"class": re.compile(r"2sc7ZR|NsTMU")}),
    ("span", {"class": re.compile(r"igWgfA")}),
    ("p", {"class": re.compile(r"date|time", re.I)}),
]

_CERTIFIED_SELECTORS: list[tuple[str, dict[str, Any]]] = [
    ("span", {"class": re.compile(r"2f_wVX|gP75ns")}),   # Certified Buyer badge
    ("div", {"class": re.compile(r"2f_wVX|gP75ns")}),
    ("span", {"class": re.compile(r"certified", re.I)}),
]

# Selectors for the review-list wrapper (holds all review cards as children)
_LIST_CONTAINER_CLASSES: list[str] = [
    "_1YokD2", "DOjaWF", "EGXnDv", "_3OzQFP", "_2kHMtA", "RYR41S",
]


# ============================================================
# Scraper class
# ============================================================


class FlipkartReviewScraper:
    """Polite Flipkart review scraper with retry logic and fallback CSS selectors.

    One instance per scraping job. The requests.Session is seeded with cookies
    from Flipkart's homepage in __init__, then warmed further with the specific
    product page before the first review fetch.
    """

    _BASE_URL: str = "https://www.flipkart.com/"

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
        "Referer": "https://www.flipkart.com/",
        "Connection": "keep-alive",
    }

    def __init__(self, delay_seconds: float = 2) -> None:
        self.delay_seconds = delay_seconds
        self._session = requests.Session()
        self._session.headers.update(self._HEADERS)
        self._session_warmed = False

        # Seed session with Flipkart homepage cookies immediately.
        # Flipkart's review endpoints validate session cookies; without this
        # initial GET the server returns empty or redirected responses.
        try:
            self._session.get(self._BASE_URL, timeout=10)
        except Exception:
            pass

    # ----------------------------------------------------------
    # Retry-aware GET
    # ----------------------------------------------------------

    def _get_with_retry(
        self,
        url: str,
        retries: int = 3,
    ) -> requests.Response | None:
        """GET *url*, retrying on 429/503 with exponential backoff (2s, 4s, 8s).

        Returns None after all attempts fail (never raises).
        """
        delay = 2.0
        for attempt in range(retries):
            try:
                response = self._session.get(url, timeout=15)
                if response.status_code in (429, 503):
                    if attempt < retries - 1:
                        time.sleep(delay)
                        delay *= 2
                    continue
                return response
            except Exception:
                if attempt < retries - 1:
                    time.sleep(delay)
                    delay *= 2

        return None

    # ----------------------------------------------------------
    # Product-page warm-up (lazy, per-product)
    # ----------------------------------------------------------

    def _warm_session(self, product_url: str) -> None:
        """GET the product page to seed product-specific session cookies.

        Runs at most once per scraper instance. The homepage GET in __init__
        handles global cookies; this covers product-page-specific ones.
        """
        if self._session_warmed:
            return
        try:
            self._get_with_retry(product_url, retries=2)
        except Exception:
            pass
        finally:
            self._session_warmed = True

    # ----------------------------------------------------------
    # URL building
    # ----------------------------------------------------------

    @staticmethod
    def _build_reviews_url(product_url: str, page: int) -> str:
        """Derive the review-page URL from a Flipkart product URL.

        Transforms:
          .../[slug]/p/[item-id]?pid=...
        to:
          .../[slug]/product-reviews/[item-id]?pid=...&sortOrder=MOST_RECENT&page={n}
        """
        reviews_url = re.sub(r"/p/", "/product-reviews/", product_url, count=1)
        parsed = urlparse(reviews_url)
        existing_qs = parse_qs(parsed.query)

        query: dict[str, str] = {}
        if "pid" in existing_qs:
            query["pid"] = existing_qs["pid"][0]
        query["sortOrder"] = "MOST_RECENT"
        query["page"] = str(page)

        return urlunparse(parsed._replace(query=urlencode(query), fragment=""))

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def get_reviews_page(
        self,
        product_url: str,
        page: int = 1,
    ) -> list[dict[str, Any]]:
        """Fetch and parse one page of Flipkart reviews.

        Args:
            product_url: Full Flipkart product URL (must contain ``/p/``).
            page: 1-based page number.

        Returns:
            List of review dicts. Empty list on any network or parse error —
            this method never raises.
        """
        self._warm_session(product_url)

        url = self._build_reviews_url(product_url, page)
        response = self._get_with_retry(url)
        if response is None or response.status_code != 200:
            return []

        content = response.text
        # Detect anti-bot walls / login redirects
        if (
            "captcha" in content.lower()
            or "robot" in content.lower()
            or "verify you are human" in content.lower()
            or len(content) < 2000  # suspiciously short → likely a redirect stub
        ):
            return []

        try:
            soup = BeautifulSoup(response.content, "lxml")
        except Exception:
            return []

        containers = self._find_review_containers(soup)
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
        product_url: str,
        product_id: str,
        product_name: str,
        brand: str,
        max_pages: int = 3,
    ) -> list[dict[str, Any]]:
        """Scrape up to *max_pages* pages and enrich each review with product metadata.

        Sleeps ``delay_seconds`` between pages. Stops early when a page yields
        no reviews (end of review list detected).
        """
        all_reviews: list[dict[str, Any]] = []

        for page in range(1, max_pages + 1):
            page_reviews = self.get_reviews_page(product_url, page)

            for review in page_reviews:
                review.update(
                    {
                        "source": "flipkart",
                        "marketplace": "flipkart_in",
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
        product_url: str,
        product_id: str,
        product_name: str,
        brand: str,
        days_back: int = 30,
    ) -> list[dict[str, Any]]:
        """Scrape up to 10 pages and return only reviews from the last *days_back* days.

        days_back defaults to 30 — Flipkart's "recent" sort mixes old reviews more
        than Amazon's, so a wider window ensures we catch all new activity.

        Reviews with an unparseable date are excluded (safer than including stale data).
        Reviews using relative dates ("2 months ago") are converted before filtering.
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()

        all_reviews = self.scrape_product(
            product_url=product_url,
            product_id=product_id,
            product_name=product_name,
            brand=brand,
            max_pages=10,
        )

        return [
            r for r in all_reviews
            if r.get("review_date", "") >= cutoff and r.get("review_date", "")
        ]

    # ----------------------------------------------------------
    # Container detection
    # ----------------------------------------------------------

    def _find_review_containers(self, soup: BeautifulSoup) -> list[Tag]:
        """Locate individual review card elements using three fallback strategies.

        Strategy 1 — named list containers:
            Find the div holding all review cards by known wrapper class names,
            then return its direct Tag children.

        Strategy 2 — anchor on rating elements:
            Find every element matching rating CSS patterns and walk up ≤6
            ancestors to find the nearest substantial text container.

        Strategy 3 — generic row scan:
            Return every ``div.row`` inside the first large-enough content div.
        """
        # Strategy 1: known wrapper class names
        for cls in _LIST_CONTAINER_CLASSES:
            wrapper = soup.find("div", class_=cls)
            if wrapper:
                children = [c for c in wrapper.children if isinstance(c, Tag)]
                if len(children) >= 1:
                    return children

        # Strategy 2: anchor on rating elements and walk up
        rating_anchors = soup.find_all(
            "div", class_=re.compile(r"LWZlK|lRcqv|XQDdHH|X_4Vge")
        )
        if rating_anchors:
            seen: set[int] = set()
            containers: list[Tag] = []
            for anchor in rating_anchors:
                node: Tag | None = anchor
                for _ in range(6):
                    node = node.parent  # type: ignore[assignment]
                    if node is None:
                        break
                    node_text = node.get_text(strip=True)
                    if len(node_text) > 60:
                        nid = id(node)
                        if nid not in seen:
                            seen.add(nid)
                            containers.append(node)
                        break
            if containers:
                return containers

        # Strategy 3: generic row scan
        for content_div in soup.find_all("div", class_=re.compile(r"\w{6,}")):
            rows = content_div.find_all("div", class_="row", recursive=False)
            if len(rows) >= 2:
                return rows

        return []

    # ----------------------------------------------------------
    # Per-review parsing
    # ----------------------------------------------------------

    def _parse_review_container(
        self, container: Tag
    ) -> dict[str, Any] | None:
        """Extract all fields from a single review card element.

        Returns None if the review body is empty or any unrecoverable error
        occurs (never raises).
        """
        try:
            # ── raw_text ─────────────────────────────────────────────────
            # Primary: div.t-ZTKy (or fallback). Flipkart nests text two divs deep.
            raw_text = ""
            body_elem = _find_first(container, _BODY_SELECTORS)
            if body_elem is not None:
                # dig into div.t-ZTKy > div > div for the actual prose
                inner = body_elem.find("div")
                if inner is not None:
                    deeper = inner.find("div")
                    raw_text = _text(deeper or inner)
                if not raw_text:
                    raw_text = _text(body_elem)
            if not raw_text:
                return None

            # ── review_id ────────────────────────────────────────────────
            # Flipkart has no stable id attr; synthesise from first 30 chars.
            review_id = f"fk_{_stable_hash(raw_text[:30])}"

            # ── rating ───────────────────────────────────────────────────
            # Tries div._3LWZlK first, then div.X_4Vge, then span[class*=_1l_f].
            rating: float | None = None
            rating_elem = _find_first(container, _RATING_SELECTORS)
            if rating_elem is not None:
                m = re.search(r"(\d+(?:\.\d+)?)", _text(rating_elem))
                if m:
                    value = float(m.group(1))
                    # Guard: some old layouts stored "50" meaning 5.0
                    rating = value / 10 if value > 5 else value

            # ── title ────────────────────────────────────────────────────
            title = ""
            title_elem = _find_first(container, _TITLE_SELECTORS)
            if title_elem is not None:
                title = _text(title_elem)

            # ── review_date ──────────────────────────────────────────────
            # Handles both "2 months ago" (relative) and "15 Mar, 2025" (absolute).
            review_date = ""
            date_elem = _find_first(container, _DATE_SELECTORS)
            if date_elem is not None:
                review_date = _parse_flipkart_date(_text(date_elem))

            # ── verified_purchase ("Certified Buyer") ─────────────────────
            certified_elem = _find_first(container, _CERTIFIED_SELECTORS)
            if certified_elem is not None:
                verified_purchase = True
            else:
                container_text = container.get_text()
                verified_purchase = "certified buyer" in container_text.lower()

            # ── helpful_votes ────────────────────────────────────────────
            # Flipkart does not surface a vote count in review card HTML.
            helpful_votes = 0

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
