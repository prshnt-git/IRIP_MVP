from __future__ import annotations

import csv
import hashlib
import random
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


@dataclass
class CollectedReview:
    review_id: str
    product_id: str
    product_name: str
    brand: str | None
    marketplace: str
    source_url: str
    raw_text: str
    rating: str | None
    review_date: str | None
    review_title: str | None
    reviewer_name: str | None
    scraped_at: str


class FlipkartReviewCollector:
    """Flipkart review collector.

    Rules:
    - Collector writes CSV only.
    - Collector does not write to DB.
    - Product identity mapping remains in the normalizer/import pipeline.
    """

    BODY_SELECTORS = [
        "div.t-ZTKy",
        "div.ZmyHeo",
        "div.qwjRop",
        "div._6K-7Co",
    ]

    TITLE_SELECTORS = [
        "p._2-N8zT",
        "p.z9E0IG",
        "div._2-N8zT",
    ]

    RATING_SELECTORS = [
        "div._3LWZlK",
        "div.XQDdHH",
    ]

    REVIEWER_SELECTORS = [
        "p._2sc7ZR",
        "p.AwS1CA",
        "div._2V5EHH",
    ]

    JUNK_TOKENS = [
        "outer ring road",
        "devarabeesanahalli",
        "flipkart internet private limited",
        "registered office",
        "cin :",
        "telephone:",
        "contact us",
        "terms of use",
        "privacy policy",
        "social media",
        "seller",
        "advertisement",
    ]

    METADATA_ONLY_PATTERNS = [
        "review for:",
        "verified purchase",
        "certified buyer",
        "flipkart customer",
    ]

    REVIEW_SIGNALS = [
        "certified buyer",
        "read more",
        "helpful",
        "very good",
        "good",
        "bad",
        "awesome",
        "excellent",
        "value for money",
        "camera",
        "battery",
        "speaker",
        "performance",
        "display",
        "charging",
        "heating",
        "network",
        "5g",
        "phone",
        "mobile",
    ]

    def __init__(
        self,
        max_reviews: int = 25,
        max_pages: int = 8,
        headful: bool = False,
        timeout_ms: int = 45000,
        debug_dir: str | Path = "data/scrape_debug",
    ) -> None:
        self.max_reviews = max_reviews
        self.max_pages = max_pages
        self.headful = headful
        self.timeout_ms = timeout_ms
        self.debug_dir = Path(debug_dir)
        self.debug_dir.mkdir(parents=True, exist_ok=True)

    def collect(
        self,
        product_id: str,
        product_name: str,
        brand: str | None,
        marketplace_url: str,
    ) -> list[CollectedReview]:
        if not marketplace_url or marketplace_url.lower() in {"unknown", "none", "null", "na", "n/a"}:
            print(f"[skip] {product_id}: missing marketplace_url")
            return []

        if not marketplace_url.startswith("http"):
            print(f"[skip] {product_id}: invalid URL: {marketplace_url}")
            return []

        reviews: list[CollectedReview] = []
        seen_ids: set[str] = set()
        seen_page_urls: set[str] = set()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=not self.headful)
            context = browser.new_context(
                viewport={"width": 1366, "height": 900},
                locale="en-IN",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            page.set_default_timeout(self.timeout_ms)

            try:
                print(f"[open] {product_name}: {marketplace_url}")
                page.goto(marketplace_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                self._human_delay()
                self._close_popups(page)

                review_url = self._find_review_url(page, marketplace_url)
                if review_url:
                    print(f"[reviews] {review_url}")
                    page.goto(review_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                    self._human_delay()
                else:
                    print("[warn] review URL not found; trying current product page")

                for page_no in range(1, self.max_pages + 1):
                    self._close_popups(page)
                    self._scroll_page(page)

                    current_url = page.url
                    if current_url in seen_page_urls:
                        print("[stop] repeated page URL")
                        break
                    seen_page_urls.add(current_url)

                    html = page.content()
                    page_reviews = self._parse_reviews_from_html(
                        html=html,
                        product_id=product_id,
                        product_name=product_name,
                        brand=brand,
                        source_url=current_url,
                    )

                    new_count = 0
                    for review in page_reviews:
                        if review.review_id not in seen_ids:
                            seen_ids.add(review.review_id)
                            reviews.append(review)
                            new_count += 1

                        if len(reviews) >= self.max_reviews:
                            break

                    print(f"[page {page_no}] found={len(page_reviews)} new={new_count} total={len(reviews)}")

                    if len(reviews) >= self.max_reviews:
                        break

                    next_url = self._find_next_url(page)
                    if next_url:
                        page.goto(next_url, wait_until="domcontentloaded", timeout=self.timeout_ms)
                        self._human_delay()
                        continue

                    if self._click_next(page):
                        self._human_delay()
                        continue

                    print("[stop] no next page")
                    break

                if not reviews:
                    self._save_debug(page, product_id)

            except Exception as exc:
                print(f"[error] {product_id}: {exc}")
                self._save_debug(page, product_id)
            finally:
                context.close()
                browser.close()

        return reviews[: self.max_reviews]

    def write_csv(self, reviews: list[CollectedReview], output_path: str | Path, append: bool = False) -> None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fields = [
            "review_id",
            "product_id",
            "product_name",
            "brand",
            "marketplace",
            "source_url",
            "raw_text",
            "rating",
            "review_date",
            "review_title",
            "reviewer_name",
            "scraped_at",
        ]

        mode = "a" if append and output_path.exists() else "w"
        write_header = mode == "w"

        with output_path.open(mode, encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            if write_header:
                writer.writeheader()
            for review in reviews:
                writer.writerow(asdict(review))

    def _find_review_url(self, page, base_url: str) -> str | None:
        anchors = page.evaluate(
            """() => Array.from(document.querySelectorAll('a')).map(a => ({
                text: (a.innerText || '').trim(),
                href: a.getAttribute('href') || ''
            }))"""
        )

        candidates: list[tuple[int, str]] = []

        for anchor in anchors:
            text = str(anchor.get("text") or "").lower()
            href = str(anchor.get("href") or "")
            if not href:
                continue

            full_url = urljoin(base_url, href)
            lower_url = full_url.lower()

            if "product-reviews" not in lower_url and "review" not in lower_url:
                continue

            score = 0
            if "product-reviews" in lower_url:
                score += 20
            if "all" in text and "review" in text:
                score += 20
            if "ratings" in text and "reviews" in text:
                score += 10
            if "review" in text:
                score += 5

            if "an=" in lower_url or "aid=" in lower_url or "cat=" in lower_url or "vert=" in lower_url:
                score -= 12

            candidates.append((score, self._normalize_flipkart_review_url(full_url)))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _normalize_flipkart_review_url(self, url: str) -> str:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)

        keep: dict[str, str] = {}
        for key in ["pid", "lid"]:
            if key in query and query[key]:
                keep[key] = query[key][0]

        clean_query = urlencode(keep)

        return urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                "",
                clean_query,
                "",
            )
        )

    def _parse_reviews_from_html(
        self,
        html: str,
        product_id: str,
        product_name: str,
        brand: str | None,
        source_url: str,
    ) -> list[CollectedReview]:
        soup = BeautifulSoup(html, "html.parser")
        cards = self._find_review_cards(soup)
        reviews: list[CollectedReview] = []

        for card in cards:
            raw_text = self._extract_body(card)
            if not raw_text or len(raw_text) < 8:
                continue

            # Only reject obvious site/footer/address junk here.
            # Metadata-only and weak-review filtering is handled by review_cleaner.py.
            if self._is_hard_junk_text(raw_text):
                continue

            rating = self._extract_rating(card)
            title = self._extract_title(card)
            reviewer = self._extract_reviewer(card)
            review_date = self._extract_date(card)

            review_id = self._make_review_id(
                marketplace="flipkart",
                product_name=product_name,
                product_id=product_id,
                raw_text=raw_text,
                rating=rating,
                review_date=review_date,
            )

            reviews.append(
                CollectedReview(
                    review_id=review_id,
                    product_id=product_id,
                    product_name=product_name,
                    brand=brand,
                    marketplace="flipkart",
                    source_url=source_url,
                    raw_text=raw_text,
                    rating=rating,
                    review_date=review_date,
                    review_title=title,
                    reviewer_name=reviewer,
                    scraped_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                )
            )

        return reviews

    def _find_review_cards(self, soup: BeautifulSoup) -> list[Any]:
        candidates = []

        body_selectors = [
            "div.t-ZTKy",
            "div.ZmyHeo",
            "div.qwjRop",
            "div._6K-7Co",
        ]

        for selector in body_selectors:
            for node in soup.select(selector):
                parent = node
                for _ in range(5):
                    if not parent:
                        break
                    text = self._clean_text(parent.get_text(" ", strip=True))
                    if self._looks_like_review_card(text):
                        candidates.append(parent)
                        break
                    parent = parent.parent

        for selector in ["div._27M-vq", "div.col.EPCmJX"]:
            for card in soup.select(selector):
                text = self._clean_text(card.get_text(" ", strip=True))
                if self._looks_like_review_card(text):
                    candidates.append(card)

        if not candidates:
            for card in soup.find_all("div"):
                text = self._clean_text(card.get_text(" ", strip=True))
                if self._looks_like_review_card(text):
                    candidates.append(card)

        seen = set()
        unique = []
        for card in candidates:
            text = self._clean_text(card.get_text(" ", strip=True))
            key = hashlib.sha1(text.encode("utf-8")).hexdigest()
            if key not in seen:
                seen.add(key)
                unique.append(card)

        return unique

    def _looks_like_review_card(self, text: str) -> bool:
        if len(text) < 20 or len(text) > 2500:
            return False

        lower = text.lower()

        if self._is_hard_junk_text(lower):
            return False

        has_rating = bool(re.search(r"\b[1-5](?:\.\d)?\s*(?:★|star|stars)?\b", lower))
        has_review_signal = any(token in lower for token in self.REVIEW_SIGNALS)
        has_enough_words = len(text.split()) >= 4

        # Keep broad candidates. Cleaner will remove metadata-only rows later.
        return has_rating and has_enough_words and (
            has_review_signal
            or "review for:" in lower
            or "verified purchase" in lower
            or "flipkart customer" in lower
        )

    def _extract_body(self, card: Any) -> str | None:
        # Prefer known Flipkart body nodes.
        for selector in self.BODY_SELECTORS:
            nodes = card.select(selector)
            body_candidates = [
                self._clean_text(node.get_text(" ", strip=True))
                for node in nodes
                if self._clean_text(node.get_text(" ", strip=True))
            ]
            body_candidates = [
                value
                for value in body_candidates
                if len(value) >= 8 and not self._is_hard_junk_text(value)
            ]
            if body_candidates:
                return max(body_candidates, key=len)

        # Fallback: keep broad lines. Cleaner will reject weak/metadata-only rows.
        lines = self._text_lines(card)
        lines = [
            line
            for line in lines
            if len(line) >= 8 and not self._is_hard_junk_text(line)
        ]

        if not lines:
            return None

        # Prefer longer opinion-like lines, but do not over-filter here.
        return max(lines, key=lambda value: (self._has_any_review_signal(value), len(value)))

    def _extract_rating(self, card: Any) -> str | None:
        for selector in self.RATING_SELECTORS:
            node = card.select_one(selector)
            if node:
                text = self._clean_text(node.get_text(" ", strip=True))
                match = re.search(r"\b([1-5](?:\.\d)?)\b", text)
                if match:
                    return match.group(1)

        text = self._clean_text(card.get_text(" ", strip=True))
        match = re.search(r"\b([1-5](?:\.\d)?)\s*(?:★|star|stars)?\b", text.lower())
        return match.group(1) if match else None

    def _extract_title(self, card: Any) -> str | None:
        for selector in self.TITLE_SELECTORS:
            node = card.select_one(selector)
            if node:
                text = self._clean_text(node.get_text(" ", strip=True))
                if 2 <= len(text) <= 120 and not self._is_junk_text(text):
                    return text

        for line in self._text_lines(card):
            lower = line.lower()
            if 2 <= len(line) <= 80:
                if "certified buyer" not in lower and "read more" not in lower and not self._is_junk_text(line):
                    if not re.fullmatch(r"[1-5](?:\.\d)?", line):
                        return line

        return None

    def _extract_reviewer(self, card: Any) -> str | None:
        for selector in self.REVIEWER_SELECTORS:
            node = card.select_one(selector)
            if node:
                text = self._clean_text(node.get_text(" ", strip=True))
                if text and len(text) <= 80 and not self._is_junk_text(text):
                    return text
        return None

    def _extract_date(self, card: Any) -> str | None:
        text = self._clean_text(card.get_text(" ", strip=True))
        match = re.search(
            r"\b(\d{1,2}\s+[A-Za-z]{3,9},?\s+\d{4}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\b",
            text,
        )
        return match.group(1) if match else None

    def _find_next_url(self, page) -> str | None:
        try:
            anchors = page.evaluate(
                """() => Array.from(document.querySelectorAll('a')).map(a => ({
                    text: (a.innerText || '').trim(),
                    href: a.getAttribute('href') || ''
                }))"""
            )
        except Exception:
            return None

        for anchor in anchors:
            text = str(anchor.get("text") or "").strip().lower()
            href = str(anchor.get("href") or "")
            if text == "next" and href:
                return urljoin(page.url, href)

        parsed = urlparse(page.url)
        query = parse_qs(parsed.query)
        page_value = query.get("page", ["1"])[0]
        current_page = int(page_value) if str(page_value).isdigit() else 1

        if current_page < self.max_pages:
            query["page"] = [str(current_page + 1)]
            clean_query = urlencode({key: value[0] for key, value in query.items() if value})
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", clean_query, ""))

        return None

    def _click_next(self, page) -> bool:
        selectors = [
            "a:has-text('Next')",
            "button:has-text('Next')",
            "span:has-text('Next')",
        ]

        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    locator.click(timeout=5000)
                    return True
            except Exception:
                continue

        return False

    def _close_popups(self, page) -> None:
        selectors = [
            "button:has-text('✕')",
            "button:has-text('×')",
            "button:has-text('Close')",
            "span:has-text('✕')",
        ]

        for selector in selectors:
            try:
                locator = page.locator(selector).first
                if locator.count() > 0:
                    locator.click(timeout=2000)
                    time.sleep(0.5)
            except Exception:
                continue

    def _scroll_page(self, page) -> None:
        try:
            page.evaluate(
                """async () => {
                    for (let i = 0; i < 4; i++) {
                        window.scrollBy(0, Math.floor(window.innerHeight * 0.8));
                        await new Promise(r => setTimeout(r, 450));
                    }
                }"""
            )
        except Exception:
            pass

    def _save_debug(self, page, product_id: str) -> None:
        safe_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", product_id)
        try:
            (self.debug_dir / f"{safe_id}.html").write_text(page.content(), encoding="utf-8")
        except Exception:
            pass
        try:
            page.screenshot(path=str(self.debug_dir / f"{safe_id}.png"), full_page=True)
        except Exception:
            pass

    def _text_lines(self, card: Any) -> list[str]:
        text = card.get_text("\n", strip=True)
        return [self._clean_text(line) for line in text.splitlines() if self._clean_text(line)]

    def _clean_text(self, value: str | None) -> str:
        if not value:
            return ""
        text = str(value)
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _has_any_review_signal(self, value: str | None) -> bool:
        if not value:
            return False
        lower = str(value).lower()
        return any(signal in lower for signal in self.REVIEW_SIGNALS)

    def _is_hard_junk_text(self, value: str | None) -> bool:
        if not value:
            return True

        lower = str(value).lower()
        hard_junk = [
            "outer ring road",
            "devarabeesanahalli",
            "flipkart internet private limited",
            "registered office",
            "cin :",
            "telephone:",
            "contact us",
            "terms of use",
            "privacy policy",
            "social media",
        ]
        return any(token in lower for token in hard_junk)

    def _is_junk_text(self, value: str | None) -> bool:
        if not value:
            return True

        text = str(value).strip()
        lower = text.lower()

        if any(token in lower for token in self.JUNK_TOKENS):
            return True

        # Reject rows that are only metadata, not review opinions.
        if any(lower == token for token in self.METADATA_ONLY_PATTERNS):
            return True

        if lower.startswith("review for:"):
            return True

        if lower in {"verified purchase", "flipkart customer", "read more"}:
            return True

        # Reject variant/spec-only strings.
        variant_tokens = ["color", "ram", "storage"]
        if "•" in text and sum(1 for token in variant_tokens if token in lower) >= 2:
            return True

        # Reject very short person/name-like strings.
        words = text.split()
        if len(words) <= 3 and not any(signal in lower for signal in self.REVIEW_SIGNALS):
            return True

        return False

    def _make_review_id(
        self,
        marketplace: str,
        product_name: str,
        product_id: str,
        raw_text: str,
        rating: str | None,
        review_date: str | None,
    ) -> str:
        payload = "|".join(
            [
                marketplace,
                product_name.lower().strip(),
                product_id.lower().strip(),
                self._clean_text(raw_text).lower(),
                rating or "",
                review_date or "",
            ]
        )
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]
        return f"{marketplace}_{digest}"

    def _human_delay(self) -> None:
        time.sleep(random.uniform(1.5, 3.5))
