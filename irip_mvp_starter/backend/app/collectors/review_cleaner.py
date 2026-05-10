from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ReviewCleanResult:
    input_count: int
    clean_count: int
    removed_count: int
    duplicate_count: int
    removed_examples: list[dict]


class ReviewCleaner:
    """Clean raw collected review CSV before importing into IRIP.

    Final clean CSV is both:
    1. presentation-ready review table
    2. import-ready dataset for /data/reviews/import-csv-normalized
    """

    REQUIRED_FIELDS = [
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
        "text_hash",
        "review_fingerprint",
        "cleaning_status",
    ]

    JUNK_TOKENS = [
        "outer ring road",
        "devarabeesanahalli",
        "flipkart internet private limited",
        "registered office",
        "telephone:",
        "terms of use",
        "privacy policy",
        "contact us",
        "social media",
        "seller",
        "advertisement",
        "verified purchase",
        "flipkart customer",
        "certified buyer",
        "read more",
    ]

    LOCATION_TOKENS = [
        "district",
        "village",
        "pincode",
        "pin code",
        "taluk",
        "mandal",
        "north twenty four parganas",
        "south twenty four parganas",
    ]

    OPINION_SIGNALS = [
        "good",
        "bad",
        "nice",
        "excellent",
        "awesome",
        "terrific",
        "brilliant",
        "poor",
        "average",
        "decent",
        "value",
        "money",
        "camera",
        "battery",
        "display",
        "performance",
        "speaker",
        "charging",
        "heating",
        "network",
        "5g",
        "phone",
        "mobile",
        "smooth",
        "fast",
        "slow",
        "backup",
        "budget",
        "price",
        "buy",
        "purchase",
        "recommend",
        "killer",
        "best",
        "beast",
        "worst",
        "superb",
        "fantastic",
        "satisfied",
        "unsatisfied",
        "issue",
        "problem",
        "friendly",
        "amazing",
        "impressive",
        "love",
        "hate",
        "useful",
        "better",
        "worse",
        "way better",
        "not good",
        "could be",
        "worth",
        "not worth",
        "ok",
        "okay",
    ]

    def clean_csv_path(
        self,
        input_path: str | Path,
        output_path: str | Path,
        report_path: str | Path,
    ) -> ReviewCleanResult:
        input_path = Path(input_path)
        output_path = Path(output_path)
        report_path = Path(report_path)

        rows = list(csv.DictReader(input_path.open(encoding="utf-8-sig")))
        clean_rows: list[dict[str, Any]] = []
        removed_examples: list[dict] = []
        seen: set[str] = set()
        duplicate_count = 0

        for index, row in enumerate(rows, start=2):
            normalized = self._normalize_row(row)
            reason = self._rejection_reason(normalized)

            if reason:
                if len(removed_examples) < 20:
                    removed_examples.append(
                        {
                            "row_number": index,
                            "reason": reason,
                            "product_name": normalized.get("product_name"),
                            "raw_text": normalized.get("raw_text"),
                            "review_title": normalized.get("review_title"),
                        }
                    )
                continue

            dedupe_key = normalized["review_fingerprint"]
            if dedupe_key in seen:
                duplicate_count += 1
                continue

            seen.add(dedupe_key)
            normalized["cleaning_status"] = "clean"
            clean_rows.append(normalized)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=self.REQUIRED_FIELDS)
            writer.writeheader()
            writer.writerows(clean_rows)

        result = ReviewCleanResult(
            input_count=len(rows),
            clean_count=len(clean_rows),
            removed_count=len(rows) - len(clean_rows) - duplicate_count,
            duplicate_count=duplicate_count,
            removed_examples=removed_examples,
        )

        report_path.write_text(
            json.dumps(
                {
                    "input_count": result.input_count,
                    "clean_count": result.clean_count,
                    "removed_count": result.removed_count,
                    "duplicate_count": result.duplicate_count,
                    "removed_examples": result.removed_examples,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        return result

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, str]:
        normalized: dict[str, str] = {}

        for field in self.REQUIRED_FIELDS:
            normalized[field] = self._clean_text(row.get(field, ""))

        normalized["raw_text"] = self._repair_review_text(normalized)
        normalized["review_title"] = self._clean_review_title(normalized.get("review_title", ""))
        normalized["rating"] = self._normalize_rating(normalized.get("rating"))

        if not normalized["product_id"]:
            normalized["product_id"] = self._resolve_product_id(
                product_name=normalized["product_name"],
                brand=normalized["brand"],
            )

        normalized["text_hash"] = self._hash_text(normalized["raw_text"])
        normalized["review_fingerprint"] = self._review_fingerprint(normalized)

        if not normalized["review_id"]:
            normalized["review_id"] = f'{normalized.get("marketplace") or "source"}_{normalized["review_fingerprint"][:20]}'

        return normalized

    def _resolve_product_id(self, product_name: str, brand: str | None = None) -> str:
        try:
            from app.services.product_identity_service import ProductIdentityService

            identity = ProductIdentityService().build_identity(
                product_name=product_name,
                brand=brand,
            )
            return identity.product_id
        except Exception:
            return self._slugify(product_name)

    def _clean_review_title(self, value: str) -> str:
        title = self._clean_text(value)
        lower = title.lower()

        if not title:
            return ""

        if lower.startswith("helpful for"):
            return ""

        if lower in {"verified purchase", "flipkart customer", "certified buyer", "read more"}:
            return ""

        if title.startswith(","):
            return ""

        if self._looks_helpful_count(title):
            return ""

        if self._looks_metadata_only(title):
            return ""

        return title

    def _repair_review_text(self, row: dict[str, str]) -> str:
        raw_text = self._clean_text(row.get("raw_text"))
        title = self._clean_text(row.get("review_title"))

        # If raw text is only helpful-count / metadata, try to salvage title.
        if self._looks_metadata_only(raw_text) or self._looks_helpful_count(raw_text):
            if self._is_useful_short_review(title):
                return title
            return raw_text

        # If raw is very short but title is more meaningful, use title.
        if len(raw_text.split()) <= 3 and self._is_useful_short_review(title):
            return title

        # If title has useful detail and raw text is generic, combine them.
        if self._is_generic_short_review(raw_text) and self._is_useful_short_review(title):
            if title.lower() != raw_text.lower():
                return f"{title}. {raw_text}"

        return raw_text

    def _looks_like_person_name(self, value: str | None) -> bool:
        text = self._clean_text(value)
        if not text:
            return True

        lower = text.lower()

        if self._has_opinion_signal(text):
            return False

        # Reject strings like "R A T A N KAR" or plain reviewer names.
        letters_only = text.replace(" ", "")
        if len(text.split()) >= 2 and letters_only.isalpha():
            uppercase_ratio = sum(1 for ch in text if ch.isupper()) / max(1, sum(1 for ch in text if ch.isalpha()))
            if uppercase_ratio > 0.7:
                return True

        if len(text.split()) <= 4 and letters_only.isalpha():
            common_name_markers = ["kumar", "kar", "singh", "rahman", "baig", "tiwari", "mankar"]
            if any(marker in lower for marker in common_name_markers):
                return True

        return False

    def _looks_helpful_count(self, value: str | None) -> bool:
        text = self._clean_text(value).lower()
        return bool(re.fullmatch(r"helpful\s+for\s+\d+", text))

    def _is_generic_short_review(self, value: str | None) -> bool:
        text = self._clean_text(value).lower()
        generic = {
            "good",
            "very good",
            "nice",
            "nice phone",
            "good phone",
            "best phone",
            "excellent",
            "awesome",
            "superb",
            "very good product",
        }
        return text in generic

    def _is_useful_short_review(self, value: str | None) -> bool:
        text = self._clean_text(value)
        if not text:
            return False

        lower = text.lower()

        if self._looks_metadata_only(text):
            return False

        if self._looks_helpful_count(text):
            return False

        if any(token in lower for token in self.JUNK_TOKENS + self.LOCATION_TOKENS):
            return False

        if self._looks_like_person_name(text):
            return False

        words = text.split()

        # Accept compact opinion rows like "Very Good Product", "Nice phone",
        # "Best Camera in low segment", "Infinix note50x model beast".
        if len(words) >= 2 and self._has_opinion_signal(text):
            return True

        # Accept slightly longer sentences even without known signal.
        if len(words) >= 5:
            return True

        return False

    def _rejection_reason(self, row: dict[str, str]) -> str | None:
        product_name = row.get("product_name", "")
        source_url = row.get("source_url", "")
        raw_text = row.get("raw_text", "")

        if not product_name:
            return "missing_product_name"

        if not source_url.startswith("http"):
            return "missing_source_url"

        if not raw_text:
            return "missing_raw_text"

        lower = raw_text.lower()

        if self._looks_helpful_count(raw_text):
            return "helpful_count_only"

        if self._looks_metadata_only(raw_text):
            return "metadata_only"

        if any(token in lower for token in self.JUNK_TOKENS):
            return "junk_token"

        if any(token in lower for token in self.LOCATION_TOKENS):
            return "location_or_address"

        if raw_text.startswith(",") or raw_text.endswith("District"):
            return "location_like_text"

        if self._looks_like_person_name(raw_text):
            return "name_like_text"

        # Accept useful short reviews. This is important for marketplace reviews.
        if self._is_useful_short_review(raw_text):
            return None

        if len(raw_text) < 15:
            return "too_short"

        if len(raw_text.split()) < 4 and not self._has_opinion_signal(raw_text):
            return "too_few_words"

        if not self._has_opinion_signal(raw_text) and len(raw_text.split()) < 8:
            return "no_opinion_signal"

        return None

    def _looks_metadata_only(self, value: str) -> bool:
        text = self._clean_text(value)
        lower = text.lower()

        if not text:
            return True

        if lower.startswith("review for:"):
            return True

        if lower in {
            "verified purchase",
            "flipkart customer",
            "certified buyer",
            "read more",
        }:
            return True

        if "•" in text:
            variant_hits = sum(
                token in lower
                for token in ["color", "ram", "storage", "gb", "variant"]
            )
            if variant_hits >= 2:
                return True

        if re.fullmatch(r"[\W\d_]+", text):
            return True

        words = text.split()
        if len(words) <= 3 and not self._has_opinion_signal(text):
            return True

        return False

    def _has_opinion_signal(self, value: str) -> bool:
        lower = value.lower()
        return any(signal in lower for signal in self.OPINION_SIGNALS)

    def _normalize_rating(self, value: str | None) -> str:
        text = self._clean_text(value)
        match = re.search(r"\b([1-5](?:\.\d)?)\b", text)
        if not match:
            return ""

        rating = match.group(1)
        if rating.endswith(".0"):
            rating = rating[:-2]

        return rating

    def _review_fingerprint(self, row: dict[str, str]) -> str:
        payload = "|".join(
            [
                row.get("marketplace", "").lower(),
                row.get("product_id", "").lower(),
                row.get("text_hash", ""),
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _hash_text(self, value: str) -> str:
        normalized_text = self._clean_text(value).lower()
        return hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()

    def _slugify(self, value: str) -> str:
        value = value.lower().strip()
        value = re.sub(r"[^a-z0-9]+", "_", value)
        return value.strip("_") or "unknown_product"

    def _clean_text(self, value: Any) -> str:
        if value is None:
            return ""

        text = str(value)
        text = text.replace("\xa0", " ")
        text = text.replace("\\_", "_")
        text = re.sub(r"\s+", " ", text).strip()
        return text
