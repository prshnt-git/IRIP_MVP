from __future__ import annotations

import csv
import io
import uuid
import urllib.request
from urllib.parse import urlparse

from pydantic import ValidationError

from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.imports import ImportErrorItem, ImportResult
from app.schemas.review import ReviewInput


class ReviewImportService:
    """CSV/URL review importer with normalization and duplicate-safe persistence."""

    REQUIRED_COLUMNS = {"product_id", "raw_text"}
    OPTIONAL_SOURCE_COLUMNS = {
        "source",
        "marketplace",
        "marketplace_product_id",
        "marketplace_review_id",
        "source_url",
        "reviewer_hash",
        "review_title",
        "verified_purchase",
        "helpful_votes",
    }

    def __init__(self, repository: ReviewRepository, analyzer: ReviewAnalyzer | None = None) -> None:
        self.repository = repository
        self.analyzer = analyzer or ReviewAnalyzer()

    def import_csv_url(self, url: str) -> ImportResult:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="Only http/https CSV URLs are supported")],
            )

        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "IRIP-MVP/0.1 CSV importer"},
            )
            with urllib.request.urlopen(request, timeout=20) as response:
                raw_bytes = response.read(5_000_001)
        except Exception as exc:  # pragma: no cover - network failures vary by environment
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Could not fetch CSV URL: {exc}")],
            )

        if len(raw_bytes) > 5_000_000:
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="CSV URL response is larger than 5 MB limit for MVP")],
            )

        csv_text = raw_bytes.decode("utf-8-sig")
        return self.import_csv_text(csv_text, discovered_via="csv_url")

    def import_csv_text(self, csv_text: str, discovered_via: str = "csv_upload") -> ImportResult:
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="CSV has no header row")],
            )

        normalized_headers = {header.strip() for header in reader.fieldnames if header}
        missing = sorted(self.REQUIRED_COLUMNS - normalized_headers)
        if missing:
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Missing required column(s): {', '.join(missing)}")],
            )

        imported = 0
        skipped_duplicates = 0
        attached_sources = 0
        possible_duplicates = 0
        errors: list[ImportErrorItem] = []
        product_ids: set[str] = set()

        for row_number, raw_row in enumerate(reader, start=2):
            row = {
                key.strip(): (value.strip() if isinstance(value, str) else value)
                for key, value in raw_row.items()
                if key
            }
            try:
                review = self._row_to_review(row)
                source_metadata = self._row_to_source_metadata(row, discovered_via=discovered_via)
                analysis = self.analyzer.analyze(review)
                save_result = self.repository.save_review_analysis(
                    review,
                    analysis,
                    source_metadata=source_metadata,
                )
                product_ids.add(review.product_id)

                if save_result.get("duplicate"):
                    skipped_duplicates += 1
                    attached_sources += 1
                    if save_result.get("duplicate_type") != "source_review_key":
                        possible_duplicates += 1
                else:
                    imported += 1
            except (ValidationError, ValueError) as exc:
                errors.append(ImportErrorItem(row_number=row_number, reason=str(exc)))

        return ImportResult(
            imported_count=imported,
            failed_count=len(errors),
            errors=errors[:50],
            product_ids=sorted(product_ids),
            skipped_duplicate_count=skipped_duplicates,
            attached_source_count=attached_sources,
            possible_duplicate_count=possible_duplicates,
        )

    def _row_to_review(self, row: dict[str, str]) -> ReviewInput:
        raw_text = row.get("raw_text") or row.get("review_text") or row.get("body") or ""
        product_id = _normalize_id(row.get("product_id") or "")
        review_id = row.get("review_id") or self._stable_review_id(row, product_id, raw_text)
        return ReviewInput(
            review_id=review_id,
            product_id=product_id,
            product_name=_empty_to_none(row.get("product_name")) or _empty_to_none(row.get("model")),
            source=_empty_to_none(row.get("source")) or _empty_to_none(row.get("marketplace")),
            rating=_to_float_or_none(row.get("rating")),
            review_date=_empty_to_none(row.get("review_date")) or _empty_to_none(row.get("date")),
            raw_text=raw_text,
            verified_purchase=_to_bool_or_none(row.get("verified_purchase")),
            helpful_votes=_to_int_or_none(row.get("helpful_votes")),
        )

    def _row_to_source_metadata(self, row: dict[str, str], discovered_via: str) -> dict:
        return {
            "marketplace": _empty_to_none(row.get("marketplace")) or _empty_to_none(row.get("source")),
            "marketplace_product_id": _empty_to_none(row.get("marketplace_product_id"))
            or _empty_to_none(row.get("asin"))
            or _empty_to_none(row.get("fsn")),
            "marketplace_review_id": _empty_to_none(row.get("marketplace_review_id"))
            or _empty_to_none(row.get("external_review_id")),
            "source_url": _empty_to_none(row.get("source_url"))
            or _empty_to_none(row.get("review_url"))
            or _empty_to_none(row.get("product_url")),
            "reviewer_hash": _empty_to_none(row.get("reviewer_hash"))
            or _empty_to_none(row.get("reviewer_id")),
            "review_title": _empty_to_none(row.get("review_title")) or _empty_to_none(row.get("title")),
            "discovered_via": discovered_via,
        }

    @staticmethod
    def _stable_review_id(row: dict[str, str], product_id: str, raw_text: str) -> str:
        source = row.get("source") or row.get("marketplace") or "unknown_source"
        marketplace_product_id = row.get("marketplace_product_id") or row.get("asin") or row.get("fsn") or product_id
        marketplace_review_id = row.get("marketplace_review_id") or row.get("external_review_id")
        review_date = row.get("review_date") or row.get("date") or "unknown_date"
        reviewer_hash = row.get("reviewer_hash") or row.get("reviewer_id") or "unknown_reviewer"

        if marketplace_review_id:
            seed = f"{source}|{marketplace_product_id}|{marketplace_review_id}"
        else:
            seed = f"{source}|{marketplace_product_id}|{review_date}|{reviewer_hash}|{raw_text}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def _normalize_id(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _empty_to_none(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


def _to_float_or_none(value: str | None) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _to_int_or_none(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    return int(float(value))


def _to_bool_or_none(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "verified", "verified_purchase"}:
        return True
    if normalized in {"false", "0", "no", "n", "unverified"}:
        return False
    return None
