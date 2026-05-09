from __future__ import annotations

import csv
import io
import re
from datetime import datetime
from typing import Any


REQUIRED_COLUMNS = {"product_id", "raw_text"}

RECOMMENDED_COLUMNS = {
    "review_id",
    "product_id",
    "product_name",
    "source",
    "rating",
    "review_date",
    "raw_text",
    "verified_purchase",
    "helpful_votes",
    "brand",
    "price_band",
    "own_brand",
}

DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ImportQualityService:
    def preview_csv_text(
        self,
        csv_text: str,
        sample_limit: int = 10,
    ) -> dict:
        reader = csv.DictReader(io.StringIO(csv_text))
        detected_columns = reader.fieldnames or []

        normalized_columns = {column.strip() for column in detected_columns if column}
        missing_required = sorted(REQUIRED_COLUMNS - normalized_columns)

        errors: list[dict] = []
        warnings: list[dict] = []
        valid_rows: list[dict[str, Any]] = []
        seen_review_ids: set[str] = set()

        if missing_required:
            return {
                "valid_count": 0,
                "failed_count": 1,
                "warning_count": 0,
                "required_columns_present": False,
                "detected_columns": detected_columns,
                "errors": [
                    {
                        "row_number": 0,
                        "reason": f"Missing required column(s): {', '.join(missing_required)}",
                    }
                ],
                "warnings": [],
                "sample_valid_rows": [],
            }

        for row_number, row in enumerate(reader, start=2):
            normalized = self._normalize_row(row)
            row_errors = self._validate_row(normalized, row_number)
            row_warnings = self._warnings_for_row(normalized, row_number, seen_review_ids)

            if normalized.get("review_id"):
                seen_review_ids.add(str(normalized["review_id"]))

            if row_errors:
                errors.extend(row_errors)
                continue

            warnings.extend(row_warnings)
            valid_rows.append(normalized)

        return {
            "valid_count": len(valid_rows),
            "failed_count": len(errors),
            "warning_count": len(warnings),
            "required_columns_present": True,
            "detected_columns": detected_columns,
            "errors": errors,
            "warnings": warnings,
            "sample_valid_rows": valid_rows[:sample_limit],
        }

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}

        for key, value in row.items():
            if key is None:
                continue

            clean_key = key.strip()
            clean_value = value.strip() if isinstance(value, str) else value

            if clean_key == "product_id" and isinstance(clean_value, str):
                clean_value = clean_value.strip().lower().replace(" ", "_")

            if clean_key == "source" and isinstance(clean_value, str):
                clean_value = clean_value.strip().lower()

            normalized[clean_key] = clean_value

        return normalized

    def _validate_row(self, row: dict[str, Any], row_number: int) -> list[dict]:
        errors: list[dict] = []

        product_id = str(row.get("product_id") or "").strip()
        raw_text = str(row.get("raw_text") or "").strip()

        if not product_id:
            errors.append(
                {
                    "row_number": row_number,
                    "reason": "product_id is empty.",
                }
            )

        if not raw_text:
            errors.append(
                {
                    "row_number": row_number,
                    "reason": "raw_text is empty.",
                }
            )

        if raw_text and len(raw_text) > 5000:
            errors.append(
                {
                    "row_number": row_number,
                    "reason": "raw_text is too long. Limit is 5000 characters.",
                }
            )

        rating = str(row.get("rating") or "").strip()
        if rating:
            try:
                rating_value = float(rating)
                if rating_value < 0 or rating_value > 5:
                    errors.append(
                        {
                            "row_number": row_number,
                            "reason": "rating must be between 0 and 5.",
                            "value": rating,
                        }
                    )
            except ValueError:
                errors.append(
                    {
                        "row_number": row_number,
                        "reason": "rating must be numeric.",
                        "value": rating,
                    }
                )

        review_date = str(row.get("review_date") or "").strip()
        if review_date:
            if not DATE_PATTERN.match(review_date):
                errors.append(
                    {
                        "row_number": row_number,
                        "reason": "review_date must use YYYY-MM-DD format.",
                        "value": review_date,
                    }
                )
            else:
                try:
                    datetime.strptime(review_date, "%Y-%m-%d")
                except ValueError:
                    errors.append(
                        {
                            "row_number": row_number,
                            "reason": "review_date is not a valid calendar date.",
                            "value": review_date,
                        }
                    )

        return errors

    def _warnings_for_row(
        self,
        row: dict[str, Any],
        row_number: int,
        seen_review_ids: set[str],
    ) -> list[dict]:
        warnings: list[dict] = []

        review_id = str(row.get("review_id") or "").strip()
        raw_text = str(row.get("raw_text") or "").strip()

        if not review_id:
            warnings.append(
                {
                    "row_number": row_number,
                    "reason": "review_id is missing. Import may auto-generate or dedupe less reliably.",
                    "value": None,
                }
            )
        elif review_id in seen_review_ids:
            warnings.append(
                {
                    "row_number": row_number,
                    "reason": "Duplicate review_id inside this file.",
                    "value": review_id,
                }
            )

        if len(raw_text) < 8:
            warnings.append(
                {
                    "row_number": row_number,
                    "reason": "raw_text is very short; sentiment may be weak.",
                    "value": raw_text,
                }
            )

        missing_recommended = sorted(
            column for column in RECOMMENDED_COLUMNS if column not in row or row.get(column) in {None, ""}
        )

        if missing_recommended:
            warnings.append(
                {
                    "row_number": row_number,
                    "reason": f"Missing recommended column value(s): {', '.join(missing_recommended[:5])}",
                    "value": None,
                }
            )

        return warnings