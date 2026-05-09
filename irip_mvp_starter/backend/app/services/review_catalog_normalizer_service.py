from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.services.product_catalog_service import ProductCatalogService
from app.services.product_identity_service import ProductIdentityService


@dataclass
class ReviewNormalizationResult:
    output_rows: list[dict]
    imported_candidate_count: int
    skipped_count: int
    duplicate_count: int
    unresolved_count: int
    errors: list[dict]
    unresolved_rows: list[dict]


class ReviewCatalogNormalizerService:
    """Normalize messy review CSV rows against the IRIP product catalog.

    Purpose:
    - Accept CSVs from Google Sheet / manual export / future provider output.
    - Resolve messy product names to canonical product_id.
    - Standardize review columns into the existing IRIP review import schema.
    - Deduplicate within the incoming batch.
    - Keep unresolved rows out of the sentiment pipeline.
    """

    TEXT_COLUMNS = [
        "raw_text",
        "review_text",
        "review",
        "comment",
        "comments",
        "body",
        "text",
        "content",
    ]

    PRODUCT_COLUMNS = [
        "product_name",
        "product",
        "model_name",
        "model",
        "title",
        "item_name",
    ]

    def __init__(
        self,
        catalog_service: ProductCatalogService | None = None,
        identity_service: ProductIdentityService | None = None,
    ) -> None:
        self.catalog_service = catalog_service or ProductCatalogService()
        self.identity_service = identity_service or ProductIdentityService()

    def normalize_csv_path(self, input_path: str | Path) -> ReviewNormalizationResult:
        text = Path(input_path).read_text(encoding="utf-8-sig")
        return self.normalize_csv_text(text)

    def normalize_csv_text(self, csv_text: str) -> ReviewNormalizationResult:
        reader = csv.DictReader(csv_text.splitlines())

        if not reader.fieldnames:
            return ReviewNormalizationResult(
                output_rows=[],
                imported_candidate_count=0,
                skipped_count=0,
                duplicate_count=0,
                unresolved_count=0,
                errors=[{"row_number": None, "reason": "CSV has no header row."}],
                unresolved_rows=[],
            )

        output_rows: list[dict] = []
        unresolved_rows: list[dict] = []
        errors: list[dict] = []
        seen_fingerprints: set[str] = set()

        skipped_count = 0
        duplicate_count = 0
        unresolved_count = 0

        for row_number, row in enumerate(reader, start=2):
            try:
                raw_text = self._first_value(row, self.TEXT_COLUMNS)
                if not raw_text:
                    skipped_count += 1
                    errors.append(
                        {
                            "row_number": row_number,
                            "reason": "Missing review text.",
                            "row": row,
                        }
                    )
                    continue

                product_id_input = self._clean(row.get("product_id"))
                product_name_input = self._first_value(row, self.PRODUCT_COLUMNS)
                brand_input = self._clean(row.get("brand"))

                resolved = self._resolve_product(
                    product_id=product_id_input,
                    product_name=product_name_input,
                    brand=brand_input,
                )

                if not resolved["product"]:
                    unresolved_count += 1
                    unresolved_rows.append(
                        {
                            "row_number": row_number,
                            "reason": "Could not resolve product to catalog.",
                            "product_id": product_id_input,
                            "product_name": product_name_input,
                            "brand": brand_input,
                            "raw_text": raw_text,
                            "resolver": resolved,
                        }
                    )
                    continue

                product = resolved["product"]
                product_id = product["product_id"]
                product_name = product.get("product_name") or product_name_input or product_id
                brand = product.get("brand") or brand_input

                rating = self._clean(row.get("rating") or row.get("stars") or row.get("score"))
                review_date = self._clean(row.get("review_date") or row.get("date") or row.get("created_at"))
                marketplace = self._clean(row.get("marketplace") or row.get("source_marketplace")) or "manual_csv"
                source = self._clean(row.get("source")) or "catalog_normalized_csv"

                fingerprint = self._fingerprint(
                    product_id=product_id,
                    raw_text=raw_text,
                    rating=rating,
                    review_date=review_date,
                )

                if fingerprint in seen_fingerprints:
                    duplicate_count += 1
                    continue

                seen_fingerprints.add(fingerprint)

                review_id = self._clean(row.get("review_id") or row.get("id"))
                if not review_id:
                    review_id = f"norm_{fingerprint[:16]}"

                output_rows.append(
                    {
                        "review_id": review_id,
                        "product_id": product_id,
                        "product_name": product_name,
                        "brand": brand,
                        "source": source,
                        "marketplace": marketplace,
                        "raw_text": raw_text,
                        "rating": rating or "",
                        "review_date": review_date or "",
                    }
                )

            except Exception as exc:
                skipped_count += 1
                errors.append(
                    {
                        "row_number": row_number,
                        "reason": str(exc),
                        "row": row,
                    }
                )

        return ReviewNormalizationResult(
            output_rows=output_rows,
            imported_candidate_count=len(output_rows),
            skipped_count=skipped_count,
            duplicate_count=duplicate_count,
            unresolved_count=unresolved_count,
            errors=errors,
            unresolved_rows=unresolved_rows,
        )

    def write_outputs(
        self,
        result: ReviewNormalizationResult,
        output_csv_path: str | Path,
        report_json_path: str | Path,
    ) -> None:
        output_csv_path = Path(output_csv_path)
        report_json_path = Path(report_json_path)

        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        report_json_path.parent.mkdir(parents=True, exist_ok=True)

        fields = [
            "review_id",
            "product_id",
            "product_name",
            "brand",
            "source",
            "marketplace",
            "raw_text",
            "rating",
            "review_date",
        ]

        with output_csv_path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(result.output_rows)

        report = {
            "imported_candidate_count": result.imported_candidate_count,
            "skipped_count": result.skipped_count,
            "duplicate_count": result.duplicate_count,
            "unresolved_count": result.unresolved_count,
            "errors": result.errors,
            "unresolved_rows": result.unresolved_rows,
        }

        report_json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _resolve_product(
        self,
        product_id: str | None,
        product_name: str | None,
        brand: str | None,
    ) -> dict:
        catalog = self.catalog_service.list_catalog()

        if product_id:
            direct = self.catalog_service.get_product(product_id)
            if direct:
                identity = self.identity_service.build_identity(
                    product_name=direct.get("product_name") or product_id,
                    brand=direct.get("brand"),
                )
                return {
                    "status": "matched",
                    "match_type": "direct_product_id",
                    "confidence": 1.0,
                    "identity": identity.__dict__,
                    "product": direct,
                }

        if not product_name:
            return {
                "status": "unmatched",
                "match_type": "missing_product_name",
                "confidence": 0.0,
                "identity": None,
                "product": None,
            }

        resolved = self.catalog_service.resolve_product(product_name=product_name, brand=brand)
        if resolved.get("status") == "matched":
            return resolved

        identity = self.identity_service.build_identity(product_name=product_name, brand=brand)

        # Strong fallback: same normalized model key, even if brand is missing in review CSV.
        model_matches = [
            item
            for item in catalog
            if item.get("normalized_model_key") == identity.normalized_model_key
        ]

        if len(model_matches) == 1:
            return {
                "status": "matched",
                "match_type": "normalized_model_key",
                "confidence": 0.94,
                "identity": identity.__dict__,
                "product": model_matches[0],
            }

        # Similarity fallback for minor spacing/casing mistakes.
        best_product = None
        best_score = 0.0

        for item in catalog:
            score = self.identity_service.similarity(
                identity.normalized_model_key,
                str(item.get("normalized_model_key") or ""),
            )
            if score > best_score:
                best_score = score
                best_product = item

        if best_product and best_score >= 0.88:
            return {
                "status": "possible_match",
                "match_type": "model_similarity",
                "confidence": best_score,
                "identity": identity.__dict__,
                "product": best_product,
            }

        return resolved

    def _first_value(self, row: dict[str, Any], columns: list[str]) -> str | None:
        for column in columns:
            value = self._clean(row.get(column))
            if value:
                return value
        return None

    def _clean(self, value: Any) -> str | None:
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        if text.lower() in {"nan", "none", "null", "n/a", "na", "-", "--"}:
            return None

        return " ".join(text.split())

    def _normalize_text_for_hash(self, value: str) -> str:
        text = value.lower()
        text = re.sub(r"[^a-z0-9]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _fingerprint(
        self,
        product_id: str,
        raw_text: str,
        rating: str | None,
        review_date: str | None,
    ) -> str:
        payload = "|".join(
            [
                product_id,
                self._normalize_text_for_hash(raw_text),
                rating or "",
                review_date or "",
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
