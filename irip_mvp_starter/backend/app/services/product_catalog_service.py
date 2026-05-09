from __future__ import annotations

import csv
import io
import urllib.request
from urllib.parse import urlparse

from app.db.repository import ReviewRepository
from app.schemas.imports import ImportErrorItem, ProductCatalogImportResult

OWN_BRAND_NAMES = {"tecno", "infinix", "itel"}
KNOWN_COMPETITOR_BRANDS = {
    "samsung",
    "xiaomi",
    "redmi",
    "poco",
    "realme",
    "vivo",
    "oppo",
    "motorola",
    "oneplus",
    "iqoo",
    "nothing",
    "lava",
}


class ProductCatalogImportService:
    """Imports product catalog metadata and ownership mappings.

    The catalog is the source of truth for whether a product belongs to
    Transsion-owned brands or should be treated as a competitor. We infer only
    when explicit own_brand is missing.
    """

    REQUIRED_COLUMNS = {"product_id"}

    def __init__(self, repository: ReviewRepository) -> None:
        self.repository = repository

    def import_csv_url(self, url: str) -> ProductCatalogImportResult:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="Only http/https CSV URLs are supported")],
            )
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "IRIP-MVP/0.1 catalog importer"})
            with urllib.request.urlopen(request, timeout=20) as response:
                raw_bytes = response.read(2_000_001)
        except Exception as exc:  # pragma: no cover
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Could not fetch catalog CSV URL: {exc}")],
            )
        if len(raw_bytes) > 2_000_000:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="Catalog CSV URL response is larger than 2 MB MVP limit")],
            )
        return self.import_csv_text(raw_bytes.decode("utf-8-sig"))

    def import_csv_text(self, csv_text: str) -> ProductCatalogImportResult:
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="CSV has no header row")],
            )
        headers = {h.strip() for h in reader.fieldnames if h}
        missing = sorted(self.REQUIRED_COLUMNS - headers)
        if missing:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Missing required column(s): {', '.join(missing)}")],
            )

        imported_products = 0
        imported_mappings = 0
        own_brand_count = 0
        competitor_brand_count = 0
        errors: list[ImportErrorItem] = []
        product_ids: set[str] = set()

        for row_number, raw_row in enumerate(reader, start=2):
            row = {
                key.strip(): (value.strip() if isinstance(value, str) else value)
                for key, value in raw_row.items()
                if key
            }
            product_id = _normalize_id(row.get("product_id") or "")
            if not product_id:
                errors.append(ImportErrorItem(row_number=row_number, reason="product_id is required"))
                continue
            try:
                brand = _empty_to_none(row.get("brand"))
                own_brand = _infer_own_brand(
                    explicit_value=row.get("own_brand"),
                    brand=brand,
                    product_name=row.get("product_name") or row.get("model"),
                )
                parent_company = _empty_to_none(row.get("parent_company"))
                if own_brand and not parent_company:
                    parent_company = "Transsion Holdings"

                self.repository.upsert_product(
                    product_id=product_id,
                    product_name=_empty_to_none(row.get("product_name")) or _empty_to_none(row.get("model")),
                    brand=brand,
                    parent_company=parent_company,
                    price_band=_empty_to_none(row.get("price_band")),
                    own_brand=own_brand,
                    marketplace=_empty_to_none(row.get("marketplace")),
                    marketplace_product_id=_empty_to_none(row.get("marketplace_product_id"))
                    or _empty_to_none(row.get("asin"))
                    or _empty_to_none(row.get("fsn")),
                    marketplace_product_url=_empty_to_none(row.get("marketplace_product_url"))
                    or _empty_to_none(row.get("product_url")),
                    launch_period=_empty_to_none(row.get("launch_period")),
                    comparison_group=_empty_to_none(row.get("comparison_group")),
                )
                imported_products += 1
                product_ids.add(product_id)
                if own_brand is True:
                    own_brand_count += 1
                elif own_brand is False:
                    competitor_brand_count += 1

                competitor_ids = _split_competitors(row.get("competitor_product_ids") or row.get("direct_competitor_ids"))
                for competitor_id_raw in competitor_ids:
                    competitor_id = _normalize_id(competitor_id_raw)
                    self.repository.upsert_product(product_id=competitor_id, own_brand=False)
                    self.repository.save_competitor_mapping(
                        product_id=product_id,
                        competitor_product_id=competitor_id,
                        comparison_group=_empty_to_none(row.get("comparison_group")) or "direct_competitor",
                        notes=_empty_to_none(row.get("mapping_notes")) or _empty_to_none(row.get("notes")),
                    )
                    imported_mappings += 1
            except ValueError as exc:
                errors.append(ImportErrorItem(row_number=row_number, reason=str(exc)))

        return ProductCatalogImportResult(
            imported_products=imported_products,
            imported_mappings=imported_mappings,
            failed_count=len(errors),
            errors=errors[:50],
            product_ids=sorted(product_ids),
            own_brand_count=own_brand_count,
            competitor_brand_count=competitor_brand_count,
        )


def _normalize_id(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _empty_to_none(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


def _to_bool_or_none(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "own", "owned", "transsion"}:
        return True
    if normalized in {"false", "0", "no", "n", "competitor", "other"}:
        return False
    return None


def _infer_own_brand(
    explicit_value: str | None,
    brand: str | None,
    product_name: str | None,
) -> bool | None:
    explicit = _to_bool_or_none(explicit_value)
    if explicit is not None:
        return explicit

    brand_norm = (brand or "").strip().lower()
    name_norm = (product_name or "").strip().lower()

    if brand_norm in OWN_BRAND_NAMES or any(item in name_norm for item in OWN_BRAND_NAMES):
        return True

    if brand_norm in KNOWN_COMPETITOR_BRANDS:
        return False

    return None


def _split_competitors(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
