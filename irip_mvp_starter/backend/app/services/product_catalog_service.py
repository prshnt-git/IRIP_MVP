from __future__ import annotations

import csv
import json
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.services.product_identity_service import ProductIdentityService


@dataclass
class CatalogImportResult:
    imported_count: int
    updated_count: int
    skipped_count: int
    failed_count: int
    errors: list[dict]
    product_ids: list[str]
    storage_path: str
    imported_mappings: int = 0

    @property
    def imported_products(self) -> int:
        """Backward-compatible name used by older tests/code.

        Old code counted every valid processed catalog row as imported.
        New code separates new rows and updated rows, so this compatibility
        property returns both.
        """
        return self.imported_count + self.updated_count
    @property
    def updated_products(self) -> int:
        """Backward-compatible alias for updated catalog rows."""
        return self.updated_count

    @property
    def skipped_products(self) -> int:
        """Backward-compatible alias for skipped catalog rows."""
        return self.skipped_count



class ProductCatalogService:
    """Controlled product catalog importer for IRIP.

    Current purpose:
    - Accept product catalog rows from CSV / Google Sheet published as CSV.
    - Normalize product identity through ProductIdentityService.
    - Store clean catalog records in a stable JSON store.
    - Keep own products and competitor products in the same structure.

    Later:
    - This same service can upsert into product_specs DB table.
    - Review ingestion will resolve incoming reviews against this catalog.
    """

    CATALOG_FIELDS = [
        "product_id",
        "company_name",
        "brand",
        "product_name",
        "model_name",
        "series_name",
        "canonical_product_key",
        "normalized_brand",
        "normalized_model_key",
        "variant_key",
        "is_own_product",
        "launch_date",
        "launch_market",
        "price_band",
        "current_price",
        "ram",
        "storage",
        "battery_capacity",
        "charging_wattage",
        "chipset",
        "display_size",
        "display_type",
        "refresh_rate",
        "screen_to_body_ratio",
        "rear_camera",
        "front_camera",
        "android_version",
        "custom_ui",
        "network_5g",
        "weight",
        "thickness",
        "official_url",
        "marketplace_url",
        "source_name",
        "source_confidence",
        "updated_at",
    ]

    OWN_COMPANY_NAME = "Transsion Holdings"

    def __init__(
        self,
        identity_service: ProductIdentityService | None = None,
        storage_path: str | Path | None = None,
    ) -> None:
        self.identity_service = identity_service or ProductIdentityService()
        backend_root = Path(__file__).resolve().parents[2]
        self.storage_path = Path(storage_path) if storage_path else backend_root / "data" / "product_catalog.json"
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    def import_csv_path(self, csv_path: str | Path) -> CatalogImportResult:
        path = Path(csv_path)
        text = path.read_text(encoding="utf-8-sig")
        return self.import_csv_text(text)

    def import_csv_url(self, url: str, timeout: int = 30) -> CatalogImportResult:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            text = response.read().decode("utf-8-sig")
        return self.import_csv_text(text)

    def import_csv_text(self, csv_text: str) -> CatalogImportResult:
        existing = self._load_catalog_map()
        reader = csv.DictReader(csv_text.splitlines())

        imported_count = 0
        updated_count = 0
        skipped_count = 0
        failed_count = 0
        imported_mappings = 0
        errors: list[dict] = []
        product_ids: list[str] = []

        if not reader.fieldnames:
            return CatalogImportResult(
                imported_count=0,
                updated_count=0,
                skipped_count=0,
                failed_count=1,
                errors=[{"row_number": None, "reason": "CSV has no header row."}],
                product_ids=[],
                storage_path=str(self.storage_path),
                imported_mappings=0,
            )

        for row_number, row in enumerate(reader, start=2):
            try:
                record = self._row_to_record(row)

                if not record["product_name"]:
                    skipped_count += 1
                    errors.append(
                        {
                            "row_number": row_number,
                            "reason": "Missing product_name.",
                            "value": row,
                        }
                    )
                    continue

                product_id = record["product_id"]
                product_ids.append(product_id)

                competitor_product_ids_raw = row.get("competitor_product_ids")
                if competitor_product_ids_raw:
                    imported_mappings += len([
                        item.strip()
                        for item in str(competitor_product_ids_raw).split(";")
                        if item.strip()
                    ])

                if product_id in existing:
                    existing[product_id] = {
                        **existing[product_id],
                        **record,
                        "updated_at": self._now_iso(),
                    }
                    updated_count += 1
                else:
                    existing[product_id] = record
                    imported_count += 1

            except Exception as exc:
                failed_count += 1
                errors.append(
                    {
                        "row_number": row_number,
                        "reason": str(exc),
                        "value": row,
                    }
                )

        self._save_catalog_map(existing)

        return CatalogImportResult(
            imported_count=imported_count,
            updated_count=updated_count,
            skipped_count=skipped_count,
            failed_count=failed_count,
            errors=errors,
            product_ids=sorted(set(product_ids)),            storage_path=str(self.storage_path),
            imported_mappings=imported_mappings,
        )

    def list_catalog(self, own_only: bool | None = None) -> list[dict]:
        records = list(self._load_catalog_map().values())
        records.sort(key=lambda item: (str(item.get("brand") or ""), str(item.get("product_name") or "")))

        if own_only is None:
            return records

        return [
            item
            for item in records
            if bool(item.get("is_own_product")) is own_only
        ]

    def get_product(self, product_id: str) -> dict | None:
        return self._load_catalog_map().get(product_id)

    def resolve_product(self, product_name: str, brand: str | None = None) -> dict:
        identity = self.identity_service.build_identity(product_name=product_name, brand=brand)
        catalog = self._load_catalog_map()

        exact = catalog.get(identity.product_id)
        if exact:
            return {
                "status": "matched",
                "match_type": "product_id",
                "confidence": 1.0,
                "identity": asdict(identity),
                "product": exact,
            }

        canonical_matches = [
            item
            for item in catalog.values()
            if item.get("canonical_product_key") == identity.canonical_product_key
        ]

        if canonical_matches:
            return {
                "status": "matched",
                "match_type": "canonical_product_key",
                "confidence": 0.98,
                "identity": asdict(identity),
                "product": canonical_matches[0],
            }

        best = None
        best_score = 0.0

        for item in catalog.values():
            score = self.identity_service.similarity(
                identity.canonical_product_key,
                str(item.get("canonical_product_key") or ""),
            )
            if score > best_score:
                best_score = score
                best = item

        if best and best_score >= 0.82:
            return {
                "status": "possible_match",
                "match_type": "similarity",
                "confidence": best_score,
                "identity": asdict(identity),
                "product": best,
            }

        return {
            "status": "unmatched",
            "match_type": "none",
            "confidence": 0.0,
            "identity": asdict(identity),
            "product": None,
        }

    def _row_to_record(self, row: dict[str, Any]) -> dict:
        product_name = self._clean(row.get("product_name") or row.get("name"))
        model_name = self._clean(row.get("model_name") or row.get("model") or product_name)
        brand = self._clean(row.get("brand"))

        identity = self.identity_service.build_identity(
            product_name=product_name,
            brand=brand,
            model_name=model_name,
        )

        normalized_brand = identity.normalized_brand
        is_own_product = self._parse_bool(row.get("is_own_product"))

        if is_own_product is None:
            is_own_product = self.identity_service.is_own_brand(normalized_brand)

        company_name = self._clean(row.get("company_name"))
        if not company_name and is_own_product:
            company_name = self.OWN_COMPANY_NAME

        if not brand and normalized_brand:
            brand = normalized_brand.upper() if normalized_brand in {"tecno", "itel"} else normalized_brand.title()

        product_id = self._clean(row.get("product_id")) or identity.product_id

        record = {
            "product_id": product_id,
            "company_name": company_name,
            "brand": brand,
            "product_name": product_name,
            "model_name": model_name,
            "series_name": self._clean(row.get("series_name") or row.get("series")),
            "canonical_product_key": identity.canonical_product_key,
            "normalized_brand": normalized_brand,
            "normalized_model_key": identity.normalized_model_key,
            "variant_key": self._clean(row.get("variant_key")) or identity.variant_key,
            "is_own_product": bool(is_own_product),
            "launch_date": self._clean(row.get("launch_date")),
            "launch_market": self._clean(row.get("launch_market") or row.get("market")),
            "price_band": self._clean(row.get("price_band")),
            "current_price": self._clean(row.get("current_price") or row.get("price")),
            "ram": self._clean(row.get("ram")),
            "storage": self._clean(row.get("storage")),
            "battery_capacity": self._clean(row.get("battery_capacity") or row.get("battery")),
            "charging_wattage": self._clean(row.get("charging_wattage") or row.get("charging")),
            "chipset": self._clean(row.get("chipset") or row.get("processor")),
            "display_size": self._clean(row.get("display_size")),
            "display_type": self._clean(row.get("display_type")),
            "refresh_rate": self._clean(row.get("refresh_rate")),
            "screen_to_body_ratio": self._clean(row.get("screen_to_body_ratio")),
            "rear_camera": self._clean(row.get("rear_camera")),
            "front_camera": self._clean(row.get("front_camera")),
            "android_version": self._clean(row.get("android_version")),
            "custom_ui": self._clean(row.get("custom_ui")),
            "network_5g": self._clean(row.get("network_5g")),
            "weight": self._clean(row.get("weight")),
            "thickness": self._clean(row.get("thickness")),
            "official_url": self._clean(row.get("official_url")),
            "marketplace_url": self._clean(row.get("marketplace_url")),
            "source_name": self._clean(row.get("source_name")) or "manual_catalog",
            "source_confidence": self._clean(row.get("source_confidence")) or "manual_pending",
            "updated_at": self._now_iso(),
        }

        return {field: record.get(field) for field in self.CATALOG_FIELDS}

    def _load_catalog_map(self) -> dict[str, dict]:
        if not self.storage_path.exists():
            return {}

        try:
            data = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

        if not isinstance(data, list):
            return {}

        records = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            product_id = str(item.get("product_id") or "").strip()
            if product_id:
                records[product_id] = item

        return records

    def _save_catalog_map(self, records: dict[str, dict]) -> None:
        data = [records[key] for key in sorted(records)]
        self.storage_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _clean(self, value: Any) -> str | None:
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        if text.lower() in {"nan", "none", "null", "n/a", "na", "-", "--"}:
            return None

        return " ".join(text.split())

    def _parse_bool(self, value: Any) -> bool | None:
        if value is None:
            return None

        text = str(value).strip().lower()
        if not text:
            return None

        if text in {"1", "true", "yes", "y", "own", "owned"}:
            return True

        if text in {"0", "false", "no", "n", "competitor"}:
            return False

        return None

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

# Backward-compatible wrapper for older app/main.py wiring.
class ProductCatalogImportService(ProductCatalogService):
    def __init__(self, repository=None, *args, **kwargs) -> None:
        self.repository = repository
        super().__init__(*args, **kwargs)

    def import_csv(self, csv_text: str):
        return self.import_csv_text(csv_text)

    def import_csv_file(self, csv_path):
        return self.import_csv_path(csv_path)

    def import_csv_from_url(self, url: str):
        return self.import_csv_url(url)


# V0.8.2 backward-compatible importer for older competitor mapping tests/routes.
# This intentionally sits at the bottom so it overrides any earlier alias/class with the same name.
class ProductCatalogImportService(ProductCatalogService):
    def __init__(self, repository=None, *args, **kwargs) -> None:
        self.repository = repository
        super().__init__(*args, **kwargs)

    def import_csv_text(self, csv_text: str):
        import csv
        import io

        result = super().import_csv_text(csv_text)

        if self.repository is None:
            return result

        imported_mappings = 0

        reader = csv.DictReader(io.StringIO(csv_text))
        for raw_row in reader:
            row = {
                str(key).strip(): (value.strip() if isinstance(value, str) else value)
                for key, value in raw_row.items()
                if key is not None
            }

            product_id = row.get("product_id")
            product_name = row.get("product_name") or row.get("name") or row.get("model_name") or row.get("model")
            brand = row.get("brand")
            price_band = row.get("price_band")
            own_brand = self._legacy_bool(row.get("own_brand") or row.get("is_own_product"))

            if not product_id:
                identity = self.identity_service.build_identity(
                    product_name=product_name or "unknown",
                    brand=brand,
                    model_name=row.get("model_name") or row.get("model"),
                )
                product_id = identity.product_id

            if hasattr(self.repository, "upsert_product"):
                self.repository.upsert_product(
                    product_id=product_id,
                    product_name=product_name,
                    brand=brand,
                    price_band=price_band,
                    own_brand=own_brand,
                )

            competitor_ids = self._legacy_split_competitors(
                row.get("competitor_product_ids") or row.get("direct_competitor_ids")
            )

            for competitor_id in competitor_ids:
                if hasattr(self.repository, "upsert_product"):
                    self.repository.upsert_product(product_id=competitor_id)

                if hasattr(self.repository, "save_competitor_mapping"):
                    self.repository.save_competitor_mapping(
                        product_id=product_id,
                        competitor_product_id=competitor_id,
                        comparison_group=row.get("comparison_group") or "direct_competitor",
                        notes=row.get("mapping_notes") or row.get("notes"),
                    )
                    imported_mappings += 1

        result.imported_mappings = imported_mappings
        return result

    def import_csv(self, csv_text: str):
        return self.import_csv_text(csv_text)

    def import_csv_file(self, csv_path):
        return self.import_csv_path(csv_path)

    def import_csv_from_url(self, url: str):
        return self.import_csv_url(url)

    def _legacy_split_competitors(self, value) -> list[str]:
        if not value:
            return []

        return [
            item.strip()
            for item in str(value).replace(";", ",").split(",")
            if item.strip()
        ]

    def _legacy_bool(self, value):
        if value is None:
            return None

        text = str(value).strip().lower()
        if not text:
            return None

        if text in {"true", "1", "yes", "y", "own", "owned"}:
            return True

        if text in {"false", "0", "no", "n", "competitor"}:
            return False

        return None

