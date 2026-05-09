from __future__ import annotations

from typing import Any

from app.services.product_catalog_service import ProductCatalogService


class CatalogBenchmarkService:
    """Build Benchmark spec comparison from IRIP product catalog.

    Source priority:
    1. product_catalog.json / Google Sheet catalog import
    2. existing Gemini/rules fallback in visualization service

    This keeps specs controlled by catalog data, not hardcoded dashboard logic.
    """

    SPEC_ROWS = [
        ("Commercial", "Current price", "current_price"),
        ("Commercial", "Price band", "price_band"),
        ("Display", "Display size", "display_size"),
        ("Display", "Display type", "display_type"),
        ("Display", "Refresh rate", "refresh_rate"),
        ("Display", "Screen-to-body ratio", "screen_to_body_ratio"),
        ("Performance", "Chipset", "chipset"),
        ("Performance", "RAM", "ram"),
        ("Performance", "Storage", "storage"),
        ("Battery", "Battery capacity", "battery_capacity"),
        ("Battery", "Charging wattage", "charging_wattage"),
        ("Camera", "Rear camera", "rear_camera"),
        ("Camera", "Front camera", "front_camera"),
        ("Software", "Android version", "android_version"),
        ("Software", "Custom UI", "custom_ui"),
        ("Network", "5G support", "network_5g"),
        ("Design", "Weight", "weight"),
        ("Design", "Thickness", "thickness"),
    ]

    UNKNOWN_VALUES = {"", "unknown", "none", "null", "n/a", "na", "-", "--"}

    def __init__(self, catalog_service: ProductCatalogService | None = None) -> None:
        self.catalog_service = catalog_service or ProductCatalogService()

    def build_spec_table(
        self,
        product_id: str,
        competitor_product_id: str | None,
    ) -> dict | None:
        if not competitor_product_id:
            return None

        selected = self.catalog_service.get_product(product_id)
        competitor = self.catalog_service.get_product(competitor_product_id)

        # Only use catalog table when both products are present in catalog.
        # If competitor is not in catalog yet, existing Gemini/rules fallback can still run.
        if not selected or not competitor:
            return None

        rows = []
        unknown_fields = []

        for category, label, key in self.SPEC_ROWS:
            selected_value = self._value(selected.get(key))
            competitor_value = self._value(competitor.get(key))

            if self._is_unknown(selected_value) or self._is_unknown(competitor_value):
                unknown_fields.append(label)

            rows.append(
                {
                    "category": category,
                    "field": label,
                    "selected_product_value": selected_value,
                    "competitor_value": competitor_value,
                    "winner": self._winner(selected_value, competitor_value),
                    "confidence": self._row_confidence(selected, competitor, selected_value, competitor_value),
                    "source_status": "catalog",
                    "why_it_matters": "Catalog-controlled specification used for product comparison.",
                }
            )

        return {
            "selected_product_name": selected.get("product_name") or product_id,
            "competitor_product_name": competitor.get("product_name") or competitor_product_id,
            "source": "catalog",
            "confidence_note": "Specification table is generated from the synced product catalog. Update the catalog CSV/Google Sheet to improve missing fields.",
            "rows": rows,
            "unknown_fields": unknown_fields,
        }

    def _value(self, value: Any) -> str:
        if value is None:
            return "Unknown"

        text = str(value).strip()
        if not text:
            return "Unknown"

        if text.lower() in self.UNKNOWN_VALUES:
            return "Unknown"

        return " ".join(text.split())

    def _is_unknown(self, value: str) -> bool:
        return value.strip().lower() in self.UNKNOWN_VALUES

    def _winner(self, selected_value: str, competitor_value: str) -> str:
        if self._is_unknown(selected_value) or self._is_unknown(competitor_value):
            return "unknown"

        if selected_value.strip().lower() == competitor_value.strip().lower():
            return "tie"

        return "unknown"

    def _row_confidence(self, selected: dict, competitor: dict, selected_value: str, competitor_value: str) -> str:
        if self._is_unknown(selected_value) or self._is_unknown(competitor_value):
            return "unknown"

        selected_confidence = str(selected.get("source_confidence") or "").lower()
        competitor_confidence = str(competitor.get("source_confidence") or "").lower()

        if "official" in selected_confidence and "official" in competitor_confidence:
            return "verified"

        if selected_confidence or competitor_confidence:
            return "likely"

        return "unknown"
