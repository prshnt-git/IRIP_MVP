from pathlib import Path

path = Path("app/services/product_catalog_service.py")
text = path.read_text(encoding="utf-8")

compat_block = r'''

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
'''

marker = "# V0.8.2 backward-compatible importer"

if marker not in text:
    text = text.rstrip() + "\n" + compat_block + "\n"
else:
    # Replace existing V0.8.2 block if rerun.
    start = text.find(marker)
    start = text.rfind("\n", 0, start)
    text = text[:start].rstrip() + "\n" + compat_block + "\n"

path.write_text(text, encoding="utf-8")
print("Installed V0.8.2 legacy ProductCatalogImportService compatibility.")
