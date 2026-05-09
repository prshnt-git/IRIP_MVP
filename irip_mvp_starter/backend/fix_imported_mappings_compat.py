from pathlib import Path
import re

path = Path("app/services/product_catalog_service.py")
text = path.read_text(encoding="utf-8")

# 1) Add imported_mappings to CatalogImportResult if missing.
if "imported_mappings:" not in text:
    text = text.replace(
        "    storage_path: str\n",
        "    storage_path: str\n    imported_mappings: int = 0\n",
        1,
    )

# 2) Make sure every CatalogImportResult(...) constructor has imported_mappings.
text = text.replace(
    "storage_path=str(self.storage_path),\n            )",
    "storage_path=str(self.storage_path),\n                imported_mappings=0,\n            )",
    1,
)

# 3) Add mapping counter in import_csv_text.
if "imported_mappings = 0" not in text:
    text = text.replace(
        "        failed_count = 0\n        errors: list[dict] = []\n        product_ids: list[str] = []\n",
        "        failed_count = 0\n        imported_mappings = 0\n        errors: list[dict] = []\n        product_ids: list[str] = []\n",
        1,
    )

# 4) Count old competitor_product_ids mappings from legacy CSVs.
if "competitor_product_ids_raw = row.get(\"competitor_product_ids\")" not in text:
    text = text.replace(
        "                product_ids.append(product_id)\n",
        '''                product_ids.append(product_id)

                competitor_product_ids_raw = row.get("competitor_product_ids")
                if competitor_product_ids_raw:
                    imported_mappings += len([
                        item.strip()
                        for item in str(competitor_product_ids_raw).split(";")
                        if item.strip()
                    ])
''',
        1,
    )

# 5) Ensure final CatalogImportResult includes imported_mappings.
text = re.sub(
    r'(\s+storage_path=str\(self\.storage_path\),\n\s+\))',
    r'            storage_path=str(self.storage_path),\n            imported_mappings=imported_mappings,\n        )',
    text,
    count=1,
)

# 6) If the earlier regex over-touched indentation in the no-header return, repair simply.
text = text.replace(
    "imported_mappings=imported_mappings,\n            )",
    "imported_mappings=0,\n            )",
    1,
)

path.write_text(text, encoding="utf-8")
print("Added legacy imported_mappings compatibility.")
