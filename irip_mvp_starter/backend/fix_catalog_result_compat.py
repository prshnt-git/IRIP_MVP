from pathlib import Path
import re

path = Path("app/services/product_catalog_service.py")
text = path.read_text(encoding="utf-8")

if "def imported_products(self) -> int:" not in text:
    pattern = r'(class CatalogImportResult:\n(?:    .+\n)+?    storage_path: str\n)'
    replacement = r'''\1
    @property
    def imported_products(self) -> int:
        """Backward-compatible name used by older tests/code."""
        return self.imported_count

    @property
    def updated_products(self) -> int:
        """Backward-compatible alias for updated catalog rows."""
        return self.updated_count

    @property
    def skipped_products(self) -> int:
        """Backward-compatible alias for skipped catalog rows."""
        return self.skipped_count

'''
    text, count = re.subn(pattern, replacement, text, count=1)

    if count != 1:
        raise SystemExit("Could not patch CatalogImportResult safely.")

path.write_text(text, encoding="utf-8")
print("Added backward-compatible CatalogImportResult properties.")
