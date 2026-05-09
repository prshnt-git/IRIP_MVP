from pathlib import Path
import re

path = Path("app/services/product_catalog_service.py")
text = path.read_text(encoding="utf-8")

new_property = '''    @property
    def imported_products(self) -> int:
        """Backward-compatible name used by older tests/code.

        Old code counted every valid processed catalog row as imported.
        New code separates new rows and updated rows, so this compatibility
        property returns both.
        """
        return self.imported_count + self.updated_count
'''

if "def imported_products(self) -> int:" in text:
    text, count = re.subn(
        r'    @property\s*\n    def imported_products\(self\) -> int:.*?(?=\n    @property|\n\nclass|\Z)',
        new_property.rstrip(),
        text,
        count=1,
        flags=re.DOTALL,
    )
    if count != 1:
        raise SystemExit("Found imported_products but could not replace it safely.")
else:
    text, count = re.subn(
        r'(class CatalogImportResult:\n(?:    .+\n)*?    storage_path: str\n)',
        r'\1' + "\n" + new_property,
        text,
        count=1,
    )
    if count != 1:
        raise SystemExit("Could not insert imported_products property safely.")

path.write_text(text, encoding="utf-8")
print("Fixed imported_products compatibility property.")

# Print the exact final property so we can verify it.
final = path.read_text(encoding="utf-8")
match = re.search(
    r'    @property\s*\n    def imported_products\(self\) -> int:.*?return self\.imported_count \+ self\.updated_count',
    final,
    flags=re.DOTALL,
)
print("verified:", bool(match))
