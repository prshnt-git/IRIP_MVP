from pathlib import Path
import re

path = Path("app/services/product_catalog_service.py")
text = path.read_text(encoding="utf-8")

text = re.sub(
    r'    @property\s+def imported_products\(self\) -> int:\s+"""Backward-compatible name used by older tests/code\."""\s+return self\.imported_count',
    '''    @property
    def imported_products(self) -> int:
        """Backward-compatible name used by older tests/code.

        Old code counted both newly inserted and updated catalog rows as imported.
        New code separates imported_count and updated_count, so this alias returns
        the total processed product rows for compatibility.
        """
        return self.imported_count + self.updated_count''',
    text,
    count=1,
)

path.write_text(text, encoding="utf-8")
print("Updated imported_products compatibility alias.")
