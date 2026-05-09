from pathlib import Path
import re

path = Path("src/App.tsx")
backup = Path("src/App.tsx.before_own_product_filter_backup")

if backup.exists():
    path.write_text(backup.read_text(encoding="utf-8"), encoding="utf-8")
    print("Restored App.tsx from backup.")
else:
    print("Backup not found. Continuing with current App.tsx.")

text = path.read_text(encoding="utf-8")

# Remove useMemo from React import if the failed patch added it and it is not otherwise needed.
react_import = re.search(r'import\s*\{([^}]+)\}\s*from\s*["\']react["\'];', text)
if react_import:
    imports = [item.strip() for item in react_import.group(1).split(",")]
    imports = [item for item in imports if item != "useMemo"]
    new_import = "import { " + ", ".join(imports) + ' } from "react";'
    text = text[:react_import.start()] + new_import + text[react_import.end():]

# Find Product dropdown area and patch only the first products.map inside it.
label_positions = [idx for idx in [text.find("Product"), text.find("PRODUCT")] if idx != -1]
if not label_positions:
    raise SystemExit("Could not find Product label in App.tsx.")

start = min(label_positions)
match = re.search(r'products\.map\(\(product\)\s*=>\s*\(', text[start:])

if not match:
    raise SystemExit("Could not find products.map after Product label.")

absolute_start = start + match.start()
absolute_end = start + match.end()

replacement = '''products
              .filter((product) => {
                const item = product as ProductItem & { brand?: string | null; own_brand?: boolean | null };
                const brand = String(item.brand || "").trim().toLowerCase();
                return item.own_brand === true || ["tecno", "infinix", "itel"].includes(brand);
              })
              .map((product) => ('''

text = text[:absolute_start] + replacement + text[absolute_end:]

path.write_text(text, encoding="utf-8")
print("Patched Product dropdown inline to show only own-brand products.")
