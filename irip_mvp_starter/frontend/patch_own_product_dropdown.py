from pathlib import Path
import re

path = Path("src/App.tsx")
text = path.read_text(encoding="utf-8")

# Backup first.
backup = path.with_suffix(".tsx.before_own_product_filter_backup")
backup.write_text(text, encoding="utf-8")

# Ensure useMemo is imported.
react_import = re.search(r'import\s*\{([^}]+)\}\s*from\s*["\']react["\'];', text)
if react_import:
    imports = [item.strip() for item in react_import.group(1).split(",")]
    if "useMemo" not in imports:
        imports.append("useMemo")
        new_import = "import { " + ", ".join(imports) + ' } from "react";'
        text = text[:react_import.start()] + new_import + text[react_import.end():]
else:
    print("WARNING: Could not find React named import. If build fails, add useMemo to React imports manually.")

if "const [selectedProductId, setSelectedProductId]" not in text:
    raise SystemExit("Could not find selectedProductId state. Stopping safely.")

# Find a stable insertion point after selected product / competitor state declarations.
if "const ownProductOptions = useMemo(" not in text:
    marker_match = re.search(r'(\n\s*const\s+\[selectedProductId,\s*setSelectedProductId\][^\n]+;\s*)', text)
    if not marker_match:
        raise SystemExit("Could not find selectedProductId state line for insertion.")

    insert_after = marker_match.end()

    # If competitor state is immediately nearby, insert after that instead.
    comp_match = re.search(
        r'(\n\s*const\s+\[competitorProductId,\s*setCompetitorProductId\][^\n]+;\s*)',
        text[insert_after:insert_after + 1200],
    )
    if comp_match:
        insert_after = insert_after + comp_match.end()

    snippet = '''

  const ownProductOptions = useMemo(() => {
    return products.filter((product) => {
      const brand = String(product.brand || "").trim().toLowerCase();
      return product.own_brand === true || ["tecno", "infinix", "itel"].includes(brand);
    });
  }, [products]);

  const competitorProductOptions = useMemo(() => {
    return products.filter((product) => product.product_id !== selectedProductId);
  }, [products, selectedProductId]);

  useEffect(() => {
    if (!ownProductOptions.length) return;

    const selectedIsOwnProduct = ownProductOptions.some(
      (product) => product.product_id === selectedProductId,
    );

    if (!selectedIsOwnProduct) {
      setSelectedProductId(ownProductOptions[0].product_id);
      if (typeof setCompetitorProductId === "function") {
        setCompetitorProductId("");
      }
    }
  }, [ownProductOptions, selectedProductId]);

'''
    text = text[:insert_after] + snippet + text[insert_after:]

# Replace product dropdown map after Product label.
def replace_first_products_map_after(label_candidates, replacement_name, source_text):
    best_idx = -1
    for label in label_candidates:
        idx = source_text.find(label)
        if idx != -1 and (best_idx == -1 or idx < best_idx):
            best_idx = idx

    if best_idx == -1:
        return source_text, False

    match = re.search(r'products\.map\(\((\w+)\)\s*=>', source_text[best_idx:])
    if not match:
        return source_text, False

    absolute_start = best_idx + match.start()
    absolute_end = best_idx + match.end()
    old = source_text[absolute_start:absolute_end]
    new = old.replace("products.map", f"{replacement_name}.map")
    return source_text[:absolute_start] + new + source_text[absolute_end:], True

text, product_replaced = replace_first_products_map_after(
    ['Product', 'PRODUCT'],
    'ownProductOptions',
    text,
)

text, competitor_replaced = replace_first_products_map_after(
    ['Compare with optional', 'Compare with', 'competitor', 'Competitor'],
    'competitorProductOptions',
    text,
)

if not product_replaced:
    raise SystemExit("Could not patch product dropdown map safely.")

if not competitor_replaced:
    print("WARNING: Could not patch competitor dropdown map. Product dropdown was patched.")

path.write_text(text, encoding="utf-8")
print("Patched product dropdown to show only own-brand products.")
print("Backup saved at:", backup)
print("Product dropdown patched:", product_replaced)
print("Competitor dropdown patched:", competitor_replaced)
