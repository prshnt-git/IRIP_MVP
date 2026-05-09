from pathlib import Path

path = Path("app/main.py")
lines = path.read_text(encoding="utf-8").splitlines()

start = None
for i, line in enumerate(lines):
    stripped = line.strip()
    if stripped.startswith('@app.get("/products"') and "/products/" not in stripped:
        start = i
        break

if start is None:
    print("Could not find exact /products route.")
    print("Available product route decorators:")
    for line in lines:
        if "@app." in line and "products" in line:
            print(line)
    raise SystemExit(1)

end = len(lines)
for j in range(start + 1, len(lines)):
    if lines[j].startswith("@app."):
        end = j
        break

replacement = '''@app.get("/products")
def list_products():
    """Return review-backed products plus catalog-backed products."""
    from app.services.product_catalog_service import ProductCatalogService

    review_products = repository.list_products()
    catalog_products = ProductCatalogService().list_catalog()

    merged = {}

    for item in review_products:
        product_id = item.get("product_id")
        if product_id:
            merged[product_id] = dict(item)

    for item in catalog_products:
        product_id = item.get("product_id")
        if not product_id:
            continue

        catalog_view = {
            "product_id": product_id,
            "product_name": item.get("product_name"),
            "brand": item.get("brand"),
            "parent_company": item.get("company_name"),
            "price_band": item.get("price_band"),
            "own_brand": bool(item.get("is_own_product")),
            "marketplace": None,
            "marketplace_product_id": None,
            "marketplace_product_url": item.get("marketplace_url"),
            "launch_period": item.get("launch_date"),
            "comparison_group": "own_product" if item.get("is_own_product") else "competitor",
            "review_count": 0,
            "first_review_date": None,
            "latest_review_date": None,
        }

        if product_id in merged:
            existing = merged[product_id]
            merged[product_id] = {
                **catalog_view,
                **existing,
                "brand": existing.get("brand") or catalog_view["brand"],
                "parent_company": existing.get("parent_company") or catalog_view["parent_company"],
                "price_band": existing.get("price_band") or catalog_view["price_band"],
                "own_brand": bool(existing.get("own_brand") or catalog_view["own_brand"]),
                "marketplace_product_url": existing.get("marketplace_product_url") or catalog_view["marketplace_product_url"],
                "launch_period": existing.get("launch_period") or catalog_view["launch_period"],
            }
        else:
            merged[product_id] = catalog_view

    products = list(merged.values())
    products.sort(
        key=lambda item: (
            not bool(item.get("own_brand")),
            str(item.get("brand") or ""),
            str(item.get("product_name") or ""),
        )
    )

    return products'''.splitlines()

new_lines = lines[:start] + replacement + lines[end:]
path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

print(f"Replaced /products route lines {start + 1} to {end}.")
