from pathlib import Path
import re

path = Path("app/main.py")
text = path.read_text(encoding="utf-8")

pattern = r'(?ms)^@app\.get\("/products"\)\s*def\s+[A-Za-z0-9_]+\([^)]*\):.*?(?=^@app\.)'

replacement = r'''@app.get("/products")
def list_products():
    """Return review-backed products plus catalog-backed products.

    Review products keep their live review counts.
    Catalog products appear even before reviews are imported, so OEM teams can
    prepare product setup first and attach reviews later.
    """
    from app.services.product_catalog_service import ProductCatalogService

    review_products = repository.list_products()
    catalog_products = ProductCatalogService().list_catalog()

    merged: dict[str, dict] = {}

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
            merged[product_id] = {
                **catalog_view,
                **merged[product_id],
                "brand": merged[product_id].get("brand") or catalog_view["brand"],
                "parent_company": merged[product_id].get("parent_company") or catalog_view["parent_company"],
                "price_band": merged[product_id].get("price_band") or catalog_view["price_band"],
                "own_brand": merged[product_id].get("own_brand") or catalog_view["own_brand"],
                "marketplace_product_url": merged[product_id].get("marketplace_product_url") or catalog_view["marketplace_product_url"],
                "launch_period": merged[product_id].get("launch_period") or catalog_view["launch_period"],
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

    return products

'''

new_text, count = re.subn(pattern, replacement, text, count=1)

if count != 1:
    print("Could not replace /products route automatically.")
    print("Matching routes found:")
    for match in re.finditer(r'(?m)^@app\.get\(".*products.*"\).*$', text):
        print(match.group(0))
    raise SystemExit(1)

path.write_text(new_text, encoding="utf-8")
print("Updated /products to merge review products + catalog products.")
