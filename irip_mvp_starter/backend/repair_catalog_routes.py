from pathlib import Path
import re

path = Path("app/main.py")
text = path.read_text(encoding="utf-8")

start_marker = "# --- IRIP Product Catalog API START ---"
end_marker = "# --- IRIP Product Catalog API END ---"

# Remove old catalog block if it was appended at the bottom.
text = re.sub(
    rf"\n?{re.escape(start_marker)}.*?{re.escape(end_marker)}\n?",
    "\n",
    text,
    flags=re.DOTALL,
)

catalog_block = r'''
# --- IRIP Product Catalog API START ---
@app.get("/products/catalog")
def list_product_catalog(own_only: bool | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.list_catalog(own_only=own_only)


@app.post("/products/catalog/import-csv")
async def import_product_catalog_csv(file: UploadFile = File(...)):
    from app.services.product_catalog_service import ProductCatalogService

    try:
        content = await file.read()
        csv_text = content.decode("utf-8-sig")
        service = ProductCatalogService()
        result = service.import_csv_text(csv_text)
        return {
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "errors": result.errors,
            "product_ids": result.product_ids,
            "storage_path": result.storage_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/products/catalog/import-csv-url")
def import_product_catalog_csv_url(url: str):
    from app.services.product_catalog_service import ProductCatalogService

    try:
        service = ProductCatalogService()
        result = service.import_csv_url(url)
        return {
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "errors": result.errors,
            "product_ids": result.product_ids,
            "storage_path": result.storage_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/products/resolve")
def resolve_product(product_name: str, brand: str | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.resolve_product(product_name=product_name, brand=brand)
# --- IRIP Product Catalog API END ---

'''

# Insert before the first dynamic product route, otherwise /products/catalog gets captured by /products/{product_id}.
patterns = [
    '@app.get("/products/{product_id}',
    '@app.get("/products/{product_id}"',
    '@app.get("/products/{',
    "@app.get('/products/{",
    '@app.post("/products/{',
    "@app.post('/products/{",
]

insert_at = -1
for pattern in patterns:
    idx = text.find(pattern)
    if idx != -1:
        insert_at = idx
        break

if insert_at == -1:
    # Fallback: put after /products list route if dynamic route is not found.
    idx = text.find('@app.get("/products"')
    if idx == -1:
        raise SystemExit("Could not find product routes in app/main.py.")
    next_route = text.find("\n@app.", idx + 1)
    insert_at = next_route if next_route != -1 else len(text)

text = text[:insert_at] + catalog_block + text[insert_at:]

path.write_text(text, encoding="utf-8")
print("Inserted catalog routes before dynamic product routes.")
