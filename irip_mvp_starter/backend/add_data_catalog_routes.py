from pathlib import Path
import re

path = Path("app/main.py")
text = path.read_text(encoding="utf-8")

start_marker = "# --- IRIP Data Catalog API START ---"
end_marker = "# --- IRIP Data Catalog API END ---"

text = re.sub(
    rf"\n?{re.escape(start_marker)}.*?{re.escape(end_marker)}\n?",
    "\n",
    text,
    flags=re.DOTALL,
)

block = r'''

# --- IRIP Data Catalog API START ---
@app.get("/data/catalog")
def list_data_catalog(own_only: bool | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.list_catalog(own_only=own_only)


@app.post("/data/catalog/import-csv")
async def import_data_catalog_csv(file: UploadFile = File(...)):
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


@app.post("/data/catalog/import-csv-url")
def import_data_catalog_csv_url(url: str):
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


@app.get("/data/resolve-product")
def resolve_data_product(product_name: str, brand: str | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.resolve_product(product_name=product_name, brand=brand)
# --- IRIP Data Catalog API END ---

'''

# Put these before dynamic product routes by inserting near top after app is initialized.
anchor = "app = FastAPI"
idx = text.find(anchor)

if idx == -1:
    # fallback: append at bottom
    text = text.rstrip() + "\n" + block + "\n"
else:
    # find next route decorator after app declaration, insert before it
    route_idx = text.find("\n@app.", idx)
    if route_idx == -1:
        text = text.rstrip() + "\n" + block + "\n"
    else:
        text = text[:route_idx] + block + text[route_idx:]

path.write_text(text, encoding="utf-8")
print("Added conflict-free /data catalog API routes.")
