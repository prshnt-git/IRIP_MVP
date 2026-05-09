from pathlib import Path
import re

path = Path("app/main.py")
text = path.read_text(encoding="utf-8")

start_marker = "# --- IRIP Catalog-Aware Review Import API START ---"
end_marker = "# --- IRIP Catalog-Aware Review Import API END ---"

text = re.sub(
    rf"\n?{re.escape(start_marker)}.*?{re.escape(end_marker)}\n?",
    "\n",
    text,
    flags=re.DOTALL,
)

block = r'''

# --- IRIP Catalog-Aware Review Import API START ---
@app.post("/data/reviews/import-csv-normalized")
async def import_reviews_catalog_normalized(file: UploadFile = File(...)):
    """Import messy review CSV by resolving product names against the product catalog first.

    This endpoint keeps the existing review pipeline intact:
    messy CSV -> catalog resolver -> normalized review rows -> repository import.
    """
    from app.services.review_catalog_normalizer_service import ReviewCatalogNormalizerService

    try:
        content = await file.read()
        csv_text = content.decode("utf-8-sig")

        normalizer = ReviewCatalogNormalizerService()
        result = normalizer.normalize_csv_text(csv_text)

        if not result.output_rows:
            return {
                "version": "V0.8",
                "status": "no_importable_rows",
                "normalization": {
                    "imported_candidate_count": result.imported_candidate_count,
                    "skipped_count": result.skipped_count,
                    "duplicate_count": result.duplicate_count,
                    "unresolved_count": result.unresolved_count,
                    "errors": result.errors,
                    "unresolved_rows": result.unresolved_rows,
                },
                "import_result": None,
            }

        import_result = repository.import_reviews(result.output_rows)

        return {
            "version": "V0.8",
            "status": "import_complete",
            "normalization": {
                "imported_candidate_count": result.imported_candidate_count,
                "skipped_count": result.skipped_count,
                "duplicate_count": result.duplicate_count,
                "unresolved_count": result.unresolved_count,
                "errors": result.errors,
                "unresolved_rows": result.unresolved_rows,
            },
            "import_result": import_result,
        }

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
# --- IRIP Catalog-Aware Review Import API END ---

'''

# Add near /data APIs if available, otherwise append.
anchor = "# --- IRIP Data Catalog API END ---"
idx = text.find(anchor)

if idx != -1:
    insert_at = idx + len(anchor)
    text = text[:insert_at] + "\n" + block + text[insert_at:]
else:
    text = text.rstrip() + "\n" + block + "\n"

path.write_text(text, encoding="utf-8")
print("Added V0.8 catalog-aware review import API.")
