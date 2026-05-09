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
    """V0.8: Import messy review CSV after resolving product names against catalog.

    Flow:
    messy CSV -> catalog resolver -> normalized CSV rows -> existing ReviewImportService.
    """
    import csv
    import io

    from app.services.review_catalog_normalizer_service import ReviewCatalogNormalizerService

    try:
        content = await file.read()
        csv_text = content.decode("utf-8-sig")

        normalizer = ReviewCatalogNormalizerService()
        result = normalizer.normalize_csv_text(csv_text)

        normalization_payload = {
            "imported_candidate_count": result.imported_candidate_count,
            "skipped_count": result.skipped_count,
            "duplicate_count": result.duplicate_count,
            "unresolved_count": result.unresolved_count,
            "errors": result.errors,
            "unresolved_rows": result.unresolved_rows,
        }

        if not result.output_rows:
            return {
                "version": "V0.8",
                "status": "no_importable_rows",
                "normalization": normalization_payload,
                "import_result": None,
            }

        fields = [
            "review_id",
            "product_id",
            "product_name",
            "brand",
            "source",
            "marketplace",
            "raw_text",
            "rating",
            "review_date",
        ]

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(result.output_rows)

        import_result = importer.import_csv_text(
            buffer.getvalue(),
            discovered_via="catalog_normalized_csv",
        )

        import_payload = (
            import_result.model_dump()
            if hasattr(import_result, "model_dump")
            else import_result
        )

        return {
            "version": "V0.8",
            "status": "import_complete",
            "normalization": normalization_payload,
            "import_result": import_payload,
        }

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
# --- IRIP Catalog-Aware Review Import API END ---

'''

anchor = "# --- IRIP Data Catalog API END ---"
idx = text.find(anchor)

if idx != -1:
    insert_at = idx + len(anchor)
    text = text[:insert_at] + "\n" + block + text[insert_at:]
else:
    text = text.rstrip() + "\n" + block + "\n"

path.write_text(text, encoding="utf-8")
print("Replaced V0.8 endpoint to use ReviewImportService importer.")
