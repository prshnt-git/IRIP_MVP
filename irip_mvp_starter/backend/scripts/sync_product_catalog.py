from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.product_catalog_service import ProductCatalogService


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync IRIP product catalog from CSV file or Google Sheet CSV URL.")
    parser.add_argument("--file", help="Local catalog CSV path.")
    parser.add_argument("--url", help="Published Google Sheet CSV URL.")
    parser.add_argument("--own-only", action="store_true", help="Print only own-brand catalog after sync.")
    args = parser.parse_args()

    if not args.file and not args.url:
        raise SystemExit("Provide either --file or --url.")

    service = ProductCatalogService()

    if args.file:
        result = service.import_csv_path(args.file)
        source = args.file
    else:
        result = service.import_csv_url(args.url)
        source = args.url

    catalog = service.list_catalog(own_only=True if args.own_only else None)

    print(json.dumps(
        {
            "version": "V0.7.1",
            "status": "catalog_sync_complete",
            "source": source,
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "product_ids": result.product_ids,
            "catalog_count": len(catalog),
            "errors": result.errors,
        },
        indent=2,
        ensure_ascii=False,
    ))


if __name__ == "__main__":
    main()
