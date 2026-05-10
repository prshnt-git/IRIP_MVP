from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.collectors.flipkart_review_collector import FlipkartReviewCollector
from app.services.product_catalog_service import ProductCatalogService
from app.services.product_identity_service import ProductIdentityService


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect online reviews into raw CSV.")
    parser.add_argument("--product-id", help="Catalog product_id to collect.")
    parser.add_argument("--all-own", action="store_true", help="Collect for all own catalog products with marketplace_url.")
    parser.add_argument("--url", help="Direct product URL for one-off collection.")
    parser.add_argument("--product-name", help="Product name for direct URL mode.")
    parser.add_argument("--brand", help="Brand for direct URL mode.")
    parser.add_argument("--max-reviews", type=int, default=25)
    parser.add_argument("--max-pages", type=int, default=8)
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--output", default="data/online_reviews_raw.csv")
    parser.add_argument("--append", action="store_true")
    args = parser.parse_args()

    service = ProductCatalogService()
    targets = []

    if args.url:
        if not args.product_name:
            raise SystemExit("--product-name is required when using --url")

        identity = ProductIdentityService().build_identity(
            product_name=args.product_name,
            brand=args.brand,
        )

        targets.append(
            {
                "product_id": identity.product_id,
                "product_name": args.product_name,
                "brand": args.brand,
                "marketplace_url": args.url,
            }
        )
    elif args.product_id:
        product = service.get_product(args.product_id)
        if not product:
            raise SystemExit(f"Product not found in catalog: {args.product_id}")
        targets.append(product)
    elif args.all_own:
        for product in service.list_catalog(own_only=True):
            url = str(product.get("marketplace_url") or "")
            if url.startswith("http"):
                targets.append(product)
    else:
        raise SystemExit("Use --product-id, --all-own, or --url.")

    collector = FlipkartReviewCollector(
        max_reviews=args.max_reviews,
        max_pages=args.max_pages,
        headful=args.headful,
    )

    output_path = Path(args.output)
    total = 0
    first_write = not args.append

    for target in targets:
        product_id = str(target.get("product_id") or "")
        product_name = str(target.get("product_name") or "")
        brand = target.get("brand")
        url = str(target.get("marketplace_url") or "")

        reviews = collector.collect(
            product_id=product_id,
            product_name=product_name,
            brand=brand,
            marketplace_url=url,
        )

        collector.write_csv(
            reviews,
            output_path,
            append=not first_write or args.append,
        )
        first_write = False
        total += len(reviews)

    print(json.dumps(
        {
            "version": "V1.1.0",
            "status": "collection_complete",
            "target_count": len(targets),
            "review_count": total,
            "output": str(output_path),
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
