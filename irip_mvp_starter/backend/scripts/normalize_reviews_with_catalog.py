from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.review_catalog_normalizer_service import ReviewCatalogNormalizerService


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize review CSV against IRIP product catalog.")
    parser.add_argument("--input", required=True, help="Input messy review CSV path.")
    parser.add_argument("--output", required=True, help="Output normalized review CSV path.")
    parser.add_argument("--report", required=True, help="Output JSON report path.")
    args = parser.parse_args()

    service = ReviewCatalogNormalizerService()
    result = service.normalize_csv_path(args.input)
    service.write_outputs(result, args.output, args.report)

    print(json.dumps(
        {
            "imported_candidate_count": result.imported_candidate_count,
            "skipped_count": result.skipped_count,
            "duplicate_count": result.duplicate_count,
            "unresolved_count": result.unresolved_count,
            "output": str(Path(args.output)),
            "report": str(Path(args.report)),
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
