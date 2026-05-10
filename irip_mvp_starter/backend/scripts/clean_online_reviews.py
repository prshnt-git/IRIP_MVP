from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.collectors.review_cleaner import ReviewCleaner


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean raw online review CSV before IRIP import.")
    parser.add_argument("--input", default="data/online_reviews_raw.csv")
    parser.add_argument("--output", default="data/online_reviews_clean.csv")
    parser.add_argument("--report", default="data/online_reviews_clean_report.json")
    args = parser.parse_args()

    cleaner = ReviewCleaner()
    result = cleaner.clean_csv_path(args.input, args.output, args.report)

    print(json.dumps(
        {
            "version": "V1.1.2",
            "status": "clean_complete",
            "input_count": result.input_count,
            "clean_count": result.clean_count,
            "removed_count": result.removed_count,
            "duplicate_count": result.duplicate_count,
            "output": args.output,
            "report": args.report,
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
