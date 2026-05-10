from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Cap clean review CSV to max rows per product.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-per-product", type=int, default=240)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    report_path = Path(args.report)

    rows = list(csv.DictReader(input_path.open("r", encoding="utf-8-sig", newline="")))
    if not rows:
        raise SystemExit(f"No rows found in {input_path}")

    fieldnames = list(rows[0].keys())
    kept = []
    counts = {}
    skipped = 0

    for row in rows:
        product_id = row.get("product_id") or "unknown"
        counts.setdefault(product_id, 0)

        if counts[product_id] < args.max_per_product:
            kept.append(row)
            counts[product_id] += 1
        else:
            skipped += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    summary = {
        "input_count": len(rows),
        "output_count": len(kept),
        "skipped_due_to_cap": skipped,
        "max_per_product": args.max_per_product,
        "count_by_product": counts,
        "output": str(output_path),
    }

    report_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
