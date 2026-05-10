from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


DEFAULT_QUOTAS = {
    "5": 84,
    "4": 60,
    "3": 36,
    "2": 24,
    "1": 36,
}


def normalize_rating(value: str | None) -> str:
    text = str(value or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text if text in {"1", "2", "3", "4", "5"} else ""


def row_quality_score(row: dict) -> tuple:
    text = str(row.get("raw_text") or "")
    title = str(row.get("review_title") or "")
    rating = normalize_rating(row.get("rating"))

    word_count = len(text.split())
    has_specific_aspect = any(
        token in text.lower()
        for token in [
            "camera",
            "battery",
            "display",
            "performance",
            "charging",
            "heat",
            "heating",
            "speaker",
            "network",
            "lag",
            "design",
            "price",
            "budget",
            "value",
        ]
    )

    # Prefer real text over generic one-liners.
    return (
        1 if has_specific_aspect else 0,
        min(word_count, 80),
        1 if title and title.lower() != text.lower() else 0,
        rating,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create balanced final review feed by rating bucket.")
    parser.add_argument("--input", nargs="+", required=True, help="One or more clean review CSV files.")
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--min-per-product", type=int, default=200)
    parser.add_argument("--max-per-product", type=int, default=240)
    args = parser.parse_args()

    rows: list[dict] = []

    for input_file in args.input:
        path = Path(input_file)
        if not path.exists():
            print(f"WARNING: Missing input file: {path}")
            continue

        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                row["rating"] = normalize_rating(row.get("rating"))
                if row.get("product_id") and row.get("raw_text") and row.get("review_fingerprint"):
                    rows.append(row)

    # Global dedupe across multiple scrape runs.
    seen = set()
    deduped: list[dict] = []

    for row in rows:
        key = (
            row.get("product_id"),
            row.get("text_hash") or row.get("review_fingerprint") or row.get("review_id"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    by_product: dict[str, list[dict]] = {}
    for row in deduped:
        by_product.setdefault(row["product_id"], []).append(row)

    final_rows: list[dict] = []
    report: dict = {
        "input_count": len(rows),
        "deduped_count": len(deduped),
        "min_per_product": args.min_per_product,
        "max_per_product": args.max_per_product,
        "quota_by_rating": DEFAULT_QUOTAS,
        "products": {},
    }

    for product_id, product_rows in by_product.items():
        buckets = {str(r): [] for r in range(1, 6)}
        unknown = []

        for row in product_rows:
            rating = normalize_rating(row.get("rating"))
            if rating in buckets:
                buckets[rating].append(row)
            else:
                unknown.append(row)

        for rating in buckets:
            buckets[rating].sort(key=row_quality_score, reverse=True)

        selected: list[dict] = []

        # First pass: rating quotas.
        for rating in ["1", "2", "3", "4", "5"]:
            quota = DEFAULT_QUOTAS[rating]
            selected.extend(buckets[rating][:quota])

        # Second pass: if still below max, fill from remaining non-5 first.
        selected_keys = {
            row.get("review_fingerprint") or row.get("review_id")
            for row in selected
        }

        remainder: list[dict] = []
        for rating in ["1", "2", "3", "4", "5"]:
            for row in buckets[rating]:
                key = row.get("review_fingerprint") or row.get("review_id")
                if key not in selected_keys:
                    remainder.append(row)

        remainder.sort(key=row_quality_score, reverse=True)

        for row in remainder:
            if len(selected) >= args.max_per_product:
                break
            selected.append(row)

        selected = selected[: args.max_per_product]
        final_rows.extend(selected)

        rating_counts = {r: 0 for r in ["1", "2", "3", "4", "5", "unknown"]}
        for row in selected:
            rating = normalize_rating(row.get("rating")) or "unknown"
            rating_counts[rating] = rating_counts.get(rating, 0) + 1

        report["products"][product_id] = {
            "available_clean_rows": len(product_rows),
            "selected_rows": len(selected),
            "meets_minimum": len(selected) >= args.min_per_product,
            "rating_counts": rating_counts,
            "note": (
                "Meets 200-review target"
                if len(selected) >= args.min_per_product
                else "Below 200 because source pages did not provide enough unique clean reviews yet"
            ),
        }

    if not final_rows:
        raise SystemExit("No final rows selected.")

    fieldnames = list(final_rows[0].keys())
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(final_rows)

    report["output_count"] = len(final_rows)
    Path(args.report).write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
