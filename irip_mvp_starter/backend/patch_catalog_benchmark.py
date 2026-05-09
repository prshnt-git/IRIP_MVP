from pathlib import Path

path = Path("app/main.py")
text = path.read_text(encoding="utf-8")

marker = "# --- IRIP Catalog Benchmark Monkey Patch START ---"

if marker in text:
    print("Catalog benchmark patch already exists. No change made.")
else:
    lines = text.splitlines()
    start = None

    for i, line in enumerate(lines):
        if "visualization_service" in line and "VisualizationService" in line and "=" in line:
            start = i
            break

    if start is None:
        print("Could not find visualization_service assignment. Candidates:")
        for line in lines:
            if "VisualizationService" in line or "visualization_service" in line:
                print(line)
        raise SystemExit(1)

    # Find end of VisualizationService(...) assignment, including multi-line constructor.
    balance = 0
    end = start

    for j in range(start, len(lines)):
        balance += lines[j].count("(")
        balance -= lines[j].count(")")
        end = j
        if j > start and balance <= 0:
            break
        if j == start and balance == 0:
            break

    patch = '''
# --- IRIP Catalog Benchmark Monkey Patch START ---
# Catalog specs should win over Gemini/rules when both selected and competitor products exist in catalog.
try:
    from app.services.catalog_benchmark_service import CatalogBenchmarkService

    if hasattr(visualization_service, "_benchmark_spec_table"):
        _original_benchmark_spec_table = visualization_service._benchmark_spec_table

        def _catalog_first_benchmark_spec_table(product_id: str, competitor_product_id: str | None = None, **kwargs):
            competitor_id = competitor_product_id or kwargs.get("competitor_id") or kwargs.get("compare_product_id")

            try:
                catalog_table = CatalogBenchmarkService().build_spec_table(
                    product_id=product_id,
                    competitor_product_id=competitor_id,
                )
                if catalog_table:
                    return catalog_table
            except Exception:
                pass

            return _original_benchmark_spec_table(product_id, competitor_id)

        visualization_service._benchmark_spec_table = _catalog_first_benchmark_spec_table
except Exception:
    pass
# --- IRIP Catalog Benchmark Monkey Patch END ---
'''.splitlines()

    new_lines = lines[: end + 1] + patch + lines[end + 1 :]
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print("Installed catalog-first benchmark spec patch.")
