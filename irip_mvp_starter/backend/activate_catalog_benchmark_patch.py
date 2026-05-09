from pathlib import Path
import re

path = Path("app/main.py")
text = path.read_text(encoding="utf-8")

start_marker = "# --- IRIP CATALOG-FIRST BENCHMARK PATCH START ---"
end_marker = "# --- IRIP CATALOG-FIRST BENCHMARK PATCH END ---"

# Remove any older version of this patch.
text = re.sub(
    rf"\n?{re.escape(start_marker)}.*?{re.escape(end_marker)}\n?",
    "\n",
    text,
    flags=re.DOTALL,
)

patch = r'''

# --- IRIP CATALOG-FIRST BENCHMARK PATCH START ---
# V0.6: Benchmark spec table should use synced product catalog before Gemini/rules.
try:
    from app.services.catalog_benchmark_service import CatalogBenchmarkService

    if "visualization_service" in globals() and hasattr(visualization_service, "_benchmark_spec_table"):
        _irip_original_benchmark_spec_table = visualization_service._benchmark_spec_table

        def _irip_catalog_first_benchmark_spec_table(
            product_id: str,
            competitor_product_id: str | None = None,
            *args,
            **kwargs,
        ):
            competitor_id = (
                competitor_product_id
                or kwargs.get("competitor_id")
                or kwargs.get("compare_product_id")
                or kwargs.get("competitor_product_id")
            )

            try:
                catalog_table = CatalogBenchmarkService().build_spec_table(
                    product_id=product_id,
                    competitor_product_id=competitor_id,
                )
                if catalog_table:
                    return catalog_table
            except Exception:
                pass

            return _irip_original_benchmark_spec_table(
                product_id=product_id,
                competitor_product_id=competitor_id,
            )

        visualization_service._benchmark_spec_table = _irip_catalog_first_benchmark_spec_table
except Exception:
    pass
# --- IRIP CATALOG-FIRST BENCHMARK PATCH END ---
'''

path.write_text(text.rstrip() + "\n" + patch + "\n", encoding="utf-8")
print("Installed bottom-level catalog-first benchmark patch.")
