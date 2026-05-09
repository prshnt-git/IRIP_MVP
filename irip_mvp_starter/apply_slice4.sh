set -euo pipefail
ROOT="${1:-$(pwd)}"
cd "$ROOT"

python - <<'PY'
from pathlib import Path
root = Path.cwd()

# 1) Extend database schema
path = root / "backend/app/db/database.py"
text = path.read_text()
if "CREATE TABLE IF NOT EXISTS competitor_mappings" not in text:
    text = text.replace(
        "CREATE TABLE IF NOT EXISTS evaluation_runs (",
        """CREATE TABLE IF NOT EXISTS competitor_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL,
    competitor_product_id TEXT NOT NULL,
    comparison_group TEXT DEFAULT 'direct_competitor',
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, competitor_product_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
    FOREIGN KEY (competitor_product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evaluation_runs (""",
    )
if "idx_competitor_mappings_product" not in text:
    text = text.replace(
        "CREATE INDEX IF NOT EXISTS idx_reviews_processed_quality ON reviews_processed(product_id, quality_score);",
        """CREATE INDEX IF NOT EXISTS idx_reviews_processed_quality ON reviews_processed(product_id, quality_score);
CREATE INDEX IF NOT EXISTS idx_competitor_mappings_product ON competitor_mappings(product_id, competitor_product_id);""",
    )
path.write_text(text)

# 2) Add schemas
path = root / "backend/app/schemas/imports.py"
text = path.read_text()
if "class ProductCatalogImportResult" not in text:
    text += '''

class ProductCatalogImportResult(BaseModel):
    imported_products: int
    imported_mappings: int
    failed_count: int
    errors: list[ImportErrorItem] = []
    product_ids: list[str] = []


class ProductCatalogUrlImportRequest(BaseModel):
    url: str = Field(min_length=8, description="Public product catalog CSV URL")


class CompetitorItem(BaseModel):
    product_id: str
    product_name: str | None = None
    brand: str | None = None
    price_band: str | None = None
    comparison_group: str | None = None
    notes: str | None = None


class BenchmarkAspectItem(BaseModel):
    aspect: str
    own_score: float
    competitor_score: float
    gap: float
    own_mentions: int
    competitor_mentions: int
    own_confidence: float | None = None
    competitor_confidence: float | None = None
    confidence_label: str
    interpretation: str


class CompetitorBenchmark(BaseModel):
    product_id: str
    competitor_product_id: str
    period: dict
    own_review_count: int
    competitor_review_count: int
    benchmark_aspects: list[BenchmarkAspectItem]
    top_strengths: list[BenchmarkAspectItem]
    top_weaknesses: list[BenchmarkAspectItem]
'''
path.write_text(text)

# 3) Add product catalog service
service_path = root / "backend/app/services/product_catalog_service.py"
if not service_path.exists():
    service_path.write_text('''from __future__ import annotations

import csv
import io
import urllib.request
from urllib.parse import urlparse

from app.db.repository import ReviewRepository
from app.schemas.imports import ImportErrorItem, ProductCatalogImportResult


class ProductCatalogImportService:
    REQUIRED_COLUMNS = {"product_id"}

    def __init__(self, repository: ReviewRepository) -> None:
        self.repository = repository

    def import_csv_url(self, url: str) -> ProductCatalogImportResult:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="Only http/https CSV URLs are supported")],
            )
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "IRIP-MVP/0.1 catalog importer"})
            with urllib.request.urlopen(request, timeout=20) as response:
                raw_bytes = response.read(2_000_001)
        except Exception as exc:  # pragma: no cover
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Could not fetch catalog CSV URL: {exc}")],
            )
        if len(raw_bytes) > 2_000_000:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="Catalog CSV URL response is larger than 2 MB MVP limit")],
            )
        return self.import_csv_text(raw_bytes.decode("utf-8-sig"))

    def import_csv_text(self, csv_text: str) -> ProductCatalogImportResult:
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="CSV has no header row")],
            )
        headers = {h.strip() for h in reader.fieldnames if h}
        missing = sorted(self.REQUIRED_COLUMNS - headers)
        if missing:
            return ProductCatalogImportResult(
                imported_products=0,
                imported_mappings=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Missing required column(s): {', '.join(missing)}")],
            )

        imported_products = 0
        imported_mappings = 0
        errors: list[ImportErrorItem] = []
        product_ids: set[str] = set()

        for row_number, raw_row in enumerate(reader, start=2):
            row = {key.strip(): (value.strip() if isinstance(value, str) else value) for key, value in raw_row.items() if key}
            product_id = row.get("product_id") or ""
            if not product_id:
                errors.append(ImportErrorItem(row_number=row_number, reason="product_id is required"))
                continue
            try:
                self.repository.upsert_product(
                    product_id=product_id,
                    product_name=_empty_to_none(row.get("product_name")) or _empty_to_none(row.get("model")),
                    brand=_empty_to_none(row.get("brand")),
                    price_band=_empty_to_none(row.get("price_band")),
                    own_brand=_to_bool_or_none(row.get("own_brand")),
                )
                imported_products += 1
                product_ids.add(product_id)

                competitor_ids = _split_competitors(row.get("competitor_product_ids") or row.get("direct_competitor_ids"))
                for competitor_id in competitor_ids:
                    self.repository.upsert_product(product_id=competitor_id)
                    self.repository.save_competitor_mapping(
                        product_id=product_id,
                        competitor_product_id=competitor_id,
                        comparison_group=_empty_to_none(row.get("comparison_group")) or "direct_competitor",
                        notes=_empty_to_none(row.get("mapping_notes")) or _empty_to_none(row.get("notes")),
                    )
                    imported_mappings += 1
            except ValueError as exc:
                errors.append(ImportErrorItem(row_number=row_number, reason=str(exc)))

        return ProductCatalogImportResult(
            imported_products=imported_products,
            imported_mappings=imported_mappings,
            failed_count=len(errors),
            errors=errors[:50],
            product_ids=sorted(product_ids),
        )


def _empty_to_none(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


def _to_bool_or_none(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y", "own", "owned"}:
        return True
    if normalized in {"false", "0", "no", "n", "competitor"}:
        return False
    return None


def _split_competitors(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.replace(";", ",").split(",") if item.strip()]
''')

# 4) Extend repository
path = root / "backend/app/db/repository.py"
text = path.read_text()
if "def upsert_product" not in text:
    insert_after = '''    def reset_review_data(self) -> dict:
        with connect(self.database_path) as connection:
            connection.execute("DELETE FROM aspect_sentiments")
            connection.execute("DELETE FROM reviews_processed")
            connection.execute("DELETE FROM reviews_raw")
            connection.execute("DELETE FROM products")
        self.seed_lexicon()
        return self.get_database_stats()

'''
    new_methods = '''    def upsert_product(
        self,
        product_id: str,
        product_name: str | None = None,
        brand: str | None = None,
        price_band: str | None = None,
        own_brand: bool | None = None,
    ) -> None:
        if not product_id:
            raise ValueError("product_id is required")
        with connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO products (product_id, product_name, brand, price_band, own_brand)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_name = COALESCE(excluded.product_name, products.product_name),
                    brand = COALESCE(excluded.brand, products.brand),
                    price_band = COALESCE(excluded.price_band, products.price_band),
                    own_brand = COALESCE(excluded.own_brand, products.own_brand),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (product_id, product_name, brand, price_band, int(own_brand) if own_brand is not None else None),
            )

    def save_competitor_mapping(
        self,
        product_id: str,
        competitor_product_id: str,
        comparison_group: str = "direct_competitor",
        notes: str | None = None,
    ) -> None:
        if product_id == competitor_product_id:
            raise ValueError("A product cannot be mapped as its own competitor")
        with connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO competitor_mappings (product_id, competitor_product_id, comparison_group, notes)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id, competitor_product_id) DO UPDATE SET
                    comparison_group = excluded.comparison_group,
                    notes = COALESCE(excluded.notes, competitor_mappings.notes)
                """,
                (product_id, competitor_product_id, comparison_group, notes),
            )

    def list_competitors(self, product_id: str) -> list[dict]:
        with connect(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    c.competitor_product_id AS product_id,
                    p.product_name,
                    p.brand,
                    p.price_band,
                    c.comparison_group,
                    c.notes
                FROM competitor_mappings c
                LEFT JOIN products p ON p.product_id = c.competitor_product_id
                WHERE c.product_id = ?
                ORDER BY c.comparison_group ASC, p.product_name ASC, c.competitor_product_id ASC
                """,
                (product_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_competitor_benchmark(
        self,
        product_id: str,
        competitor_product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        own_summary = self.get_product_summary(product_id, start_date, end_date)
        competitor_summary = self.get_product_summary(competitor_product_id, start_date, end_date)
        own_aspects = {item["aspect"]: item for item in self.get_aspect_summary(product_id, start_date, end_date)}
        competitor_aspects = {item["aspect"]: item for item in self.get_aspect_summary(competitor_product_id, start_date, end_date)}
        all_aspects = sorted(set(own_aspects) | set(competitor_aspects))

        benchmark_aspects = []
        for aspect in all_aspects:
            own = own_aspects.get(aspect) or _empty_aspect(aspect)
            competitor = competitor_aspects.get(aspect) or _empty_aspect(aspect)
            gap = round(float(own["aspect_score"]) - float(competitor["aspect_score"]), 1)
            confidence_label = _benchmark_confidence_label(
                own_mentions=int(own["mentions"]),
                competitor_mentions=int(competitor["mentions"]),
                own_confidence=own.get("avg_confidence"),
                competitor_confidence=competitor.get("avg_confidence"),
            )
            benchmark_aspects.append(
                {
                    "aspect": aspect,
                    "own_score": float(own["aspect_score"]),
                    "competitor_score": float(competitor["aspect_score"]),
                    "gap": gap,
                    "own_mentions": int(own["mentions"]),
                    "competitor_mentions": int(competitor["mentions"]),
                    "own_confidence": own.get("avg_confidence"),
                    "competitor_confidence": competitor.get("avg_confidence"),
                    "confidence_label": confidence_label,
                    "interpretation": _gap_interpretation(aspect, gap, confidence_label),
                }
            )

        strengths = sorted(
            [item for item in benchmark_aspects if item["gap"] > 0],
            key=lambda item: (item["gap"], item["own_mentions"] + item["competitor_mentions"]),
            reverse=True,
        )[:3]
        weaknesses = sorted(
            [item for item in benchmark_aspects if item["gap"] < 0],
            key=lambda item: (item["gap"], -(item["own_mentions"] + item["competitor_mentions"])),
        )[:3]
        return {
            "product_id": product_id,
            "competitor_product_id": competitor_product_id,
            "period": {"start_date": start_date, "end_date": end_date},
            "own_review_count": own_summary["review_count"],
            "competitor_review_count": competitor_summary["review_count"],
            "benchmark_aspects": benchmark_aspects,
            "top_strengths": strengths,
            "top_weaknesses": weaknesses,
        }

'''
    text = text.replace(insert_after, insert_after + new_methods)

if "competitor_mappings" not in text.split("def get_database_stats",1)[1].split("]",1)[0]:
    text = text.replace('            "evaluation_runs",\n        ]', '            "evaluation_runs",\n            "competitor_mappings",\n        ]')

if "def _empty_aspect" not in text:
    text += '''


def _empty_aspect(aspect: str) -> dict:
    return {
        "aspect": aspect,
        "mentions": 0,
        "positive_count": 0,
        "negative_count": 0,
        "neutral_count": 0,
        "avg_confidence": None,
        "aspect_score": 0.0,
    }


def _benchmark_confidence_label(
    own_mentions: int,
    competitor_mentions: int,
    own_confidence: float | None,
    competitor_confidence: float | None,
) -> str:
    total_mentions = own_mentions + competitor_mentions
    avg_conf = _safe_average([own_confidence, competitor_confidence])
    if total_mentions >= 50 and avg_conf >= 0.82:
        return "high"
    if total_mentions >= 10 and avg_conf >= 0.7:
        return "medium"
    return "low"


def _safe_average(values: list[float | None]) -> float:
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return 0.0
    return sum(valid) / len(valid)


def _gap_interpretation(aspect: str, gap: float, confidence_label: str) -> str:
    if abs(gap) < 5:
        base = f"Near parity on {aspect}."
    elif gap > 0:
        base = f"Our product is ahead on {aspect} by {gap} points."
    else:
        base = f"Competitor is ahead on {aspect} by {abs(gap)} points."
    if confidence_label == "low":
        return base + " Treat as directional because evidence volume/confidence is still low."
    return base
'''
path.write_text(text)

# 5) Extend main.py
path = root / "backend/app/main.py"
text = path.read_text()
text = text.replace(
    "from app.schemas.imports import AspectSummaryItem, CsvUrlImportRequest, DatabaseStats, ImportResult, ProductSummary",
    "from app.schemas.imports import AspectSummaryItem, BenchmarkAspectItem, CompetitorBenchmark, CompetitorItem, CsvUrlImportRequest, DatabaseStats, ImportResult, ProductCatalogImportResult, ProductCatalogUrlImportRequest, ProductSummary",
)
if "from app.services.product_catalog_service" not in text:
    text = text.replace(
        "from app.services.import_service import ReviewImportService\n",
        "from app.services.import_service import ReviewImportService\nfrom app.services.product_catalog_service import ProductCatalogImportService\n",
    )
if "catalog_importer =" not in text:
    text = text.replace(
        "importer = ReviewImportService(repository=repository, analyzer=analyzer)\n",
        "importer = ReviewImportService(repository=repository, analyzer=analyzer)\ncatalog_importer = ProductCatalogImportService(repository=repository)\n",
    )
if "def import_product_catalog_csv" not in text:
    insert = '''

@app.post("/products/import-catalog-csv", response_model=ProductCatalogImportResult)
async def import_product_catalog_csv(file: UploadFile = File(...)) -> ProductCatalogImportResult:
    raw_bytes = await file.read()
    csv_text = raw_bytes.decode("utf-8-sig")
    return catalog_importer.import_csv_text(csv_text)


@app.post("/products/import-catalog-csv-url", response_model=ProductCatalogImportResult)
def import_product_catalog_csv_url(payload: ProductCatalogUrlImportRequest) -> ProductCatalogImportResult:
    return catalog_importer.import_csv_url(payload.url)


@app.get("/products/{product_id}/competitors", response_model=list[CompetitorItem])
def list_product_competitors(product_id: str) -> list[CompetitorItem]:
    return [CompetitorItem(**item) for item in repository.list_competitors(product_id)]


@app.get("/products/{product_id}/benchmark/{competitor_product_id}", response_model=CompetitorBenchmark)
def get_competitor_benchmark(
    product_id: str,
    competitor_product_id: str,
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> CompetitorBenchmark:
    data = repository.get_competitor_benchmark(product_id, competitor_product_id, start_date, end_date)
    data["benchmark_aspects"] = [BenchmarkAspectItem(**item) for item in data["benchmark_aspects"]]
    data["top_strengths"] = [BenchmarkAspectItem(**item) for item in data["top_strengths"]]
    data["top_weaknesses"] = [BenchmarkAspectItem(**item) for item in data["top_weaknesses"]]
    return CompetitorBenchmark(**data)
'''
    text = text.replace("@app.get(\"/products\")\ndef list_products()", insert + "\n\n@app.get(\"/products\")\ndef list_products()")
path.write_text(text)

# 6) Add sample product catalog
sample = root / "backend/sample_data/sample_product_catalog.csv"
if not sample.exists():
    sample.write_text("""product_id,product_name,brand,price_band,own_brand,competitor_product_ids,comparison_group,notes
phone_a,Demo Phone A,TECNO,10000-15000,true,phone_b,direct_competitor,Demo competitor mapping for local testing
phone_b,Competitor Phone B,CompetitorBrand,10000-15000,false,,direct_competitor,
""")

# 7) Add tests
path = root / "backend/tests/test_competitor_benchmark.py"
if not path.exists():
    path.write_text(r'''from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.product_catalog_service import ProductCatalogImportService


def test_catalog_mapping_and_benchmark(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()
    catalog = ProductCatalogImportService(repo)
    result = catalog.import_csv_text(
        "product_id,product_name,brand,price_band,own_brand,competitor_product_ids\n"
        "phone_a,Demo Phone A,TECNO,10000-15000,true,phone_b\n"
        "phone_b,Competitor Phone B,Other,10000-15000,false,\n"
    )
    assert result.imported_products == 2
    assert result.imported_mappings == 1
    assert repo.list_competitors("phone_a")[0]["product_id"] == "phone_b"

    review_a = ReviewInput(
        review_id="a1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-05-01",
        raw_text="Camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )
    review_b = ReviewInput(
        review_id="b1",
        product_id="phone_b",
        product_name="Competitor Phone B",
        source="flipkart",
        rating=4,
        review_date="2026-05-01",
        raw_text="Battery backup mast hai but camera bekar hai",
        verified_purchase=True,
    )
    repo.save_review_analysis(review_a, analyzer.analyze(review_a))
    repo.save_review_analysis(review_b, analyzer.analyze(review_b))

    benchmark = repo.get_competitor_benchmark("phone_a", "phone_b")
    assert benchmark["product_id"] == "phone_a"
    assert benchmark["competitor_product_id"] == "phone_b"
    aspects = {item["aspect"]: item for item in benchmark["benchmark_aspects"]}
    assert "battery" in aspects
    assert "camera" in aspects
    assert aspects["camera"]["gap"] > 0
    assert aspects["battery"]["gap"] < 0
''')
PY

echo "Slice 4 competitor mapping and benchmark files applied."
