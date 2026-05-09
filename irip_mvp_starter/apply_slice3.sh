set -euo pipefail

ROOT="${1:-$(pwd)}"
cd "$ROOT"

mkdir -p scripts

cat > scripts/reset_db.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_DIR="$ROOT/backend/data"
DB_FILE="$DB_DIR/irip_mvp.db"
rm -f "$DB_FILE" "$DB_FILE-shm" "$DB_FILE-wal"
mkdir -p "$DB_DIR"
echo "Reset complete: removed SQLite database at $DB_FILE"
echo "Restart uvicorn, then re-import your CSV."
EOF
chmod +x scripts/reset_db.sh

cat > scripts/smoke_test.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

echo "Checking $BASE_URL/health"
curl -fsS "$BASE_URL/health" | python -m json.tool

echo "Checking products"
curl -fsS "$BASE_URL/products" | python -m json.tool

echo "Checking DB stats"
curl -fsS "$BASE_URL/debug/db-stats" | python -m json.tool
EOF
chmod +x scripts/smoke_test.sh

python - <<'PY'
from pathlib import Path

root = Path.cwd()

imports_path = root / "backend/app/schemas/imports.py"
text = imports_path.read_text()
if "class CsvUrlImportRequest" not in text:
    text += '''

class CsvUrlImportRequest(BaseModel):
    url: str = Field(min_length=8, description="Public CSV URL, such as a Google Sheets published CSV link")


class DatabaseStats(BaseModel):
    products: int
    reviews_raw: int
    reviews_processed: int
    aspect_sentiments: int
    living_lexicon: int
    evaluation_runs: int
'''
    imports_path.write_text(text)

service_path = root / "backend/app/services/import_service.py"
text = service_path.read_text()
if "import urllib.request" not in text:
    text = text.replace("import uuid\n", "import uuid\nimport urllib.request\nfrom urllib.parse import urlparse\n")
if "def import_csv_url" not in text:
    marker = "    def import_csv_text(self, csv_text: str) -> ImportResult:\n"
    insert = '''    def import_csv_url(self, url: str) -> ImportResult:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="Only http/https CSV URLs are supported")],
            )

        try:
            request = urllib.request.Request(
                url,
                headers={"User-Agent": "IRIP-MVP/0.1 CSV importer"},
            )
            with urllib.request.urlopen(request, timeout=20) as response:
                raw_bytes = response.read(5_000_001)
        except Exception as exc:  # pragma: no cover - network failures vary by environment
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason=f"Could not fetch CSV URL: {exc}")],
            )

        if len(raw_bytes) > 5_000_000:
            return ImportResult(
                imported_count=0,
                failed_count=1,
                errors=[ImportErrorItem(row_number=0, reason="CSV URL response is larger than 5 MB limit for MVP")],
            )

        csv_text = raw_bytes.decode("utf-8-sig")
        return self.import_csv_text(csv_text)

'''
    text = text.replace(marker, insert + marker)
    service_path.write_text(text)

repo_path = root / "backend/app/db/repository.py"
text = repo_path.read_text()
if "def get_database_stats" not in text:
    marker = "    def list_products(self) -> list[dict]:\n"
    insert = '''    def get_database_stats(self) -> dict:
        tables = [
            "products",
            "reviews_raw",
            "reviews_processed",
            "aspect_sentiments",
            "living_lexicon",
            "evaluation_runs",
        ]
        with connect(self.database_path) as connection:
            return {
                table: int(connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"])
                for table in tables
            }

    def reset_review_data(self) -> dict:
        with connect(self.database_path) as connection:
            connection.execute("DELETE FROM aspect_sentiments")
            connection.execute("DELETE FROM reviews_processed")
            connection.execute("DELETE FROM reviews_raw")
            connection.execute("DELETE FROM products")
        self.seed_lexicon()
        return self.get_database_stats()

'''
    text = text.replace(marker, insert + marker)
    repo_path.write_text(text)

main_path = root / "backend/app/main.py"
text = main_path.read_text()
text = text.replace(
    "from app.schemas.imports import AspectSummaryItem, ImportResult, ProductSummary",
    "from app.schemas.imports import AspectSummaryItem, CsvUrlImportRequest, DatabaseStats, ImportResult, ProductSummary",
)
if "def import_reviews_csv_url" not in text:
    marker = '''@app.post("/reviews/import-csv", response_model=ImportResult)
async def import_reviews_csv(file: UploadFile = File(...)) -> ImportResult:
    raw_bytes = await file.read()
    csv_text = raw_bytes.decode("utf-8-sig")
    return importer.import_csv_text(csv_text)

'''
    insert = marker + '''@app.post("/reviews/import-csv-url", response_model=ImportResult)
def import_reviews_csv_url(payload: CsvUrlImportRequest) -> ImportResult:
    return importer.import_csv_url(payload.url)


@app.get("/debug/db-stats", response_model=DatabaseStats)
def get_database_stats() -> DatabaseStats:
    return DatabaseStats(**repository.get_database_stats())


@app.post("/debug/reset-review-data", response_model=DatabaseStats)
def reset_review_data() -> DatabaseStats:
    return DatabaseStats(**repository.reset_review_data())

'''
    text = text.replace(marker, insert)
    main_path.write_text(text)

test_path = root / "backend/tests/test_debug_repository.py"
if not test_path.exists():
    test_path.write_text('''from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput


def test_database_stats_and_reset(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()
    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-05-01",
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )
    repo.save_review_analysis(review, analyzer.analyze(review))

    stats = repo.get_database_stats()
    assert stats["products"] == 1
    assert stats["reviews_raw"] == 1
    assert stats["aspect_sentiments"] >= 2
    assert stats["living_lexicon"] > 0

    reset_stats = repo.reset_review_data()
    assert reset_stats["products"] == 0
    assert reset_stats["reviews_raw"] == 0
    assert reset_stats["aspect_sentiments"] == 0
    assert reset_stats["living_lexicon"] > 0
''')
PY

echo "Slice 3 files applied."
