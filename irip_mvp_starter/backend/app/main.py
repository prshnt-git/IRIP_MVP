from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from fastapi import FastAPI, File, HTTPException, Query, UploadFile

from app.core.config import get_settings
from app.db.database import connect
from app.db.repository import ReviewRepository
from app.pipeline.hybrid_analyzer import HybridReviewAnalyzer
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.active_evaluation import (
    ActiveEvaluationPromoteRequest,
    ActiveEvaluationQueueBuildResponse,
    ActiveEvaluationQueueItem,
    ActiveEvaluationQueueStatusUpdate,
    GoldenReviewCaseItem,
    GoldenReviewCaseUpdateRequest,
)
from app.schemas.evaluation import EvaluationCase, EvaluationResult
from app.schemas.feedback import (
    ExtractionFeedbackCreate,
    ExtractionFeedbackItem,
    ProviderQualityItem,
)
from app.schemas.imports import (
    AspectSummaryItem,
    BenchmarkAspectItem,
    CompetitorBenchmark,
    CompetitorItem,
    CsvUrlImportRequest,
    DatabaseStats,
    ImportPreviewResponse,
    ImportResult,
    ProductCatalogImportResult,
    ProductCatalogUrlImportRequest,
    ProductSummary,
)
from app.schemas.intelligence import (
    IntelligenceBriefResponse,
    ProductForecastResponse,
    ProductThemesResponse,
)
from app.schemas.lexicon import LexiconEntryItem
from app.schemas.llm import (
    LlmModeUpdateRequest,
    LlmModeUpdateResponse,
    LlmProviderStatus,
    LlmReviewExtractionRequest,
    LlmReviewExtractionResponse,
)
from app.schemas.news import (
    NewsBriefResponse,
    NewsIngestResponse,
    NewsIngestRssRequest,
    NewsIngestXmlRequest,
    NewsItem,
    NewsRescoreResponse,
    TrustedNewsSourceItem,
)
from app.schemas.review import ReviewAnalysis, ReviewInput, Sentiment
from app.schemas.router import ProviderConfig
from app.schemas.reports import ExecutiveReportResponse
from app.schemas.system import SystemReadinessResponse, SystemVersionResponse
from app.schemas.visuals import VisualDashboardResponse
from app.services.active_evaluation_service import ActiveEvaluationService
from app.services.evaluation_service import EvaluationService
from app.services.golden_evaluator import GoldenEvaluator
from app.services.import_quality_service import ImportQualityService
from app.services.import_service import ReviewImportService
from app.services.intelligence_service import IntelligenceService
from app.services.llm_service import LlmService
from app.services.news_brief_service import NewsBriefService
from app.services.news_ingestion_service import NewsIngestionService
from app.services.product_catalog_service import ProductCatalogImportService
from app.services.resource_router import ResourceRouter
from app.services.trusted_news_sources import TrustedNewsSourceService
from app.services.executive_report_service import ExecutiveReportService
from app.services.system_readiness_service import SystemReadinessService
from app.services.visualization_service import VisualizationService

settings = get_settings()

app = FastAPI(title=settings.app_name, version=settings.app_version)

router = ResourceRouter()

repository = ReviewRepository(settings.database_path)

rule_analyzer = ReviewAnalyzer()
llm_service = LlmService()
analyzer = HybridReviewAnalyzer(rule_analyzer=rule_analyzer, llm_service=llm_service)

evaluator = EvaluationService(analyzer=analyzer)
importer = ReviewImportService(repository=repository, analyzer=analyzer)
catalog_importer = ProductCatalogImportService(repository=repository)
intelligence_service = IntelligenceService(repository=repository)
active_evaluation_service = ActiveEvaluationService(repository)
import_quality_service = ImportQualityService()

trusted_news_source_service = TrustedNewsSourceService(settings.database_path)
news_ingestion_service = NewsIngestionService(
    database_path=settings.database_path,
    source_service=trusted_news_source_service,
)
news_brief_service = NewsBriefService(news_ingestion_service)
executive_report_service = ExecutiveReportService(
    repository=repository,
    intelligence_service=intelligence_service,
    news_brief_service=news_brief_service,
)
system_readiness_service = SystemReadinessService(
    repository=repository,
    news_ingestion_service=news_ingestion_service,
)

visualization_service = VisualizationService(
    repository=repository,
    executive_report_service=executive_report_service,
    news_brief_service=news_brief_service,
    system_readiness_service=system_readiness_service,
)

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



# --- IRIP Data Catalog API START ---
@app.get("/data/catalog")
def list_data_catalog(own_only: bool | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.list_catalog(own_only=own_only)


@app.post("/data/catalog/import-csv")
async def import_data_catalog_csv(file: UploadFile = File(...)):
    from app.services.product_catalog_service import ProductCatalogService

    try:
        content = await file.read()
        csv_text = content.decode("utf-8-sig")
        service = ProductCatalogService()
        result = service.import_csv_text(csv_text)
        return {
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "errors": result.errors,
            "product_ids": result.product_ids,
            "storage_path": result.storage_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/data/catalog/import-csv-url")
def import_data_catalog_csv_url(url: str):
    from app.services.product_catalog_service import ProductCatalogService

    try:
        service = ProductCatalogService()
        result = service.import_csv_url(url)
        return {
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "errors": result.errors,
            "product_ids": result.product_ids,
            "storage_path": result.storage_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/data/resolve-product")
def resolve_data_product(product_name: str, brand: str | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.resolve_product(product_name=product_name, brand=brand)
# --- IRIP Data Catalog API END ---


# --- IRIP Catalog-Aware Review Import API START ---
@app.post("/data/reviews/import-csv-normalized")
async def import_reviews_catalog_normalized(file: UploadFile = File(...)):
    """V0.8: Import messy review CSV after resolving product names against catalog.

    Flow:
    messy CSV -> catalog resolver -> normalized CSV rows -> existing ReviewImportService.
    """
    import csv
    import io

    from app.services.review_catalog_normalizer_service import ReviewCatalogNormalizerService

    try:
        content = await file.read()
        csv_text = content.decode("utf-8-sig")

        normalizer = ReviewCatalogNormalizerService()
        result = normalizer.normalize_csv_text(csv_text)

        normalization_payload = {
            "imported_candidate_count": result.imported_candidate_count,
            "skipped_count": result.skipped_count,
            "duplicate_count": result.duplicate_count,
            "unresolved_count": result.unresolved_count,
            "errors": result.errors,
            "unresolved_rows": result.unresolved_rows,
        }

        if not result.output_rows:
            return {
                "version": "V0.8",
                "status": "no_importable_rows",
                "normalization": normalization_payload,
                "import_result": None,
            }

        fields = [
            "review_id",
            "product_id",
            "product_name",
            "brand",
            "source",
            "marketplace",
            "raw_text",
            "rating",
            "review_date",
        ]

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fields)
        writer.writeheader()
        writer.writerows(result.output_rows)

        import_result = importer.import_csv_text(
            buffer.getvalue(),
            discovered_via="catalog_normalized_csv",
        )

        import_payload = (
            import_result.model_dump()
            if hasattr(import_result, "model_dump")
            else import_result
        )

        return {
            "version": "V0.8",
            "status": "import_complete",
            "normalization": normalization_payload,
            "import_result": import_payload,
        }

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
# --- IRIP Catalog-Aware Review Import API END ---








@app.get("/")
def root() -> dict:
    return {
        "status": "ok",
        "message": "IRIP MVP API is running. Use /health for status or /docs for Swagger.",
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "app": settings.app_name, "version": settings.app_version}


@app.get("/system/version", response_model=SystemVersionResponse)
def get_system_version() -> SystemVersionResponse:
    result = system_readiness_service.version(api_version=settings.app_version)
    return SystemVersionResponse(**result)


@app.get("/system/readiness", response_model=SystemReadinessResponse)
def get_system_readiness() -> SystemReadinessResponse:
    result = system_readiness_service.readiness(product_version="v1.0")
    return SystemReadinessResponse(**result)


@app.post("/reviews/import-preview", response_model=ImportPreviewResponse)
def preview_import_csv_text(payload: dict) -> ImportPreviewResponse:
    csv_text = str(payload.get("csv_text") or "")

    if not csv_text.strip():
        raise HTTPException(status_code=400, detail="csv_text is required.")

    result = import_quality_service.preview_csv_text(csv_text)
    return ImportPreviewResponse(**result)


@app.post("/reviews/import-csv-url-preview", response_model=ImportPreviewResponse)
def preview_import_csv_url(payload: dict) -> ImportPreviewResponse:
    url = str(payload.get("url") or "")

    if not url.strip():
        raise HTTPException(status_code=400, detail="url is required.")

    try:
        with urlopen(url, timeout=20) as response:
            csv_text = response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError, UnicodeDecodeError) as error:
        raise HTTPException(status_code=400, detail=f"Unable to fetch CSV URL: {error}") from error

    result = import_quality_service.preview_csv_text(csv_text)
    return ImportPreviewResponse(**result)


@app.get("/resources/providers")
def list_providers() -> dict[str, list[ProviderConfig]]:
    return router.list_providers()


@app.post("/reviews/analyze", response_model=ReviewAnalysis)
def analyze_review(review: ReviewInput) -> ReviewAnalysis:
    return analyzer.analyze(review)


@app.post("/reviews/import-csv", response_model=ImportResult)
async def import_reviews_csv(file: UploadFile = File(...)) -> ImportResult:
    raw_bytes = await file.read()
    csv_text = raw_bytes.decode("utf-8-sig")
    return importer.import_csv_text(csv_text)


@app.post("/reviews/import-csv-url", response_model=ImportResult)
def import_reviews_csv_url(payload: CsvUrlImportRequest) -> ImportResult:
    return importer.import_csv_url(payload.url)


@app.get("/debug/db-stats", response_model=DatabaseStats)
def get_database_stats() -> DatabaseStats:
    return DatabaseStats(**repository.get_database_stats())


@app.post("/debug/reset-review-data", response_model=DatabaseStats)
def reset_review_data() -> DatabaseStats:
    return DatabaseStats(**repository.reset_review_data())


@app.get("/llm/status", response_model=LlmProviderStatus)
def get_llm_status() -> LlmProviderStatus:
    return llm_service.status()


@app.post("/llm/mode", response_model=LlmModeUpdateResponse)
def update_llm_mode(payload: LlmModeUpdateRequest) -> LlmModeUpdateResponse:
    try:
        mode = llm_service.set_mode(payload.mode)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return LlmModeUpdateResponse(
        mode=mode,
        message=f"LLM mode updated to {mode} for this backend process.",
    )


@app.post("/llm/extract-review", response_model=LlmReviewExtractionResponse)
def extract_review_with_llm(
    payload: LlmReviewExtractionRequest,
) -> LlmReviewExtractionResponse:
    return llm_service.extract_review_intelligence(payload)


@app.post("/feedback/extraction", response_model=ExtractionFeedbackItem)
def create_extraction_feedback(
    payload: ExtractionFeedbackCreate,
) -> ExtractionFeedbackItem:
    item = repository.save_extraction_feedback(
        review_id=payload.review_id,
        product_id=payload.product_id,
        aspect=payload.aspect,
        predicted_sentiment=payload.predicted_sentiment,
        provider=payload.provider,
        is_correct=payload.is_correct,
        corrected_aspect=payload.corrected_aspect,
        corrected_sentiment=payload.corrected_sentiment,
        note=payload.note,
    )
    return ExtractionFeedbackItem(**item)


@app.get("/feedback/extraction", response_model=list[ExtractionFeedbackItem])
def list_extraction_feedback(
    product_id: str | None = Query(default=None),
    provider: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> list[ExtractionFeedbackItem]:
    return [
        ExtractionFeedbackItem(**item)
        for item in repository.list_extraction_feedback(
            product_id=product_id,
            provider=provider,
            limit=limit,
        )
    ]


@app.get("/feedback/provider-quality", response_model=list[ProviderQualityItem])
def get_provider_quality() -> list[ProviderQualityItem]:
    return [ProviderQualityItem(**item) for item in repository.get_provider_quality()]


@app.post("/products/import-catalog-csv", response_model=ProductCatalogImportResult)
async def import_product_catalog_csv(file: UploadFile = File(...)) -> ProductCatalogImportResult:
    raw_bytes = await file.read()
    csv_text = raw_bytes.decode("utf-8-sig")
    return catalog_importer.import_csv_text(csv_text)


@app.post("/products/import-catalog-csv-url", response_model=ProductCatalogImportResult)
def import_product_catalog_csv_url(
    payload: ProductCatalogUrlImportRequest,
) -> ProductCatalogImportResult:
    return catalog_importer.import_csv_url(payload.url)


@app.get("/products")
def list_products():
    """Return review-backed products plus catalog-backed products."""
    from app.services.product_catalog_service import ProductCatalogService

    review_products = repository.list_products()
    catalog_products = ProductCatalogService().list_catalog()

    merged = {}

    for item in review_products:
        product_id = item.get("product_id")
        if product_id:
            merged[product_id] = dict(item)

    for item in catalog_products:
        product_id = item.get("product_id")
        if not product_id:
            continue

        catalog_view = {
            "product_id": product_id,
            "product_name": item.get("product_name"),
            "brand": item.get("brand"),
            "parent_company": item.get("company_name"),
            "price_band": item.get("price_band"),
            "own_brand": bool(item.get("is_own_product")),
            "marketplace": None,
            "marketplace_product_id": None,
            "marketplace_product_url": item.get("marketplace_url"),
            "launch_period": item.get("launch_date"),
            "comparison_group": "own_product" if item.get("is_own_product") else "competitor",
            "review_count": 0,
            "first_review_date": None,
            "latest_review_date": None,
        }

        if product_id in merged:
            existing = merged[product_id]
            merged[product_id] = {
                **catalog_view,
                **existing,
                "brand": existing.get("brand") or catalog_view["brand"],
                "parent_company": existing.get("parent_company") or catalog_view["parent_company"],
                "price_band": existing.get("price_band") or catalog_view["price_band"],
                "own_brand": bool(existing.get("own_brand") or catalog_view["own_brand"]),
                "marketplace_product_url": existing.get("marketplace_product_url") or catalog_view["marketplace_product_url"],
                "launch_period": existing.get("launch_period") or catalog_view["launch_period"],
            }
        else:
            merged[product_id] = catalog_view

    products = list(merged.values())

    # Hide obvious demo/test products from user-facing product lists.
    # Backend tests and stored data remain untouched.
    hidden_demo_ids = {"phone_a", "phone_b"}
    products = [
        product
        for product in products
        if product.get("product_id") not in hidden_demo_ids
        and "demo phone" not in str(product.get("product_name") or "").lower()
        and "competitor phone" not in str(product.get("product_name") or "").lower()
    ]

    products.sort(
        key=lambda item: (
            not bool(item.get("own_brand")),
            str(item.get("brand") or ""),
            str(item.get("product_name") or ""),
        )
    )


    # Final demo scope: only products launched from Nov 2025 to May 2026.
    # Old/test products remain internally available but are hidden from the user-facing product list.
    final_demo_product_ids = {
        "infinix_note_60_pro",
        "itel_zeno_200",
        "tecno_pova_curve_2_5g",
        "realme_narzo_90x_5g",
        "iqoo_z_11x_5g",
    }

    products = [
        product
        for product in products
        if product.get("product_id") in final_demo_product_ids
    ]

    return products
@app.get("/products/catalog")
def list_product_catalog(own_only: bool | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.list_catalog(own_only=own_only)


@app.post("/products/catalog/import-csv")
async def import_product_catalog_csv(file: UploadFile = File(...)):
    from app.services.product_catalog_service import ProductCatalogService

    try:
        content = await file.read()
        csv_text = content.decode("utf-8-sig")
        service = ProductCatalogService()
        result = service.import_csv_text(csv_text)
        return {
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "errors": result.errors,
            "product_ids": result.product_ids,
            "storage_path": result.storage_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/products/catalog/import-csv-url")
def import_product_catalog_csv_url(url: str):
    from app.services.product_catalog_service import ProductCatalogService

    try:
        service = ProductCatalogService()
        result = service.import_csv_url(url)
        return {
            "imported_count": result.imported_count,
            "updated_count": result.updated_count,
            "skipped_count": result.skipped_count,
            "failed_count": result.failed_count,
            "errors": result.errors,
            "product_ids": result.product_ids,
            "storage_path": result.storage_path,
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/products/resolve")
def resolve_product(product_name: str, brand: str | None = None):
    from app.services.product_catalog_service import ProductCatalogService

    service = ProductCatalogService()
    return service.resolve_product(product_name=product_name, brand=brand)
# --- IRIP Product Catalog API END ---

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
    data = repository.get_competitor_benchmark(
        product_id,
        competitor_product_id,
        start_date,
        end_date,
    )
    data["benchmark_aspects"] = [BenchmarkAspectItem(**item) for item in data["benchmark_aspects"]]
    data["top_strengths"] = [BenchmarkAspectItem(**item) for item in data["top_strengths"]]
    data["top_weaknesses"] = [BenchmarkAspectItem(**item) for item in data["top_weaknesses"]]
    return CompetitorBenchmark(**data)


@app.get("/products/{product_id}/themes", response_model=ProductThemesResponse)
def get_product_themes(
    product_id: str,
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
    limit: int = Query(default=5, ge=1, le=20),
) -> ProductThemesResponse:
    return intelligence_service.get_product_themes(
        product_id=product_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )


@app.get("/products/{product_id}/forecast", response_model=ProductForecastResponse)
def get_product_forecast(
    product_id: str,
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> ProductForecastResponse:
    return intelligence_service.get_product_forecast(
        product_id=product_id,
        start_date=start_date,
        end_date=end_date,
    )


@app.get("/products/{product_id}/intelligence-brief", response_model=IntelligenceBriefResponse)
def get_product_intelligence_brief(
    product_id: str,
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> IntelligenceBriefResponse:
    return intelligence_service.get_intelligence_brief(
        product_id=product_id,
        start_date=start_date,
        end_date=end_date,
    )


@app.get("/products/{product_id}/summary", response_model=ProductSummary)
def get_product_summary(
    product_id: str,
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> ProductSummary:
    return ProductSummary(**repository.get_product_summary(product_id, start_date, end_date))


@app.get("/products/{product_id}/aspects", response_model=list[AspectSummaryItem])
def get_product_aspects(
    product_id: str,
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> list[AspectSummaryItem]:
    return [
        AspectSummaryItem(**item)
        for item in repository.get_aspect_summary(product_id, start_date, end_date)
    ]


@app.get("/products/{product_id}/evidence")
def get_review_evidence(
    product_id: str,
    aspect: str | None = None,
    sentiment: Sentiment | None = None,
    limit: int = Query(default=10, ge=1, le=50),
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> list[dict]:
    return repository.list_evidence(
        product_id=product_id,
        aspect=aspect,
        sentiment=sentiment,
        limit=limit,
        start_date=start_date,
        end_date=end_date,
    )


@app.get("/lexicon", response_model=list[LexiconEntryItem])
def list_lexicon_entries(
    search: str | None = Query(default=None),
    aspect: str | None = Query(default=None),
    sentiment_prior: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> list[LexiconEntryItem]:
    return [
        LexiconEntryItem(**item)
        for item in repository.list_lexicon_entries(
            search=search,
            aspect=aspect,
            sentiment_prior=sentiment_prior,
            limit=limit,
        )
    ]


@app.post("/evaluation/run", response_model=EvaluationResult)
def run_evaluation(cases: list[EvaluationCase]) -> EvaluationResult:
    return evaluator.evaluate(cases)


def _golden_report_to_dict(mode: str, report) -> dict:
    return {
        "mode": mode,
        "total_cases": report.total_cases,
        "total_expected": report.total_expected,
        "total_predicted": report.total_predicted,
        "total_matched": report.total_matched,
        "total_sentiment_matched": report.total_sentiment_matched,
        "aspect_recall": report.aspect_recall,
        "sentiment_accuracy_on_matched": report.sentiment_accuracy_on_matched,
        "over_extraction_count": report.over_extraction_count,
        "exact_case_pass_count": report.exact_case_pass_count,
        "exact_case_pass_rate": report.exact_case_pass_rate,
        "failed_cases": [
            {
                "case_id": item.case_id,
                "expected_count": item.expected_count,
                "predicted_count": item.predicted_count,
                "matched_count": item.matched_count,
                "sentiment_matched_count": item.sentiment_matched_count,
                "missing": item.missing,
                "unexpected": item.unexpected,
            }
            for item in report.failed_cases
        ],
    }


def _select_golden_analyzer(mode: str):
    if mode == "rules":
        return ReviewAnalyzer()

    if mode == "hybrid":
        return analyzer

    if mode == "llm":
        return HybridReviewAnalyzer(
            rule_analyzer=ReviewAnalyzer(),
            llm_service=llm_service,
            mode_override="always",
        )

    raise HTTPException(status_code=400, detail="Unsupported evaluation mode.")


@app.get("/evaluation/golden")
def run_golden_evaluation(
    mode: str = Query(default="rules", pattern="^(rules|hybrid|llm)$"),
) -> dict:
    selected_analyzer = _select_golden_analyzer(mode)
    report = GoldenEvaluator(selected_analyzer, repository).run()
    return _golden_report_to_dict(mode, report)


@app.get("/evaluation/golden/compare")
def compare_golden_evaluation_modes(
    include_llm: bool = Query(default=False),
) -> dict:
    modes = ["rules", "hybrid"]

    if include_llm:
        modes.append("llm")

    reports = {}

    for mode in modes:
        selected_analyzer = _select_golden_analyzer(mode)
        report = GoldenEvaluator(selected_analyzer, repository).run()
        reports[mode] = _golden_report_to_dict(mode, report)

    summary = {
        mode: {
            "aspect_recall": item["aspect_recall"],
            "sentiment_accuracy_on_matched": item["sentiment_accuracy_on_matched"],
            "over_extraction_count": item["over_extraction_count"],
            "exact_case_pass_rate": item["exact_case_pass_rate"],
            "failed_case_count": len(item["failed_cases"]),
        }
        for mode, item in reports.items()
    }

    return {
        "modes": modes,
        "summary": summary,
        "reports": reports,
        "note": (
            "rules is deterministic baseline; hybrid is current router behavior; "
            "llm forces Gemini for every case and may consume API quota."
        ),
    }


@app.post("/evaluation/active-queue/build", response_model=ActiveEvaluationQueueBuildResponse)
def build_active_evaluation_queue(
    product_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> ActiveEvaluationQueueBuildResponse:
    result = active_evaluation_service.build_queue(product_id=product_id, limit=limit)
    return ActiveEvaluationQueueBuildResponse(**result)


@app.get("/evaluation/active-queue", response_model=list[ActiveEvaluationQueueItem])
def list_active_evaluation_queue(
    status: str = Query(default="open"),
    product_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[ActiveEvaluationQueueItem]:
    return [
        ActiveEvaluationQueueItem(**item)
        for item in active_evaluation_service.list_queue(
            status=status,
            product_id=product_id,
            limit=limit,
        )
    ]


@app.patch("/evaluation/active-queue/{item_id}", response_model=ActiveEvaluationQueueItem)
def update_active_evaluation_queue_item(
    item_id: int,
    payload: ActiveEvaluationQueueStatusUpdate,
) -> ActiveEvaluationQueueItem:
    item = active_evaluation_service.update_status(item_id=item_id, status=payload.status)

    if item is None:
        raise HTTPException(status_code=404, detail="Active evaluation queue item not found.")

    return ActiveEvaluationQueueItem(**item)


@app.post(
    "/evaluation/active-queue/{item_id}/promote",
    response_model=GoldenReviewCaseItem,
)
def promote_active_queue_item_to_golden(
    item_id: int,
    payload: ActiveEvaluationPromoteRequest,
) -> GoldenReviewCaseItem:
    try:
        item = active_evaluation_service.promote_to_golden_case(
            item_id=item_id,
            expected_aspect=payload.expected_aspect,
            expected_sentiment=payload.expected_sentiment,
            expected=[label.model_dump() for label in payload.expected] if payload.expected else None,
            note=payload.note,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    if item is None:
        raise HTTPException(status_code=404, detail="Active evaluation queue item not found.")

    return GoldenReviewCaseItem(**item)


@app.get("/evaluation/golden/cases", response_model=list[GoldenReviewCaseItem])
def list_db_golden_cases(
    product_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[GoldenReviewCaseItem]:
    return [
        GoldenReviewCaseItem(**item)
        for item in active_evaluation_service.list_golden_cases(
            product_id=product_id,
            limit=limit,
        )
    ]


@app.patch(
    "/evaluation/golden/cases/{case_id}",
    response_model=GoldenReviewCaseItem,
)
def update_db_golden_case(
    case_id: str,
    payload: GoldenReviewCaseUpdateRequest,
) -> GoldenReviewCaseItem:
    try:
        item = active_evaluation_service.update_golden_case(
            case_id=case_id,
            expected=[label.model_dump() for label in payload.expected],
            note=payload.note,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    if item is None:
        raise HTTPException(status_code=404, detail="Golden case not found.")

    return GoldenReviewCaseItem(**item)


@app.get("/news/sources", response_model=list[TrustedNewsSourceItem])
def list_trusted_news_sources() -> list[TrustedNewsSourceItem]:
    return [
        TrustedNewsSourceItem(**item)
        for item in trusted_news_source_service.list_sources()
    ]


@app.post("/news/ingest-rss", response_model=NewsIngestResponse)
def ingest_news_rss(payload: NewsIngestRssRequest) -> NewsIngestResponse:
    try:
        result = news_ingestion_service.ingest_rss_url(
            source_id=payload.source_id,
            rss_url=payload.rss_url,
            discovered_via=payload.discovered_via,
            max_items=payload.max_items,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    return NewsIngestResponse(**result)


@app.post("/news/ingest-rss-xml", response_model=NewsIngestResponse)
def ingest_news_rss_xml(payload: NewsIngestXmlRequest) -> NewsIngestResponse:
    try:
        result = news_ingestion_service.ingest_rss_xml(
            source_id=payload.source_id,
            rss_xml=payload.rss_xml,
            discovered_via=payload.discovered_via,
            max_items=payload.max_items,
        )
    except ValueError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error

    return NewsIngestResponse(**result)


@app.post("/news/rescore", response_model=NewsRescoreResponse)
def rescore_news_items(
    source_id: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
) -> NewsRescoreResponse:
    result = news_ingestion_service.rescore_news_items(
        source_id=source_id,
        limit=limit,
    )
    return NewsRescoreResponse(**result)


@app.get("/news/items", response_model=list[NewsItem])
def list_news_items(
    source_id: str | None = Query(default=None),
    min_relevance_score: float | None = Query(default=None, ge=0, le=100),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[NewsItem]:
    return [
        NewsItem(**item)
        for item in news_ingestion_service.list_news_items(
            source_id=source_id,
            min_relevance_score=min_relevance_score,
            limit=limit,
        )
    ]


@app.get("/visuals/dashboard", response_model=VisualDashboardResponse)
def get_visual_dashboard(
    product_id: str,
    competitor_product_id: str | None = Query(default=None),
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> VisualDashboardResponse:
    result = visualization_service.dashboard(
        product_id=product_id,
        competitor_product_id=competitor_product_id,
        start_date=start_date,
        end_date=end_date,
    )
    return VisualDashboardResponse(**result)


@app.get("/news/brief", response_model=NewsBriefResponse)
def get_news_brief(
    min_relevance_score: float = Query(default=35, ge=0, le=100),
    limit: int = Query(default=10, ge=1, le=50),
) -> NewsBriefResponse:
    result = news_brief_service.build_brief(
        min_relevance_score=min_relevance_score,
        limit=limit,
    )
    return NewsBriefResponse(**result)


@app.get("/reports/executive", response_model=ExecutiveReportResponse)
def get_executive_report(
    product_id: str,
    competitor_product_id: str | None = Query(default=None),
    start_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD start date"),
    end_date: str | None = Query(default=None, description="Inclusive YYYY-MM-DD end date"),
) -> ExecutiveReportResponse:
    result = executive_report_service.build_report(
        product_id=product_id,
        competitor_product_id=competitor_product_id,
        start_date=start_date,
        end_date=end_date,
    )
    return ExecutiveReportResponse(**result)

@app.get("/acquisition/providers")
def list_acquisition_providers() -> dict:
    """List supported/planned review acquisition routes.

    This endpoint is intentionally conservative: live Amazon/Flipkart scraping is
    not placed inside the core API yet. Providers are adapter slots so we can plug
    in CSV, third-party APIs, or controlled marketplace collectors safely.
    """
    return {
        "providers": [
            {
                "provider_id": "manual_csv",
                "label": "Manual CSV / Google Sheet CSV",
                "status": "active",
                "supports_reviews": True,
                "supports_ratings": True,
                "notes": "Use /reviews/import-csv or /reviews/import-csv-url.",
            },
            {
                "provider_id": "amazon_marketplace_adapter",
                "label": "Amazon Marketplace Review Adapter",
                "status": "planned",
                "supports_reviews": True,
                "supports_ratings": True,
                "notes": "Adapter slot for approved third-party API or controlled collector.",
            },
            {
                "provider_id": "flipkart_marketplace_adapter",
                "label": "Flipkart Marketplace Review Adapter",
                "status": "planned",
                "supports_reviews": True,
                "supports_ratings": True,
                "notes": "Adapter slot for approved third-party API or controlled collector.",
            },
            {
                "provider_id": "third_party_review_api",
                "label": "Third-party Review API",
                "status": "planned",
                "supports_reviews": True,
                "supports_ratings": True,
                "notes": "Adapter slot for DataForSEO/SerpApi/Unwrangle/ScraperAPI-style providers.",
            },
        ]
    }


@app.get("/reviews/sources")
def list_review_sources(
    product_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    clauses = []
    params: list[object] = []

    if product_id:
        clauses.append("product_id = ?")
        params.append(product_id)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    with connect(settings.database_path) as connection:
        rows = connection.execute(
            f"""
            SELECT
                id,
                canonical_review_id,
                product_id,
                source_review_key,
                source,
                marketplace,
                marketplace_product_id,
                marketplace_review_id,
                source_url,
                reviewer_hash,
                discovered_via,
                first_seen_at,
                last_seen_at
            FROM review_sources
            {where_sql}
            ORDER BY last_seen_at DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/reviews/duplicates")
def list_review_duplicates(
    product_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    clauses = []
    params: list[object] = []

    if product_id:
        clauses.append("product_id = ?")
        params.append(product_id)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    with connect(settings.database_path) as connection:
        rows = connection.execute(
            f"""
            SELECT
                id,
                incoming_review_id,
                canonical_review_id,
                product_id,
                duplicate_type,
                confidence,
                reason,
                created_at
            FROM review_duplicate_candidates
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

    return [dict(row) for row in rows]


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

