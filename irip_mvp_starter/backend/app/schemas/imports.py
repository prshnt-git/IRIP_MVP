from __future__ import annotations

from pydantic import BaseModel, Field


class ImportValidationWarning(BaseModel):
    row_number: int
    reason: str
    value: str | None = None


class ImportPreviewResponse(BaseModel):
    valid_count: int
    failed_count: int
    warning_count: int
    required_columns_present: bool
    detected_columns: list[str]
    errors: list[dict]
    warnings: list[ImportValidationWarning]
    sample_valid_rows: list[dict]


class ImportErrorItem(BaseModel):
    row_number: int
    reason: str


class ImportResult(BaseModel):
    imported_count: int
    failed_count: int
    errors: list[ImportErrorItem] = []
    product_ids: list[str] = []
    skipped_duplicate_count: int = 0
    attached_source_count: int = 0
    possible_duplicate_count: int = 0


class ProductSummary(BaseModel):
    product_id: str
    period: dict
    review_count: int
    average_rating: float | None
    average_quality_score: float | None
    net_sentiment_score: float
    sentiment_counts: dict[str, int]
    contradiction_count: int
    sarcasm_count: int
    top_aspects: list[dict]


class AspectSummaryItem(BaseModel):
    aspect: str
    mentions: int
    positive_count: int
    negative_count: int
    neutral_count: int
    avg_confidence: float | None
    aspect_score: float = Field(ge=-100, le=100)
    sub_aspects: dict[str, float] | None = None


class CsvUrlImportRequest(BaseModel):
    url: str = Field(min_length=8, description="Public CSV URL, such as a Google Sheets published CSV link")


class DatabaseStats(BaseModel):
    products: int
    reviews_raw: int
    reviews_processed: int
    aspect_sentiments: int
    living_lexicon: int
    evaluation_runs: int
    competitor_mappings: int = 0
    extraction_feedback: int = 0
    review_sources: int = 0
    review_duplicate_candidates: int = 0


class ProductCatalogImportResult(BaseModel):
    imported_products: int
    imported_mappings: int
    failed_count: int
    errors: list[ImportErrorItem] = []
    product_ids: list[str] = []
    own_brand_count: int = 0
    competitor_brand_count: int = 0


class ProductCatalogUrlImportRequest(BaseModel):
    url: str = Field(min_length=8, description="Public product catalog CSV URL")


class CompetitorItem(BaseModel):
    product_id: str
    product_name: str | None = None
    brand: str | None = None
    price_band: str | None = None
    comparison_group: str | None = None
    notes: str | None = None
    own_brand: bool | None = None
    marketplace: str | None = None
    marketplace_product_url: str | None = None


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


class ReviewSourceItem(BaseModel):
    id: int
    canonical_review_id: str
    source_review_key: str
    product_id: str
    marketplace: str | None = None
    source: str | None = None
    source_url: str | None = None
    marketplace_product_id: str | None = None
    marketplace_review_id: str | None = None
    reviewer_hash: str | None = None
    first_seen_at: str | None = None
    last_seen_at: str | None = None


class AcquisitionProviderItem(BaseModel):
    provider_id: str
    provider_name: str
    source_type: str
    configured: bool
    status: str
    notes: str


class MarketplaceReviewImportRequest(BaseModel):
    product_id: str = Field(min_length=1)
    marketplace: str = Field(min_length=2, description="amazon, flipkart, or a configured provider name")
    product_url: str = Field(min_length=8)
    provider: str | None = Field(default=None, description="Optional third-party provider adapter id")
    max_reviews: int = Field(default=50, ge=1, le=500)
