from __future__ import annotations

from app.db.repository import ReviewRepository
from app.services.news_ingestion_service import NewsIngestionService


class SystemReadinessService:
    def __init__(
        self,
        repository: ReviewRepository,
        news_ingestion_service: NewsIngestionService,
    ) -> None:
        self.repository = repository
        self.news_ingestion_service = news_ingestion_service

    def version(self, api_version: str) -> dict:
        return {
            "product_name": "IRIP — India Review Intelligence Platform",
            "product_version": "v1.0",
            "api_version": api_version,
            "stage": "team-ready internal MVP checkpoint",
            "description": (
                "A quality-first smartphone market intelligence backend combining "
                "review intelligence, aspect sentiment, competitor benchmarking, "
                "trusted news signals, golden evaluation, and executive reporting."
            ),
            "locked_capabilities": [
                "Review import and validation",
                "Aspect-level sentiment extraction",
                "Evidence-backed review intelligence",
                "Competitor benchmarking",
                "Golden benchmark evaluation loop",
                "Provider comparison: rules, hybrid, LLM",
                "Trusted news ingestion",
                "MICI-grade news relevance scoring",
                "News intelligence brief",
                "Executive intelligence report",
            ],
            "next_recommended_focus": [
                "Increase real review volume",
                "Expand golden benchmark with real validated cases",
                "Improve confidence calibration using larger datasets",
                "Polish executive report narrative without weakening evidence discipline",
                "Prepare frontend workspace around existing backend capabilities",
            ],
        }

    def readiness(self, product_version: str = "v1.0") -> dict:
        stats = self.repository.get_database_stats()
        products = self.repository.list_products()
        news_items = self.news_ingestion_service.list_news_items(limit=10)

        checks = []
        warnings = []

        checks.append(
            self._check(
                name="database",
                passed=bool(stats),
                passed_detail="Database is reachable and stats endpoint can read core tables.",
                failed_detail="Database stats could not be read.",
            )
        )

        checks.append(
            self._check(
                name="product_data",
                passed=stats.get("products", 0) > 0 and stats.get("reviews_raw", 0) > 0,
                passed_detail=f"Found {stats.get('products', 0)} product(s) and {stats.get('reviews_raw', 0)} raw review(s).",
                failed_detail="No product/review data found. Import review data before using reports.",
            )
        )

        checks.append(
            self._check(
                name="processed_reviews",
                passed=stats.get("reviews_processed", 0) > 0 and stats.get("aspect_sentiments", 0) > 0,
                passed_detail=f"Found {stats.get('reviews_processed', 0)} processed review(s) and {stats.get('aspect_sentiments', 0)} aspect sentiment row(s).",
                failed_detail="No processed review/aspect sentiment data found.",
            )
        )

        checks.append(
            self._check(
                name="living_lexicon",
                passed=stats.get("living_lexicon", 0) > 0,
                passed_detail=f"Living lexicon has {stats.get('living_lexicon', 0)} entries.",
                failed_detail="Living lexicon is empty.",
            )
        )

        checks.append(
            self._check(
                name="trusted_news",
                passed=len(news_items) > 0,
                passed_detail=f"Found {len(news_items)} trusted news item(s) available for intelligence brief.",
                failed_detail="No trusted news items found.",
            )
        )

        low_volume_products = [
            item for item in products if item.get("review_count", 0) < 30
        ]

        if low_volume_products:
            warnings.append(
                "Some products have fewer than 30 reviews. Treat sentiment, benchmark, and forecast outputs as directional."
            )

        if stats.get("reviews_raw", 0) < 100:
            warnings.append(
                "Overall review volume is still low for production-grade sentiment confidence. Expand real imported data."
            )

        if len(news_items) < 10:
            warnings.append(
                "Trusted news coverage is still thin. Expand curated sources before relying on market-level conclusions."
            )

        failed_checks = [item for item in checks if item["status"] == "fail"]
        warning_checks = [item for item in checks if item["status"] == "warn"]

        if failed_checks:
            readiness_status = "not_ready"
        elif warning_checks or warnings:
            readiness_status = "ready_with_limitations"
        else:
            readiness_status = "ready"

        return {
            "product_version": product_version,
            "readiness_status": readiness_status,
            "checks": checks,
            "warnings": warnings,
            "recommended_next_actions": [
                "Import a larger real review dataset before leadership use.",
                "Promote uncertain/high-impact extractions into the golden benchmark.",
                "Keep reports evidence-linked and avoid unsupported conclusions.",
                "Use frontend to simplify workflows, not to hide confidence warnings.",
            ],
        }

    def _check(
        self,
        name: str,
        passed: bool,
        passed_detail: str,
        failed_detail: str,
    ) -> dict:
        return {
            "name": name,
            "status": "pass" if passed else "fail",
            "detail": passed_detail if passed else failed_detail,
        }