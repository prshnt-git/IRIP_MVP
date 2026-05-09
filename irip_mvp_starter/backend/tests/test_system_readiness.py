from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.news_ingestion_service import NewsIngestionService
from app.services.system_readiness_service import SystemReadinessService
from app.services.trusted_news_sources import TrustedNewsSourceService


def test_system_version_response_is_stable(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    source_service = TrustedNewsSourceService(repo.database_path)
    news_service = NewsIngestionService(repo.database_path, source_service)

    service = SystemReadinessService(
        repository=repo,
        news_ingestion_service=news_service,
    )

    result = service.version(api_version="0.1.0")

    assert result["product_version"] == "v1.0"
    assert result["product_name"]
    assert "Executive intelligence report" in result["locked_capabilities"]


def test_system_readiness_reports_limitations_for_low_volume_data(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-04-01",
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(review, analyzer.analyze(review))

    source_service = TrustedNewsSourceService(repo.database_path)
    news_service = NewsIngestionService(repo.database_path, source_service)

    rss_xml = """<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
      <channel>
        <item>
          <title>Qualcomm announces Snapdragon platform with on-device AI for smartphones in India</title>
          <link>https://www.qualcomm.com/news/releases/readiness-test</link>
          <description>New NPU improves camera AI and Android smartphone performance.</description>
          <pubDate>Wed, 06 May 2026 12:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    news_service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=rss_xml,
        discovered_via="test_fixture",
    )

    service = SystemReadinessService(
        repository=repo,
        news_ingestion_service=news_service,
    )

    result = service.readiness(product_version="v1.0")

    assert result["readiness_status"] in {"ready_with_limitations", "ready"}
    assert result["checks"]
    assert result["warnings"]