from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.executive_report_service import ExecutiveReportService
from app.services.intelligence_service import IntelligenceService
from app.services.news_brief_service import NewsBriefService
from app.services.news_ingestion_service import NewsIngestionService
from app.services.trusted_news_sources import TrustedNewsSourceService


def test_executive_report_combines_review_competitor_and_news(tmp_path):
    db_path = tmp_path / "test.db"
    repo = ReviewRepository(db_path)
    analyzer = ReviewAnalyzer()

    own_review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-04-01",
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )

    competitor_review = ReviewInput(
        review_id="r2",
        product_id="phone_b",
        product_name="Competitor Phone B",
        source="flipkart",
        rating=4,
        review_date="2026-04-02",
        raw_text="Battery backup mast hai but camera bekar hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(own_review, analyzer.analyze(own_review))
    repo.save_review_analysis(competitor_review, analyzer.analyze(competitor_review))

    repo.save_competitor_mapping(
        product_id="phone_a",
        competitor_product_id="phone_b",
        comparison_group="direct_competitor",
        notes="Test competitor.",
    )

    source_service = TrustedNewsSourceService(db_path)
    news_ingestion_service = NewsIngestionService(db_path, source_service)

    rss_xml = """<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
      <channel>
        <item>
          <title>Qualcomm announces Snapdragon platform with on-device AI for smartphones in India</title>
          <link>https://www.qualcomm.com/news/releases/executive-report-test</link>
          <description>New NPU improves camera AI and Android smartphone performance.</description>
          <pubDate>Wed, 06 May 2026 12:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    news_ingestion_service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=rss_xml,
        discovered_via="test_fixture",
    )

    service = ExecutiveReportService(
        repository=repo,
        intelligence_service=IntelligenceService(repo),
        news_brief_service=NewsBriefService(news_ingestion_service),
    )

    report = service.build_report(
        product_id="phone_a",
        competitor_product_id="phone_b",
    )

    assert report["report_title"] == "IRIP Executive Intelligence Report"
    assert report["product_id"] == "phone_a"
    assert report["competitor_product_id"] == "phone_b"
    assert report["executive_summary"]
    assert report["key_strengths"]
    assert report["key_risks"]
    assert report["competitor_takeaways"]
    assert report["market_news_signals"]
    assert report["recommended_actions"]
    assert report["evidence_links"]