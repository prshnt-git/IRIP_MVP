from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.executive_report_service import ExecutiveReportService
from app.services.intelligence_service import IntelligenceService
from app.services.news_brief_service import NewsBriefService
from app.services.news_ingestion_service import NewsIngestionService
from app.services.system_readiness_service import SystemReadinessService
from app.services.trusted_news_sources import TrustedNewsSourceService
from app.services.visualization_service import VisualizationService


def test_visual_dashboard_returns_echarts_ready_blocks(tmp_path):
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
          <link>https://www.qualcomm.com/news/releases/visual-dashboard-test</link>
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

    news_brief_service = NewsBriefService(news_ingestion_service)
    intelligence_service = IntelligenceService(repo)
    executive_report_service = ExecutiveReportService(
        repository=repo,
        intelligence_service=intelligence_service,
        news_brief_service=news_brief_service,
    )
    system_readiness_service = SystemReadinessService(
        repository=repo,
        news_ingestion_service=news_ingestion_service,
    )

    service = VisualizationService(
        repository=repo,
        executive_report_service=executive_report_service,
        news_brief_service=news_brief_service,
        system_readiness_service=system_readiness_service,
    )

    result = service.dashboard(
        product_id="phone_a",
        competitor_product_id="phone_b",
    )

    assert result["product_id"] == "phone_a"
    assert result["competitor_product_id"] == "phone_b"
    assert result["workflow_tiles"]
    assert result["kpi_cards"]
    assert result["sentiment_distribution_chart"]["chart_type"] == "echarts_donut"
    assert result["top_aspect_chart"]["chart_type"] == "echarts_horizontal_bar"
    assert result["competitor_gap_chart"]["chart_type"] == "echarts_diverging_bar"
    assert result["news_signal_chart"]["chart_type"] == "echarts_horizontal_bar"
    assert result["source_tier_chart"]["chart_type"] == "echarts_donut"
    assert result["quality_cards"]
    assert result["news_signal_chips"]
    assert result["recommended_actions"]
    assert result["evidence_links"]