from app.db.database import init_db
from app.services.news_ingestion_service import NewsIngestionService
from app.services.trusted_news_sources import TrustedNewsSourceService


def _service(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    source_service = TrustedNewsSourceService(db_path)
    return NewsIngestionService(db_path, source_service)


def test_high_priority_ai_smartphone_chipset_news(tmp_path):
    service = _service(tmp_path)

    tags = service._classify_item(
        title="Qualcomm announces Snapdragon platform with on-device AI for smartphones",
        summary="New NPU improves camera AI and generative AI performance on Android devices.",
        default_tags=["chipset", "edge_ai", "smartphone"],
    )

    assert tags["priority_label"] == "high"
    assert tags["relevance_score"] >= 70
    assert "ai" in tags["topic_tags"]
    assert "smartphone" in tags["topic_tags"]
    assert "qualcomm" in tags["company_tags"]
    assert "snapdragon" in tags["technology_tags"]
    assert tags["why_it_matters"]


def test_medium_or_high_priority_emerging_market_oem_news(tmp_path):
    service = _service(tmp_path)

    tags = service._classify_item(
        title="Smartphone shipments grow in India and Africa",
        summary="OEMs including Samsung and Transsion expand in emerging markets.",
        default_tags=["market", "technology"],
    )

    assert tags["priority_label"] in {"medium", "high"}
    assert "india" in tags["region_tags"]
    assert "africa" in tags["region_tags"]
    assert "samsung" in tags["company_tags"]
    assert "transsion" in tags["company_tags"]
    assert tags["why_it_matters"]


def test_low_priority_unrelated_news(tmp_path):
    service = _service(tmp_path)

    tags = service._classify_item(
        title="Corporate office event announced",
        summary="Company announces internal office celebration.",
        default_tags=[],
    )

    assert tags["priority_label"] == "low"
    assert tags["relevance_score"] < 20
    assert "Low immediate MICI relevance" in tags["why_it_matters"]


def test_ingested_news_item_contains_priority_and_why_it_matters(tmp_path):
    service = _service(tmp_path)

    rss_xml = """<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
      <channel>
        <item>
          <title>Qualcomm announces Snapdragon platform with on-device AI for smartphones</title>
          <link>https://www.qualcomm.com/news/releases/snapdragon-ai-phone</link>
          <description>New NPU improves camera AI and Android smartphone performance.</description>
          <pubDate>Wed, 06 May 2026 10:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>
    """

    result = service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=rss_xml,
        discovered_via="test_fixture",
    )

    assert result["inserted_count"] == 1
    item = result["items"][0]
    assert item["priority_label"] == "high"
    assert item["why_it_matters"]