from app.db.database import init_db
from app.services.news_brief_service import NewsBriefService
from app.services.news_ingestion_service import NewsIngestionService
from app.services.trusted_news_sources import TrustedNewsSourceService


RSS_FIXTURE = """<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
  <channel>
    <item>
      <title>Qualcomm announces Snapdragon platform with on-device AI for smartphones in India</title>
      <link>https://www.qualcomm.com/news/releases/brief-test-snapdragon-ai-india</link>
      <description>New NPU improves camera AI, Android smartphone performance, and edge AI use cases.</description>
      <pubDate>Wed, 06 May 2026 12:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


def test_news_brief_generates_evidence_linked_summary(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    ingestion_service = NewsIngestionService(db_path, source_service)

    ingestion_service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=RSS_FIXTURE,
        discovered_via="test_fixture",
    )

    brief_service = NewsBriefService(ingestion_service)
    brief = brief_service.build_brief(min_relevance_score=10, limit=10)

    assert brief["total_items_considered"] == 1
    assert brief["high_priority_count"] == 1
    assert brief["top_items"][0]["source_tier"] == 1
    assert brief["top_items"][0]["evidence_url"].startswith("https://www.qualcomm.com/")
    assert brief["executive_summary"]
    assert brief["recommended_actions"]
    assert brief["evidence_links"]


def test_news_brief_empty_state_is_safe(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    ingestion_service = NewsIngestionService(db_path, source_service)
    brief_service = NewsBriefService(ingestion_service)

    brief = brief_service.build_brief(min_relevance_score=90, limit=10)

    assert brief["total_items_considered"] == 0
    assert brief["top_items"] == []
    assert brief["executive_summary"]
    assert brief["recommended_actions"]