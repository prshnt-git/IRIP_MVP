from app.db.database import init_db
from app.services.news_ingestion_service import NewsIngestionService
from app.services.trusted_news_sources import TrustedNewsSourceService


RSS_FIXTURE = """<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">
  <channel>
    <title>Qualcomm Newsroom</title>
    <item>
      <title>Qualcomm announces new Snapdragon platform with on-device AI for smartphones</title>
      <link>https://www.qualcomm.com/news/releases/new-snapdragon-ai-smartphone</link>
      <description>New chipset improves smartphone AI, camera, and NPU performance.</description>
      <pubDate>Wed, 06 May 2026 10:00:00 GMT</pubDate>
    </item>
    <item>
      <title>Unrelated corporate office event</title>
      <link>https://www.qualcomm.com/news/releases/office-event</link>
      <description>Company event update.</description>
      <pubDate>Wed, 06 May 2026 11:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""


def test_trusted_news_sources_seeded(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    sources = source_service.list_sources()

    assert any(item["source_id"] == "qualcomm_newsroom" for item in sources)
    assert all(item["source_tier"] in {1, 2} for item in sources)


def test_news_ingestion_from_rss_fixture(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    service = NewsIngestionService(db_path, source_service)

    result = service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=RSS_FIXTURE,
        discovered_via="test_fixture",
    )

    assert result["inserted_count"] == 2
    assert result["skipped_duplicate_count"] == 0

    items = service.list_news_items(source_id="qualcomm_newsroom")
    assert len(items) == 2
    assert items[0]["source_tier"] == 1
    assert items[0]["evidence_url"].startswith("https://www.qualcomm.com/")


def test_news_ingestion_is_duplicate_safe(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    service = NewsIngestionService(db_path, source_service)

    first = service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=RSS_FIXTURE,
        discovered_via="test_fixture",
    )
    second = service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=RSS_FIXTURE,
        discovered_via="test_fixture",
    )

    assert first["inserted_count"] == 2
    assert second["inserted_count"] == 0
    assert second["skipped_duplicate_count"] == 2


def test_news_ingestion_rejects_untrusted_item_url(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    service = NewsIngestionService(db_path, source_service)

    bad_rss = """<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
      <channel>
        <item>
          <title>Fake Qualcomm item</title>
          <link>https://example.com/fake</link>
          <description>Fake item.</description>
        </item>
      </channel>
    </rss>
    """

    result = service.ingest_rss_xml(
        source_id="qualcomm_newsroom",
        rss_xml=bad_rss,
        discovered_via="test_fixture",
    )

    assert result["inserted_count"] == 0
    assert result["rejected_count"] == 1