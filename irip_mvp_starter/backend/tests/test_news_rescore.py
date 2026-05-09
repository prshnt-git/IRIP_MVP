from app.db.database import connect, init_db
from app.services.news_ingestion_service import NewsIngestionService
from app.services.trusted_news_sources import TrustedNewsSourceService


def test_news_rescore_updates_existing_old_item(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)

    source_service = TrustedNewsSourceService(db_path)
    service = NewsIngestionService(db_path, source_service)

    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO news_items (
                source_id,
                source_name,
                source_tier,
                title,
                canonical_url,
                published_at,
                summary,
                discovered_via,
                topic_tags_json,
                company_tags_json,
                technology_tags_json,
                region_tags_json,
                relevance_score,
                priority_label,
                why_it_matters,
                evidence_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "qualcomm_newsroom",
                "Qualcomm Newsroom",
                1,
                "Qualcomm announces Snapdragon platform with on-device AI for smartphones in India",
                "https://www.qualcomm.com/news/releases/rescore-test",
                "2026-05-06",
                "New NPU improves camera AI and Android smartphone performance.",
                "test_fixture",
                "[]",
                "[]",
                "[]",
                "[]",
                0,
                "low",
                None,
                "https://www.qualcomm.com/news/releases/rescore-test",
            ),
        )

    result = service.rescore_news_items(source_id="qualcomm_newsroom")

    items = service.list_news_items(source_id="qualcomm_newsroom")

    assert result["updated_count"] == 1
    assert items[0]["priority_label"] == "high"
    assert items[0]["why_it_matters"]
    assert items[0]["relevance_score"] >= 70
    assert "india" in items[0]["region_tags"]