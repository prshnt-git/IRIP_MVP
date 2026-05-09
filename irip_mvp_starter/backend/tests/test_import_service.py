from pathlib import Path

from app.db.repository import ReviewRepository
from app.services.import_service import ReviewImportService


def test_csv_import_persists_raw_processed_and_aspect_summaries(tmp_path: Path):
    repository = ReviewRepository(tmp_path / "irip_test.db")
    service = ReviewImportService(repository=repository)
    csv_text = """review_id,product_id,product_name,source,rating,review_date,raw_text,verified_purchase,helpful_votes
r1,phone_a,Demo Phone,flipkart,3,2026-05-01,Bhai camera mast hai but battery backup bekar hai,true,4
r2,phone_a,Demo Phone,amazon,5,2026-05-02,Display good hai but delivery late thi,false,0
"""

    result = service.import_csv_text(csv_text)

    assert result.imported_count == 2
    assert result.failed_count == 0
    assert result.product_ids == ["phone_a"]

    products = repository.list_products()
    assert products[0]["product_id"] == "phone_a"
    assert products[0]["review_count"] == 2

    summary = repository.get_product_summary("phone_a")
    assert summary["review_count"] == 2
    assert summary["average_rating"] == 4.0
    assert summary["net_sentiment_score"] < 100

    aspects = {item["aspect"]: item for item in repository.get_aspect_summary("phone_a")}
    assert aspects["camera"]["positive_count"] == 1
    assert aspects["battery"]["negative_count"] == 1
    assert aspects["display"]["positive_count"] == 1

    evidence = repository.list_evidence("phone_a", aspect="battery")
    assert len(evidence) == 1
    assert evidence[0]["sentiment"] == "negative"


def test_csv_import_reports_missing_required_columns(tmp_path: Path):
    repository = ReviewRepository(tmp_path / "irip_test.db")
    service = ReviewImportService(repository=repository)

    result = service.import_csv_text("review_id,raw_text\nr1,camera mast hai\n")

    assert result.imported_count == 0
    assert result.failed_count == 1
    assert "Missing required column" in result.errors[0].reason
