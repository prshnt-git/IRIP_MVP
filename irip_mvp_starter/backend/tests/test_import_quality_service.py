from app.services.import_quality_service import ImportQualityService


def test_import_preview_accepts_valid_minimum_csv():
    service = ImportQualityService()

    csv_text = """review_id,product_id,raw_text,rating,review_date
r1,Phone A,Camera mast hai,5,2026-04-01
"""

    result = service.preview_csv_text(csv_text)

    assert result["required_columns_present"] is True
    assert result["valid_count"] == 1
    assert result["failed_count"] == 0
    assert result["sample_valid_rows"][0]["product_id"] == "phone_a"


def test_import_preview_rejects_missing_required_columns():
    service = ImportQualityService()

    csv_text = """review_id,product_name,text
r1,Phone A,Camera mast hai
"""

    result = service.preview_csv_text(csv_text)

    assert result["required_columns_present"] is False
    assert result["failed_count"] == 1
    assert "Missing required column" in result["errors"][0]["reason"]


def test_import_preview_rejects_bad_rating_and_date():
    service = ImportQualityService()

    csv_text = """review_id,product_id,raw_text,rating,review_date
r1,phone_a,Camera mast hai,9,2026-99-99
"""

    result = service.preview_csv_text(csv_text)

    assert result["valid_count"] == 0
    assert result["failed_count"] >= 2
    reasons = " ".join(error["reason"] for error in result["errors"])
    assert "rating" in reasons
    assert "review_date" in reasons


def test_import_preview_warns_duplicate_review_id():
    service = ImportQualityService()

    csv_text = """review_id,product_id,raw_text,rating,review_date
r1,phone_a,Camera mast hai,5,2026-04-01
r1,phone_a,Battery bekar hai,2,2026-04-02
"""

    result = service.preview_csv_text(csv_text)

    assert result["valid_count"] == 2
    assert any("Duplicate review_id" in warning["reason"] for warning in result["warnings"])