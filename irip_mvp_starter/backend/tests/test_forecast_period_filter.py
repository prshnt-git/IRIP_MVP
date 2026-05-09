from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.intelligence_service import IntelligenceService


def test_forecast_respects_date_period(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    old_review = ReviewInput(
        review_id="old",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=2,
        review_date="2026-03-01",
        raw_text="battery bekar hai",
        verified_purchase=True,
    )
    repo.save_review_analysis(old_review, analyzer.analyze(old_review))

    new_review = ReviewInput(
        review_id="new",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=5,
        review_date="2026-04-01",
        raw_text="camera mast hai",
        verified_purchase=True,
    )
    repo.save_review_analysis(new_review, analyzer.analyze(new_review))

    service = IntelligenceService(repo)

    filtered = service.get_product_forecast(
        product_id="phone_a",
        start_date="2026-04-01",
        end_date="2026-04-01",
    )

    aspects = {item.aspect for item in filtered.aspects}

    assert "camera" in aspects
    assert "battery" not in aspects