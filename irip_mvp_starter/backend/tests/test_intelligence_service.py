from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.intelligence_service import IntelligenceService


def test_product_themes_forecast_and_brief(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()
    service = IntelligenceService(repo)

    reviews = [
        ReviewInput(
            review_id="r1",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=3,
            review_date="2026-04-01",
            raw_text="Bhai camera mast hai but battery backup bekar hai",
            verified_purchase=True,
        ),
        ReviewInput(
            review_id="r2",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=5,
            review_date="2026-05-01",
            raw_text="Camera zabardast hai display bhi mast hai",
            verified_purchase=True,
        ),
        ReviewInput(
            review_id="r3",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=2,
            review_date="2026-05-03",
            raw_text="Battery backup bakwas hai aur phone garam hota hai",
            verified_purchase=True,
        ),
    ]

    for review in reviews:
        repo.save_review_analysis(review, analyzer.analyze(review))

    themes = service.get_product_themes("phone_a")
    assert themes.product_id == "phone_a"
    assert len(themes.complaint_themes) >= 1
    assert any(theme.aspect == "battery" for theme in themes.complaint_themes)
    assert len(themes.delight_themes) >= 1

    forecast = service.get_product_forecast("phone_a")
    assert forecast.product_id == "phone_a"
    assert " vs " in forecast.forecast_window
    assert "2026-04-01 to 2026-05-03" in forecast.forecast_window
    assert len(forecast.aspects) >= 1

    brief = service.get_intelligence_brief("phone_a")
    assert brief.product_id == "phone_a"
    assert "phone_a currently has" in brief.executive_summary
    assert len(brief.recommended_actions) >= 1