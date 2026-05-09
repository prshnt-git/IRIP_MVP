from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput


def test_database_stats_and_reset(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()
    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-05-01",
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )
    repo.save_review_analysis(review, analyzer.analyze(review))

    stats = repo.get_database_stats()
    assert stats["products"] == 1
    assert stats["reviews_raw"] == 1
    assert stats["aspect_sentiments"] >= 2
    assert stats["living_lexicon"] > 0

    reset_stats = repo.reset_review_data()
    assert reset_stats["products"] == 0
    assert reset_stats["reviews_raw"] == 0
    assert reset_stats["aspect_sentiments"] == 0
    assert reset_stats["living_lexicon"] > 0
