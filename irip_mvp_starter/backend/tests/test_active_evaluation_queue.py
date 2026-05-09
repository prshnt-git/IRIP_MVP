from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.active_evaluation_service import ActiveEvaluationService


def test_active_evaluation_queue_builds_from_negative_review(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=5,
        review_date="2026-04-01",
        raw_text="Battery backup bakwas hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(review, analyzer.analyze(review))

    service = ActiveEvaluationService(repo)
    result = service.build_queue(product_id="phone_a")

    queue = service.list_queue(product_id="phone_a")

    assert result["open_count"] >= 1
    assert len(queue) >= 1
    assert any(item["reason"] for item in queue)


def test_active_evaluation_queue_is_duplicate_safe(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=5,
        review_date="2026-04-01",
        raw_text="Battery backup bakwas hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(review, analyzer.analyze(review))

    service = ActiveEvaluationService(repo)
    first = service.build_queue(product_id="phone_a")
    second = service.build_queue(product_id="phone_a")

    assert first["open_count"] >= 1
    assert second["inserted_count"] == 0


def test_active_evaluation_queue_status_update(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=5,
        review_date="2026-04-01",
        raw_text="Battery backup bakwas hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(review, analyzer.analyze(review))

    service = ActiveEvaluationService(repo)
    service.build_queue(product_id="phone_a")

    queue = service.list_queue(product_id="phone_a")
    updated = service.update_status(queue[0]["id"], "reviewed")

    assert updated is not None
    assert updated["status"] == "reviewed"