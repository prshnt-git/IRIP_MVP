from app.db.repository import ReviewRepository
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput
from app.services.active_evaluation_service import ActiveEvaluationService
from app.services.golden_evaluator import GoldenEvaluator


def test_promote_active_queue_item_to_golden_case(tmp_path):
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

    promoted = service.promote_to_golden_case(
        item_id=queue[0]["id"],
        expected_aspect="battery",
        expected_sentiment="negative",
        note="Validated from active queue test.",
    )

    golden_cases = service.list_golden_cases(product_id="phone_a")
    updated_queue = service.list_queue(status="promoted", product_id="phone_a")

    assert promoted is not None
    assert promoted["expected"] == [{"aspect": "battery", "sentiment": "negative"}]
    assert len(golden_cases) == 1
    assert len(updated_queue) == 1
    assert updated_queue[0]["status"] == "promoted"


def test_golden_evaluator_includes_promoted_db_cases(tmp_path):
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

    service.promote_to_golden_case(
        item_id=queue[0]["id"],
        expected_aspect="battery",
        expected_sentiment="negative",
        note="Validated from active queue test.",
    )

    report = GoldenEvaluator(analyzer, repo).run()

    assert report.total_cases > 12
    assert report.total_expected > 15

def test_promote_active_queue_item_to_multi_label_golden_case(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-04-01",
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(review, analyzer.analyze(review))

    service = ActiveEvaluationService(repo)
    service.build_queue(product_id="phone_a")
    queue = service.list_queue(product_id="phone_a")

    promoted = service.promote_to_golden_case(
        item_id=queue[0]["id"],
        expected=[
            {"aspect": "camera", "sentiment": "positive"},
            {"aspect": "battery", "sentiment": "negative"},
        ],
        note="Validated full multi-aspect review.",
    )

    assert promoted is not None
    assert promoted["expected"] == [
        {"aspect": "camera", "sentiment": "positive"},
        {"aspect": "battery", "sentiment": "negative"},
    ]

    report = GoldenEvaluator(analyzer, repo).run()

    promoted_case = [
        item for item in report.failed_cases if item.case_id == promoted["case_id"]
    ]

    assert promoted_case == []

def test_update_existing_golden_case_labels(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")
    analyzer = ReviewAnalyzer()

    review = ReviewInput(
        review_id="r1",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=3,
        review_date="2026-04-01",
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )

    repo.save_review_analysis(review, analyzer.analyze(review))

    service = ActiveEvaluationService(repo)
    service.build_queue(product_id="phone_a")
    queue = service.list_queue(product_id="phone_a")

    promoted = service.promote_to_golden_case(
        item_id=queue[0]["id"],
        expected_aspect="battery",
        expected_sentiment="negative",
        note="Initial single-label case.",
    )

    assert promoted is not None

    updated = service.update_golden_case(
        case_id=promoted["case_id"],
        expected=[
            {"aspect": "camera", "sentiment": "positive"},
            {"aspect": "battery", "sentiment": "negative"},
        ],
        note="Updated to full multi-aspect label.",
    )

    assert updated is not None
    assert updated["expected"] == [
        {"aspect": "camera", "sentiment": "positive"},
        {"aspect": "battery", "sentiment": "negative"},
    ]

    report = GoldenEvaluator(analyzer, repo).run()
    promoted_case = [
        item for item in report.failed_cases if item.case_id == promoted["case_id"]
    ]

    assert promoted_case == []