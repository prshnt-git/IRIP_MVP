from app.pipeline.review_analyzer import ReviewAnalyzer
from app.services.golden_evaluator import GoldenEvaluator


def test_golden_evaluation_report_has_expected_shape():
    evaluator = GoldenEvaluator(ReviewAnalyzer())

    report = evaluator.run()

    assert report.total_cases >= 10
    assert report.total_expected >= 10
    assert 0 <= report.aspect_recall <= 1
    assert 0 <= report.sentiment_accuracy_on_matched <= 1
    assert report.total_predicted >= 0


def test_golden_evaluation_rules_baseline_minimum_quality():
    evaluator = GoldenEvaluator(ReviewAnalyzer())

    report = evaluator.run()

    # Baseline rules do not need to be perfect yet, but they must not collapse.
    assert report.aspect_recall >= 0.45
    assert report.sentiment_accuracy_on_matched >= 0.65