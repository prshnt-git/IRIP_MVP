from app.pipeline.hybrid_analyzer import HybridReviewAnalyzer
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.services.golden_evaluator import GoldenEvaluator


class FakeDisabledLlmService:
    def status(self):
        return {
            "provider": "gemini",
            "enabled": False,
            "model": "fake-model",
            "reason": "disabled in test",
        }


def test_golden_evaluator_exact_case_metrics_exist():
    report = GoldenEvaluator(ReviewAnalyzer()).run()

    assert 0 <= report.exact_case_pass_rate <= 1
    assert report.exact_case_pass_count <= report.total_cases


def test_hybrid_analyzer_accepts_mode_override():
    analyzer = HybridReviewAnalyzer(
        rule_analyzer=ReviewAnalyzer(),
        llm_service=FakeDisabledLlmService(),
        mode_override="off",
    )

    assert analyzer.mode_override == "off"