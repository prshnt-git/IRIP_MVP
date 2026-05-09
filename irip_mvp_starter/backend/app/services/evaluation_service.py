from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.evaluation import EvaluationCase, EvaluationResult
from app.schemas.review import ReviewInput


class EvaluationService:
    def __init__(self, analyzer: ReviewAnalyzer | None = None) -> None:
        self.analyzer = analyzer or ReviewAnalyzer()

    def evaluate(self, cases: list[EvaluationCase], provider_id: str = "aspect_rules_v1") -> EvaluationResult:
        exact_aspect_hits = 0
        sentiment_hits = 0
        failed_case_ids: list[str] = []

        for case in cases:
            review = ReviewInput(product_id="eval_product", raw_text=case.text)
            analysis = self.analyzer.analyze(review)
            predicted = {(item.aspect, item.sentiment) for item in analysis.aspect_sentiments}
            expected = {(item.aspect, item.sentiment) for item in case.expected_aspects}
            predicted_aspects = {item.aspect for item in analysis.aspect_sentiments}
            expected_aspects = {item.aspect for item in case.expected_aspects}

            if expected_aspects.issubset(predicted_aspects):
                exact_aspect_hits += 1
            if expected.issubset(predicted):
                sentiment_hits += 1
            else:
                failed_case_ids.append(case.case_id)

        total = max(1, len(cases))
        return EvaluationResult(
            provider_id=provider_id,
            total_cases=len(cases),
            exact_aspect_hits=exact_aspect_hits,
            sentiment_hits=sentiment_hits,
            aspect_precision_proxy=round(exact_aspect_hits / total, 3),
            sentiment_accuracy_proxy=round(sentiment_hits / total, 3),
            failed_case_ids=failed_case_ids,
        )
