from __future__ import annotations

import json
from dataclasses import dataclass

from app.db.database import connect
from app.db.repository import ReviewRepository
from app.evaluation.golden_reviews import GOLDEN_REVIEWS
from app.pipeline.hybrid_analyzer import HybridReviewAnalyzer
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput


@dataclass(frozen=True)
class GoldenCaseResult:
    case_id: str
    expected_count: int
    predicted_count: int
    matched_count: int
    sentiment_matched_count: int
    missing: list[dict]
    unexpected: list[dict]


@dataclass(frozen=True)
class GoldenEvaluationReport:
    total_cases: int
    total_expected: int
    total_predicted: int
    total_matched: int
    total_sentiment_matched: int
    aspect_recall: float
    sentiment_accuracy_on_matched: float
    over_extraction_count: int
    exact_case_pass_count: int
    exact_case_pass_rate: float
    failed_cases: list[GoldenCaseResult]

class GoldenEvaluator:
    def __init__(
        self,
        analyzer: ReviewAnalyzer | HybridReviewAnalyzer,
        repository: ReviewRepository | None = None,
    ) -> None:
        self.analyzer = analyzer
        self.repository = repository

    def run(self) -> GoldenEvaluationReport:
        cases = self._all_cases()
        case_results: list[GoldenCaseResult] = []

        total_expected = 0
        total_predicted = 0
        total_matched = 0
        total_sentiment_matched = 0
        over_extraction_count = 0

        for case in cases:
            result = self._evaluate_case(case)
            case_results.append(result)

            total_expected += result.expected_count
            total_predicted += result.predicted_count
            total_matched += result.matched_count
            total_sentiment_matched += result.sentiment_matched_count
            over_extraction_count += len(result.unexpected)

            failed_cases = [
                item
                for item in case_results
                if item.missing or item.unexpected or item.sentiment_matched_count < item.matched_count
            ]

            exact_case_pass_count = len(cases) - len(failed_cases)
            exact_case_pass_rate = round(exact_case_pass_count / len(cases), 3) if cases else 1.0

            aspect_recall = round(total_matched / total_expected, 3) if total_expected else 1.0
            sentiment_accuracy = (
            round(total_sentiment_matched / total_matched, 3) if total_matched else 1.0
        )

        return GoldenEvaluationReport(
            total_cases=len(cases),
            total_expected=total_expected,
            total_predicted=total_predicted,
            total_matched=total_matched,
            total_sentiment_matched=total_sentiment_matched,
            aspect_recall=aspect_recall,
            sentiment_accuracy_on_matched=sentiment_accuracy,
            over_extraction_count=over_extraction_count,
            exact_case_pass_count=exact_case_pass_count,
            exact_case_pass_rate=exact_case_pass_rate,
            failed_cases=failed_cases,
        )
    def _all_cases(self) -> list[dict]:
        return [*GOLDEN_REVIEWS, *self._db_golden_cases()]

    def _db_golden_cases(self) -> list[dict]:
        if self.repository is None:
            return []

        with connect(self.repository.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    case_id,
                    product_id,
                    product_name,
                    source,
                    rating,
                    review_date,
                    raw_text,
                    expected_json
                FROM golden_review_cases
                WHERE approved_by_human = 1
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()

        cases: list[dict] = []

        for row in rows:
            try:
                expected = json.loads(row["expected_json"] or "[]")
            except json.JSONDecodeError:
                expected = []

            cases.append(
                {
                    "case_id": row["case_id"],
                    "product_id": row["product_id"],
                    "product_name": row["product_name"],
                    "source": row["source"],
                    "rating": row["rating"],
                    "review_date": row["review_date"],
                    "raw_text": row["raw_text"],
                    "expected": expected,
                }
            )

        return cases

    def _evaluate_case(self, case: dict) -> GoldenCaseResult:
        review = ReviewInput(
            review_id=case["case_id"],
            product_id=case["product_id"],
            product_name=case["product_name"],
            source=case["source"],
            rating=case["rating"],
            review_date=case["review_date"],
            raw_text=case["raw_text"],
            verified_purchase=True,
        )

        analysis = self.analyzer.analyze(review)

        expected = [
            {
                "aspect": item["aspect"],
                "sentiment": item["sentiment"],
            }
            for item in case.get("expected", [])
        ]
        predicted = [
            {
                "aspect": item.aspect,
                "sentiment": item.sentiment.value,
            }
            for item in analysis.aspect_sentiments
        ]

        expected_by_aspect = {item["aspect"]: item for item in expected}
        predicted_by_aspect = {item["aspect"]: item for item in predicted}

        matched_aspects = set(expected_by_aspect) & set(predicted_by_aspect)
        missing_aspects = set(expected_by_aspect) - set(predicted_by_aspect)
        unexpected_aspects = set(predicted_by_aspect) - set(expected_by_aspect)

        sentiment_matched_count = sum(
            1
            for aspect in matched_aspects
            if expected_by_aspect[aspect]["sentiment"] == predicted_by_aspect[aspect]["sentiment"]
        )

        return GoldenCaseResult(
            case_id=case["case_id"],
            expected_count=len(expected),
            predicted_count=len(predicted),
            matched_count=len(matched_aspects),
            sentiment_matched_count=sentiment_matched_count,
            missing=[expected_by_aspect[aspect] for aspect in sorted(missing_aspects)],
            unexpected=[predicted_by_aspect[aspect] for aspect in sorted(unexpected_aspects)],
        )