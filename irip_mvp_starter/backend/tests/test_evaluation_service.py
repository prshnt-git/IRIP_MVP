from app.schemas.evaluation import EvaluationCase, GoldAspect
from app.schemas.review import Sentiment
from app.services.evaluation_service import EvaluationService


def test_evaluation_loop_scores_basic_gold_cases():
    service = EvaluationService()
    cases = [
        EvaluationCase(
            case_id="case_1",
            text="camera mast hai",
            expected_aspects=[GoldAspect(aspect="camera", sentiment=Sentiment.positive)],
        ),
        EvaluationCase(
            case_id="case_2",
            text="battery bakwas hai",
            expected_aspects=[GoldAspect(aspect="battery", sentiment=Sentiment.negative)],
        ),
    ]

    result = service.evaluate(cases)

    assert result.total_cases == 2
    assert result.sentiment_accuracy_proxy == 1.0
