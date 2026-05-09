from pydantic import BaseModel, Field
from app.schemas.review import Sentiment


class GoldAspect(BaseModel):
    aspect: str
    sentiment: Sentiment


class EvaluationCase(BaseModel):
    case_id: str
    text: str
    expected_aspects: list[GoldAspect]
    notes: str | None = None


class EvaluationResult(BaseModel):
    provider_id: str
    total_cases: int
    exact_aspect_hits: int
    sentiment_hits: int
    aspect_precision_proxy: float = Field(ge=0, le=1)
    sentiment_accuracy_proxy: float = Field(ge=0, le=1)
    failed_case_ids: list[str]
