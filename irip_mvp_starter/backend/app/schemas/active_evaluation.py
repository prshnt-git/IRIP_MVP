from __future__ import annotations

from pydantic import BaseModel, Field


class ActiveEvaluationQueueItem(BaseModel):
    id: int
    review_id: str
    product_id: str
    aspect: str | None = None
    predicted_sentiment: str | None = None
    provider: str | None = None
    reason: str
    priority_score: float
    status: str
    created_at: str
    updated_at: str


class ActiveEvaluationQueueBuildResponse(BaseModel):
    inserted_count: int
    open_count: int
    reasons: dict[str, int]


class ActiveEvaluationQueueStatusUpdate(BaseModel):
    status: str = Field(pattern="^(open|reviewed|ignored|promoted)$")

class GoldenExpectedLabel(BaseModel):
    aspect: str
    sentiment: str = Field(pattern="^(positive|negative|neutral|mixed)$")


class ActiveEvaluationPromoteRequest(BaseModel):
    expected_aspect: str | None = None
    expected_sentiment: str | None = Field(
        default=None,
        pattern="^(positive|negative|neutral|mixed)$",
    )
    expected: list[GoldenExpectedLabel] | None = None
    note: str | None = None


class GoldenReviewCaseItem(BaseModel):
    id: int
    case_id: str
    source_queue_item_id: int | None = None
    review_id: str
    product_id: str
    product_name: str | None = None
    source: str | None = None
    rating: float | None = None
    review_date: str | None = None
    raw_text: str
    expected: list[dict]
    note: str | None = None
    approved_by_human: bool
    created_at: str
    updated_at: str

class GoldenReviewCaseUpdateRequest(BaseModel):
    expected: list[GoldenExpectedLabel]
    note: str | None = None