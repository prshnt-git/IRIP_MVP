from __future__ import annotations

from pydantic import BaseModel, Field


class ExtractionFeedbackCreate(BaseModel):
    review_id: str
    product_id: str
    aspect: str
    predicted_sentiment: str
    provider: str | None = None
    is_correct: bool
    corrected_aspect: str | None = None
    corrected_sentiment: str | None = None
    note: str | None = None


class ExtractionFeedbackItem(BaseModel):
    id: int
    review_id: str
    product_id: str
    aspect: str
    predicted_sentiment: str
    provider: str | None = None
    is_correct: bool
    corrected_aspect: str | None = None
    corrected_sentiment: str | None = None
    note: str | None = None
    created_at: str


class ProviderQualityItem(BaseModel):
    provider: str
    total_feedback: int
    correct_count: int
    incorrect_count: int
    accuracy: float