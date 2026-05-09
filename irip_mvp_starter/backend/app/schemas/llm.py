from __future__ import annotations

from pydantic import BaseModel, Field


class LlmProviderStatus(BaseModel):
    provider: str
    enabled: bool
    model: str | None = None
    mode: str = "selective"
    reason: str | None = None


class LlmModeUpdateRequest(BaseModel):
    mode: str = Field(description="Allowed values: off, selective, always")


class LlmModeUpdateResponse(BaseModel):
    mode: str
    message: str


class LlmAspectSentiment(BaseModel):
    aspect: str
    sub_aspect: str | None = None
    sentiment: str
    intensity: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_span: str
    reasoning_note: str | None = None


class LlmReviewExtractionRequest(BaseModel):
    product_id: str
    product_name: str | None = None
    raw_text: str
    rating: float | None = None
    source: str | None = None


class LlmReviewExtractionResponse(BaseModel):
    provider: str
    model: str
    product_id: str
    overall_sentiment: str
    language_profile: dict
    product_signal: bool
    delivery_signal: bool
    service_signal: bool
    sarcasm_flag: bool
    contradiction_flag: bool
    aspects: list[LlmAspectSentiment]
    confidence: float
    raw_model_text: str | None = None