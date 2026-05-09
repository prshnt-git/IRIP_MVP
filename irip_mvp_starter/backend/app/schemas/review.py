from enum import Enum
from pydantic import BaseModel, Field


class Sentiment(str, Enum):
    positive = "positive"
    negative = "negative"
    neutral = "neutral"
    mixed = "mixed"
    unknown = "unknown"


class SignalType(str, Enum):
    product = "product"
    delivery = "delivery"
    service = "service"
    packaging = "packaging"
    seller = "seller"
    unclear = "unclear"


class ReviewInput(BaseModel):
    review_id: str | None = None
    product_id: str
    product_name: str | None = None
    source: str | None = None
    rating: float | None = Field(default=None, ge=0, le=5)
    review_date: str | None = None
    raw_text: str = Field(min_length=1)
    verified_purchase: bool | None = None
    helpful_votes: int | None = Field(default=None, ge=0)


class AspectSentiment(BaseModel):
    aspect: str
    sub_aspect: str | None = None
    sentiment: Sentiment
    intensity: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    evidence_span: str
    provider: str


class ReviewAnalysis(BaseModel):
    review_id: str | None
    product_id: str
    clean_text: str
    language_profile: dict
    signal_types: list[SignalType]
    aspect_sentiments: list[AspectSentiment]
    quality_score: float = Field(ge=0, le=1)
    contradiction_flag: bool
    sarcasm_flag: bool
    processing_notes: list[str]
