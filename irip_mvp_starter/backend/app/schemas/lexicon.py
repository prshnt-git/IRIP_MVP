from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class LexiconTermType(str, Enum):
    # Core categories used by current seed_lexicon.py
    slang = "slang"
    aspect = "aspect"
    sentiment = "sentiment"
    intensifier = "intensifier"
    negation = "negation"
    emoji = "emoji"
    phrase = "phrase"
    spelling_variant = "spelling_variant"
    delivery_service = "delivery_service"
    sarcasm_marker = "sarcasm_marker"

    # Additional safe categories for Indian e-commerce review noise
    service = "service"
    after_sales = "after_sales"
    seller = "seller"
    product = "product"
    noise = "noise"
    rating_signal = "rating_signal"
    comparator = "comparator"
    metaphor = "metaphor"
    abbreviation = "abbreviation"
    complaint_marker = "complaint_marker"
    delight_marker = "delight_marker"
    issue_marker = "issue_marker"
    quality_marker = "quality_marker"
    purchase_context = "purchase_context"

    # Compatibility aliases for newer naming styles
    aspect_keyword = "aspect"
    sentiment_word = "sentiment"
    typo = "spelling_variant"


class LivingLexiconEntry(BaseModel):
    term: str
    normalized_term: str
    term_type: LexiconTermType
    language_type: str = "hi_en_mixed"
    aspect: str | None = None
    sentiment_prior: str | None = None
    intensity: float = Field(default=0.5, ge=0.0, le=1.0)
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    source: str = "seed"
    approved_by_human: bool = True
    examples: list[str] = Field(default_factory=list)


class LexiconEntryItem(BaseModel):
    id: int
    term: str
    normalized_term: str
    term_type: str
    language_type: str
    aspect: str | None = None
    sentiment_prior: str | None = None
    intensity: float | None = None
    confidence: float
    source: str
    approved_by_human: bool
    examples: list[str]
    created_at: str