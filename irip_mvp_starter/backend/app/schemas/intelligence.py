from __future__ import annotations

from pydantic import BaseModel


class ThemeEvidenceItem(BaseModel):
    review_id: str
    source: str | None = None
    rating: float | None = None
    review_date: str | None = None
    raw_text: str
    evidence_span: str | None = None
    confidence: float | None = None
    provider: str | None = None


class ThemeItem(BaseModel):
    theme_id: str
    theme_name: str
    aspect: str
    theme_type: str
    sentiment: str
    mention_count: int
    avg_intensity: float
    avg_confidence: float
    severity_score: float
    actionability: str
    interpretation: str
    evidence: list[ThemeEvidenceItem] = []


class ProductThemesResponse(BaseModel):
    product_id: str
    period: dict
    complaint_themes: list[ThemeItem]
    delight_themes: list[ThemeItem]
    watchlist_themes: list[ThemeItem]


class ForecastAspectItem(BaseModel):
    aspect: str
    current_score: float
    previous_score: float | None = None
    movement: float | None = None
    direction: str
    current_mentions: int
    previous_mentions: int
    confidence_label: str
    explanation: str


class ProductForecastResponse(BaseModel):
    product_id: str
    forecast_basis: str
    forecast_window: str
    overall_direction: str
    confidence_label: str
    aspects: list[ForecastAspectItem]
    caveats: list[str]


class IntelligenceBriefResponse(BaseModel):
    product_id: str
    period: dict
    executive_summary: str
    top_strengths: list[str]
    top_risks: list[str]
    recommended_actions: list[str]
    evidence_note: str
    confidence_note: str