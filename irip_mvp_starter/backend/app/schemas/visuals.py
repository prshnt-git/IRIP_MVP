from __future__ import annotations

from pydantic import BaseModel


class VisualKpiCard(BaseModel):
    id: str
    label: str
    value: str | int | float | None
    helper_text: str | None = None
    status: str | None = None


class VisualChartEncoding(BaseModel):
    x: str | None = None
    y: str | None = None
    value: str | None = None
    category: str | None = None
    color_by: str | None = None


class VisualEchartsRecommendation(BaseModel):
    series_type: str
    orientation: str | None = None
    interactive: list[str] = []
    suggested_chart: str


class VisualChartDatum(BaseModel):
    label: str
    value: float | int
    secondary_value: float | int | None = None
    category: str | None = None
    status: str | None = None
    helper_text: str | None = None


class VisualChartBlock(BaseModel):
    chart_id: str
    chart_type: str
    title: str
    description: str | None = None
    data: list[VisualChartDatum]
    encoding: VisualChartEncoding
    recommended_echarts: VisualEchartsRecommendation


class VisualAspectSentimentDatum(BaseModel):
    aspect: str
    mentions: int
    positive_count: int
    negative_count: int
    neutral_count: int = 0
    aspect_score: float
    avg_confidence: float | None = None
    sentiment_label: str
    priority_bucket: str
    interpretation: str


class VisualAspectSentimentChart(BaseModel):
    chart_id: str
    chart_type: str
    title: str
    description: str | None = None
    data: list[VisualAspectSentimentDatum]
    encoding: VisualChartEncoding
    recommended_echarts: VisualEchartsRecommendation


class VisualSentimentPriorityDatum(BaseModel):
    aspect: str
    mentions: int
    aspect_score: float
    sentiment_label: str
    priority_bucket: str
    priority_level: str
    interpretation: str


class VisualSentimentPriorityMatrix(BaseModel):
    matrix_id: str
    title: str
    description: str | None = None
    data: list[VisualSentimentPriorityDatum]


class VisualAspectReasonCard(BaseModel):
    aspect: str
    reaction: str
    mention_count: int
    positive_count: int
    negative_count: int
    neutral_count: int = 0
    one_liner: str
    evidence_terms: list[str] = []
    evidence_examples: list[str] = []
    confidence_label: str

    # Debug / transparency fields for the analyst and developer.
    # Frontend can ignore these, but they help us verify whether Gemini or rules produced the card.
    llm_generated: bool = False
    reason_source: str | None = None

class VisualBenchmarkSummary(BaseModel):
    headline: str
    selected_product_summary: str
    competitor_summary: str
    risk_summary: str
    bullets: list[str] = []
    source: str | None = None

class VisualCompetitorGapDatum(BaseModel):
    aspect: str
    gap: float
    own_score: float
    competitor_score: float
    confidence_label: str
    interpretation: str


class VisualCompetitorGapChart(BaseModel):
    chart_id: str
    chart_type: str
    title: str
    description: str | None = None
    data: list[VisualCompetitorGapDatum]
    encoding: VisualChartEncoding
    recommended_echarts: VisualEchartsRecommendation


class VisualSignalChip(BaseModel):
    label: str
    signal_type: str
    weight: int | float | None = None


class VisualEvidenceLink(BaseModel):
    label: str
    source_type: str
    source_name: str | None = None
    evidence_url: str | None = None
    reference_id: str | None = None


class VisualDashboardResponse(BaseModel):
    product_id: str
    competitor_product_id: str | None = None
    readiness_status: str
    workflow_tiles: list[dict]
    kpi_cards: list[VisualKpiCard]

    sentiment_distribution_chart: VisualChartBlock
    top_aspect_chart: VisualChartBlock
    aspect_sentiment_chart: VisualAspectSentimentChart | None = None
    sentiment_priority_matrix: VisualSentimentPriorityMatrix | None = None
    sentiment_insight_cards: list[VisualKpiCard] = []
    aspect_reason_cards: list[VisualAspectReasonCard] = []

    competitor_gap_chart: VisualCompetitorGapChart
    benchmark_spec_table: dict | None = None
    benchmark_summary: VisualBenchmarkSummary | None = None
    news_signal_chart: VisualChartBlock
    source_tier_chart: VisualChartBlock
    quality_cards: list[VisualKpiCard]
    news_signal_chips: list[VisualSignalChip]
    recommended_actions: list[str]
    evidence_links: list[VisualEvidenceLink]