from __future__ import annotations

from pydantic import BaseModel


class ExecutiveReportSection(BaseModel):
    title: str
    bullets: list[str]


class ExecutiveReportEvidenceLink(BaseModel):
    label: str
    source_type: str
    source_name: str | None = None
    evidence_url: str | None = None
    reference_id: str | None = None


class ExecutiveReportResponse(BaseModel):
    report_title: str
    product_id: str
    competitor_product_id: str | None = None
    period: dict
    confidence_note: str
    executive_summary: list[str]
    key_strengths: list[str]
    key_risks: list[str]
    competitor_takeaways: list[str]
    market_news_signals: list[str]
    recommended_actions: list[str]
    sections: list[ExecutiveReportSection]
    evidence_links: list[ExecutiveReportEvidenceLink]