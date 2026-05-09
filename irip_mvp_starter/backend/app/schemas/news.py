from __future__ import annotations

from pydantic import BaseModel, Field


class TrustedNewsSourceItem(BaseModel):
    source_id: str
    source_name: str
    source_tier: int
    source_type: str
    allowed_domains: list[str]
    default_tags: list[str]
    is_active: bool


class NewsIngestRssRequest(BaseModel):
    source_id: str
    rss_url: str
    discovered_via: str = "rss"
    max_items: int = Field(default=20, ge=1, le=100)


class NewsIngestXmlRequest(BaseModel):
    source_id: str
    rss_xml: str
    discovered_via: str = "rss_fixture"
    max_items: int = Field(default=20, ge=1, le=100)


class NewsItem(BaseModel):
    id: int
    source_id: str
    source_name: str
    source_tier: int
    title: str
    canonical_url: str
    published_at: str | None = None
    summary: str | None = None
    discovered_via: str
    topic_tags: list[str]
    company_tags: list[str]
    technology_tags: list[str]
    region_tags: list[str]
    relevance_score: float
    priority_label: str
    why_it_matters: str | None = None
    evidence_url: str
    ingested_at: str


class NewsIngestResponse(BaseModel):
    source_id: str
    source_name: str
    inserted_count: int
    skipped_duplicate_count: int
    rejected_count: int
    items: list[NewsItem]

class NewsRescoreResponse(BaseModel):
    updated_count: int
    source_id: str | None = None
    limit: int

class NewsBriefItem(BaseModel):
    id: int
    title: str
    source_name: str
    source_tier: int
    published_at: str | None = None
    priority_label: str
    relevance_score: float
    why_it_matters: str | None = None
    evidence_url: str
    topic_tags: list[str]
    company_tags: list[str]
    technology_tags: list[str]
    region_tags: list[str]


class NewsBriefResponse(BaseModel):
    brief_title: str
    period: dict
    total_items_considered: int
    high_priority_count: int
    medium_priority_count: int
    source_tier_mix: dict[str, int]
    key_technology_signals: list[str]
    key_company_signals: list[str]
    key_region_signals: list[str]
    executive_summary: list[str]
    recommended_actions: list[str]
    top_items: list[NewsBriefItem]
    evidence_links: list[dict]