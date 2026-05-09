from __future__ import annotations

from collections import Counter

from app.services.news_ingestion_service import NewsIngestionService


class NewsBriefService:
    def __init__(self, news_ingestion_service: NewsIngestionService) -> None:
        self.news_ingestion_service = news_ingestion_service

    def build_brief(
        self,
        min_relevance_score: float = 35,
        limit: int = 10,
    ) -> dict:
        items = self.news_ingestion_service.list_news_items(
            min_relevance_score=min_relevance_score,
            limit=limit,
        )

        priority_counter = Counter(item["priority_label"] for item in items)
        source_tier_counter = Counter(str(item["source_tier"]) for item in items)

        technology_counter: Counter[str] = Counter()
        company_counter: Counter[str] = Counter()
        region_counter: Counter[str] = Counter()
        topic_counter: Counter[str] = Counter()

        for item in items:
            technology_counter.update(item["technology_tags"])
            company_counter.update(item["company_tags"])
            region_counter.update(item["region_tags"])
            topic_counter.update(item["topic_tags"])

        top_items = [self._brief_item(item) for item in items]
        evidence_links = [
            {
                "title": item["title"],
                "source_name": item["source_name"],
                "source_tier": item["source_tier"],
                "evidence_url": item["evidence_url"],
                "published_at": item["published_at"],
            }
            for item in items
        ]

        executive_summary = self._executive_summary(
            items=items,
            technology_counter=technology_counter,
            company_counter=company_counter,
            region_counter=region_counter,
            priority_counter=priority_counter,
        )

        recommended_actions = self._recommended_actions(
            technology_counter=technology_counter,
            company_counter=company_counter,
            region_counter=region_counter,
            priority_counter=priority_counter,
            item_count=len(items),
        )

        return {
            "brief_title": "Trusted News Intelligence Brief",
            "period": self._period(items),
            "total_items_considered": len(items),
            "high_priority_count": priority_counter.get("high", 0),
            "medium_priority_count": priority_counter.get("medium", 0),
            "source_tier_mix": dict(source_tier_counter),
            "key_technology_signals": [
                item for item, _ in technology_counter.most_common(8)
            ],
            "key_company_signals": [
                item for item, _ in company_counter.most_common(8)
            ],
            "key_region_signals": [
                item for item, _ in region_counter.most_common(8)
            ],
            "executive_summary": executive_summary,
            "recommended_actions": recommended_actions,
            "top_items": top_items,
            "evidence_links": evidence_links,
        }

    def _brief_item(self, item: dict) -> dict:
        return {
            "id": item["id"],
            "title": item["title"],
            "source_name": item["source_name"],
            "source_tier": item["source_tier"],
            "published_at": item["published_at"],
            "priority_label": item["priority_label"],
            "relevance_score": item["relevance_score"],
            "why_it_matters": item["why_it_matters"],
            "evidence_url": item["evidence_url"],
            "topic_tags": item["topic_tags"],
            "company_tags": item["company_tags"],
            "technology_tags": item["technology_tags"],
            "region_tags": item["region_tags"],
        }

    def _period(self, items: list[dict]) -> dict:
        dates = sorted(
            item["published_at"]
            for item in items
            if item.get("published_at")
        )

        return {
            "start_date": dates[0] if dates else None,
            "end_date": dates[-1] if dates else None,
        }

    def _executive_summary(
        self,
        items: list[dict],
        technology_counter: Counter[str],
        company_counter: Counter[str],
        region_counter: Counter[str],
        priority_counter: Counter[str],
    ) -> list[str]:
        if not items:
            return [
                "No trusted news items met the relevance threshold for this brief."
            ]

        summary: list[str] = []

        high_count = priority_counter.get("high", 0)
        if high_count:
            summary.append(
                f"{high_count} high-priority trusted news item(s) were identified for smartphone/OEM market intelligence."
            )
        else:
            summary.append(
                "No high-priority item was identified; current signals should be treated as background monitoring."
            )

        top_tech = [item for item, _ in technology_counter.most_common(4)]
        if top_tech:
            summary.append(
                f"Key technology signals include {', '.join(top_tech)}."
            )

        top_companies = [item for item, _ in company_counter.most_common(4)]
        if top_companies:
            summary.append(
                f"Key company/ecosystem signals include {', '.join(top_companies)}."
            )

        top_regions = [item for item, _ in region_counter.most_common(4)]
        if top_regions:
            summary.append(
                f"Priority region signals include {', '.join(top_regions)}."
            )

        tier_1_count = sum(1 for item in items if item["source_tier"] == 1)
        if tier_1_count:
            summary.append(
                f"{tier_1_count} item(s) come from Tier-1 official or primary sources, improving evidence quality."
            )

        return summary

    def _recommended_actions(
        self,
        technology_counter: Counter[str],
        company_counter: Counter[str],
        region_counter: Counter[str],
        priority_counter: Counter[str],
        item_count: int,
    ) -> list[str]:
        if item_count == 0:
            return [
                "Expand trusted source ingestion or lower the relevance threshold if coverage is unexpectedly empty."
            ]

        actions: list[str] = []

        if priority_counter.get("high", 0):
            actions.append(
                "Review high-priority items for possible inclusion in the weekly MICI intelligence report."
            )

        tech_signals = set(technology_counter)
        if tech_signals & {"npu", "edge_ai", "on-device", "camera_ai", "llm", "multimodal"}:
            actions.append(
                "Assess whether detected AI/on-device technology signals affect smartphone roadmap, camera stack, assistant strategy, or chipset planning."
            )

        if company_counter:
            actions.append(
                "Map company signals against competitor/OEM watchlists and update the market intelligence tracker."
            )

        if region_counter:
            actions.append(
                "Check whether region-specific signals have implications for India, Africa, MEA, LATAM, or other priority emerging markets."
            )

        if not actions:
            actions.append(
                "Keep the items as background monitoring; no immediate product or market action is suggested."
            )

        return actions