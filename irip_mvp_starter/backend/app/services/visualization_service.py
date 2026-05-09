from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from typing import Any

from app.db.repository import ReviewRepository
from app.services.aspect_reason_service import AspectReasonService
from app.services.benchmark_summary_service import BenchmarkSummaryService
from app.services.executive_report_service import ExecutiveReportService
from app.services.llm_service import LlmService
from app.services.news_brief_service import NewsBriefService
from app.services.system_readiness_service import SystemReadinessService

class VisualizationService:
    def __init__(
        self,
        repository: ReviewRepository,
        executive_report_service: ExecutiveReportService,
        news_brief_service: NewsBriefService,
        system_readiness_service: SystemReadinessService,
    ) -> None:
        self.repository = repository
        self.executive_report_service = executive_report_service
        self.news_brief_service = news_brief_service
        self.system_readiness_service = system_readiness_service
        self.benchmark_summary_service = BenchmarkSummaryService()
        self.aspect_reason_service = AspectReasonService(repository)
        self.llm_service = LlmService()
        self._spec_benchmark_cache: dict[str, dict] = {}

    def dashboard(
        self,
        product_id: str,
        competitor_product_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        product_summary = self.repository.get_product_summary(
        product_id=product_id,
        start_date=start_date,
        end_date=end_date,
        )

        aspect_summary = self.repository.get_aspect_summary(
            product_id=product_id,
            start_date=start_date,
            end_date=end_date,
        )

        competitor_benchmark = None
        if competitor_product_id:
            competitor_benchmark = self.repository.get_competitor_benchmark(
                product_id=product_id,
                competitor_product_id=competitor_product_id,
                start_date=start_date,
                end_date=end_date,
            )

        news_brief = self.news_brief_service.build_brief(
            min_relevance_score=35,
            limit=8,
        )

        executive_report = self.executive_report_service.build_report(
            product_id=product_id,
            competitor_product_id=competitor_product_id,
            start_date=start_date,
            end_date=end_date,
        )

        readiness = self.system_readiness_service.readiness(product_version="v1.0")

        aspect_reason_cards = self.aspect_reason_service.build_cards(
            product_id=product_id,
            aspect_summary=aspect_summary,
            start_date=start_date,
            end_date=end_date,
        )

        return {
            "product_id": product_id,
            "competitor_product_id": competitor_product_id,
            "readiness_status": readiness["readiness_status"],
            "workflow_tiles": self._workflow_tiles(
                product_summary=product_summary,
                competitor_benchmark=competitor_benchmark,
                news_brief=news_brief,
                executive_report=executive_report,
                readiness=readiness,
                has_competitor=bool(competitor_product_id),
            ),
            "kpi_cards": self._kpi_cards(product_summary),
            "sentiment_distribution_chart": self._sentiment_distribution_chart(product_summary),
            "top_aspect_chart": self._top_aspect_chart(product_summary),
            "aspect_sentiment_chart": self._aspect_sentiment_chart(aspect_summary),
            "sentiment_priority_matrix": self._sentiment_priority_matrix(aspect_summary),
            "sentiment_insight_cards": self._sentiment_insight_cards(
                product_summary=product_summary,
                aspect_summary=aspect_summary,
            ),
            "aspect_reason_cards": aspect_reason_cards,
            "competitor_gap_chart": self._competitor_gap_chart(
                competitor_benchmark=competitor_benchmark,
                has_competitor=bool(competitor_product_id),
            ),
            "benchmark_summary": self.benchmark_summary_service.build_summary(
                product_id=product_id,
                competitor_product_id=competitor_product_id,
                competitor_benchmark=competitor_benchmark,
                selected_reason_cards=aspect_reason_cards,
            ),
            "benchmark_spec_table": self._benchmark_spec_table(
                product_id=product_id,
                competitor_product_id=competitor_product_id,
            ),
            "news_signal_chart": self._news_signal_chart(news_brief),
            "source_tier_chart": self._source_tier_chart(news_brief),
            "quality_cards": self._quality_cards(
                product_summary=product_summary,
                readiness=readiness,
            ),
            "news_signal_chips": self._news_signal_chips(news_brief),
            "recommended_actions": executive_report.get("recommended_actions", [])[:6],
            "evidence_links": executive_report.get("evidence_links", []),
        }

    def _workflow_tiles(
        self,
        product_summary: dict,
        competitor_benchmark: dict | None,
        news_brief: dict,
        executive_report: dict,
        readiness: dict,
        has_competitor: bool,
    ) -> list[dict]:
        review_count = int(product_summary.get("review_count") or 0)
        strengths = executive_report.get("key_strengths", []) or []
        risks = executive_report.get("key_risks", []) or []

        return [
            {
                "id": "customer_signal",
                "title": "Customer Signal",
                "status": "active" if strengths else "thin_evidence",
                "primary_text": strengths[0] if strengths else "No strong positive signal yet.",
                "secondary_text": "The clearest positive theme from selected product reviews.",
            },
            {
                "id": "risk_signal",
                "title": "Risk Signal",
                "status": "active" if risks else "thin_evidence",
                "primary_text": risks[0] if risks else "No strong risk signal yet.",
                "secondary_text": "The issue most worth validating with more evidence.",
            },
            {
                "id": "evidence_volume",
                "title": "Evidence Volume",
                "status": "low_volume" if review_count < 30 else "usable",
                "primary_text": f"{review_count} review(s) analyzed.",
                "secondary_text": (
                    "Treat as directional until review volume improves."
                    if review_count < 30
                    else "Enough review volume for stronger directional signals."
                ),
            },
            {
                "id": "benchmark_mode",
                "title": "Benchmark Mode",
                "status": "active" if competitor_benchmark else "product_only",
                "primary_text": (
                    f"{len(competitor_benchmark.get('benchmark_aspects', []))} comparable aspect(s)."
                    if competitor_benchmark
                    else "Product-only analysis."
                ),
                "secondary_text": (
                    "Competitor comparison is included."
                    if has_competitor
                    else "Add competitor only when benchmark comparison is needed."
                ),
            },
            {
                "id": "market_context",
                "title": "Market Context",
                "status": "active" if news_brief.get("total_items_considered", 0) else "empty",
                "primary_text": f"{news_brief.get('high_priority_count', 0)} high-priority market signal(s).",
                "secondary_text": "Use as external context, not as a replacement for review evidence.",
            },
        ]

    def _kpi_cards(self, product_summary: dict) -> list[dict]:
        review_count = int(product_summary.get("review_count") or 0)
        confidence_status = "low_volume" if review_count < 30 else "stronger_volume"

        return [
            {
                "id": "review_count",
                "label": "Review Volume",
                "value": review_count,
                "helper_text": "Total usable reviews in the selected scope.",
                "status": confidence_status,
            },
            {
                "id": "average_rating",
                "label": "User Rating",
                "value": product_summary.get("average_rating"),
                "helper_text": "Average marketplace rating for the selected product.",
                "status": None,
            },
            {
                "id": "quality_score",
                "label": "Input Quality",
                "value": product_summary.get("average_quality_score"),
                "helper_text": "Signal quality of imported review text.",
                "status": None,
            },
            {
                "id": "net_sentiment",
                "label": "Sentiment Direction",
                "value": product_summary.get("net_sentiment_score"),
                "helper_text": "Directional aggregate sentiment, not a final product score.",
                "status": "directional" if review_count < 30 else "usable",
            },
        ]

    def _sentiment_distribution_chart(self, product_summary: dict) -> dict:
        sentiment_counts = product_summary.get("sentiment_counts", {}) or {}

        data = [
            {
                "label": sentiment,
                "value": count,
                "category": "sentiment",
                "status": sentiment,
                "helper_text": f"{count} {sentiment} signal(s)",
            }
            for sentiment, count in sorted(sentiment_counts.items())
        ]

        return {
            "chart_id": "sentiment_distribution",
            "chart_type": "echarts_donut",
            "title": "Customer Sentiment Mix",
            "description": "Overall split of positive, negative, and neutral aspect signals.",
            "data": data,
            "encoding": {
                "value": "value",
                "category": "label",
                "color_by": "status",
            },
            "recommended_echarts": {
                "series_type": "pie",
                "orientation": None,
                "interactive": ["tooltip", "legend"],
                "suggested_chart": "donut",
            },
        }

    def _top_aspect_chart(self, product_summary: dict) -> dict:
        data = [
            {
                "label": item["aspect"],
                "value": item["mentions"],
                "category": "aspect_mentions",
                "status": "volume",
                "helper_text": f"{item['mentions']} mention(s)",
            }
            for item in product_summary.get("top_aspects", [])
        ]

        return {
            "chart_id": "top_aspects",
            "chart_type": "echarts_horizontal_bar",
            "title": "Aspect Discussion Volume",
            "description": "Ranks the smartphone aspects users mention most often.",
            "data": data,
            "encoding": {
                "x": "value",
                "y": "label",
                "value": "value",
                "category": "label",
                "color_by": "category",
            },
            "recommended_echarts": {
                "series_type": "bar",
                "orientation": "horizontal",
                "interactive": ["tooltip", "dataZoom"],
                "suggested_chart": "horizontal_bar",
            },
        }

    def _aspect_sentiment_chart(self, aspect_summary: list[dict]) -> dict:
        data = []

        for item in self._sorted_aspects(aspect_summary):
            aspect = str(item.get("aspect") or "unknown")
            mentions = int(item.get("mentions") or 0)
            positive_count = int(item.get("positive_count") or 0)
            negative_count = int(item.get("negative_count") or 0)
            neutral_count = int(item.get("neutral_count") or 0)
            aspect_score = float(item.get("aspect_score") or 0)
            avg_confidence = item.get("avg_confidence")

            sentiment_label = self._sentiment_label(
                positive_count=positive_count,
                negative_count=negative_count,
                neutral_count=neutral_count,
                aspect_score=aspect_score,
            )
            priority_bucket = self._sentiment_priority_bucket(
                mentions=mentions,
                positive_count=positive_count,
                negative_count=negative_count,
                aspect_score=aspect_score,
            )

            data.append(
                {
                    "aspect": aspect,
                    "mentions": mentions,
                    "positive_count": positive_count,
                    "negative_count": negative_count,
                    "neutral_count": neutral_count,
                    "aspect_score": round(aspect_score, 2),
                    "avg_confidence": avg_confidence,
                    "sentiment_label": sentiment_label,
                    "priority_bucket": priority_bucket,
                    "interpretation": self._aspect_sentiment_interpretation(
                        aspect=aspect,
                        mentions=mentions,
                        sentiment_label=sentiment_label,
                        priority_bucket=priority_bucket,
                    ),
                }
            )

        return {
            "chart_id": "aspect_sentiment_polarity",
            "chart_type": "echarts_diverging_stacked_bar",
            "title": "Aspect Sentiment by Topic",
            "description": "Shows which product aspects are positive-heavy, negative-heavy, mixed, or thin evidence.",
            "data": data,
            "encoding": {
                "x": "positive_negative_counts",
                "y": "aspect",
                "value": "mentions",
                "category": "sentiment_label",
                "color_by": "sentiment_label",
            },
            "recommended_echarts": {
                "series_type": "bar",
                "orientation": "horizontal",
                "interactive": ["tooltip", "dataZoom", "legend"],
                "suggested_chart": "diverging_stacked_horizontal_bar",
            },
        }

    def _sentiment_priority_matrix(self, aspect_summary: list[dict]) -> dict:
        data = []

        for item in self._sorted_aspects(aspect_summary):
            aspect = str(item.get("aspect") or "unknown")
            mentions = int(item.get("mentions") or 0)
            positive_count = int(item.get("positive_count") or 0)
            negative_count = int(item.get("negative_count") or 0)
            neutral_count = int(item.get("neutral_count") or 0)
            aspect_score = float(item.get("aspect_score") or 0)

            sentiment_label = self._sentiment_label(
                positive_count=positive_count,
                negative_count=negative_count,
                neutral_count=neutral_count,
                aspect_score=aspect_score,
            )
            priority_bucket = self._sentiment_priority_bucket(
                mentions=mentions,
                positive_count=positive_count,
                negative_count=negative_count,
                aspect_score=aspect_score,
            )
            priority_level = self._priority_level(priority_bucket)

            data.append(
                {
                    "aspect": aspect,
                    "mentions": mentions,
                    "aspect_score": round(aspect_score, 2),
                    "sentiment_label": sentiment_label,
                    "priority_bucket": priority_bucket,
                    "priority_level": priority_level,
                    "interpretation": self._aspect_sentiment_interpretation(
                        aspect=aspect,
                        mentions=mentions,
                        sentiment_label=sentiment_label,
                        priority_bucket=priority_bucket,
                    ),
                }
            )

        return {
            "matrix_id": "sentiment_priority_matrix",
            "title": "Sentiment Priority Matrix",
            "description": "Classifies aspect signals by mention volume and sentiment direction.",
            "data": data,
        }

    def _sentiment_insight_cards(
        self,
        product_summary: dict,
        aspect_summary: list[dict],
    ) -> list[dict]:
        review_count = int(product_summary.get("review_count") or 0)
        sentiment_counts = product_summary.get("sentiment_counts", {}) or {}
        positive = int(sentiment_counts.get("positive") or 0)
        negative = int(sentiment_counts.get("negative") or 0)
        neutral = int(sentiment_counts.get("neutral") or 0)

        strongest_positive = self._strongest_positive_aspect(aspect_summary)
        strongest_negative = self._strongest_negative_aspect(aspect_summary)
        polarized = self._polarized_aspect(aspect_summary)

        mood_label, mood_helper, mood_status = self._mood_balance(
            positive=positive,
            negative=negative,
            neutral=neutral,
        )

        return [
            {
                "id": "mood_balance",
                "label": "Mood Balance",
                "value": mood_label,
                "helper_text": mood_helper,
                "status": mood_status,
            },
            {
                "id": "dominant_positive_area",
                "label": "Positive Driver",
                "value": self._labelize(strongest_positive["aspect"]) if strongest_positive else "Not clear",
                "helper_text": (
                    self._aspect_sentiment_interpretation(
                        aspect=strongest_positive["aspect"],
                        mentions=int(strongest_positive.get("mentions") or 0),
                        sentiment_label="positive-heavy",
                        priority_bucket=self._sentiment_priority_bucket(
                            mentions=int(strongest_positive.get("mentions") or 0),
                            positive_count=int(strongest_positive.get("positive_count") or 0),
                            negative_count=int(strongest_positive.get("negative_count") or 0),
                            aspect_score=float(strongest_positive.get("aspect_score") or 0),
                        ),
                    )
                    if strongest_positive
                    else "No strong positive aspect signal yet."
                ),
                "status": "good" if strongest_positive else "neutral",
            },
            {
                "id": "dominant_negative_area",
                "label": "Complaint Driver",
                "value": self._labelize(strongest_negative["aspect"]) if strongest_negative else "Not clear",
                "helper_text": (
                    self._aspect_sentiment_interpretation(
                        aspect=strongest_negative["aspect"],
                        mentions=int(strongest_negative.get("mentions") or 0),
                        sentiment_label="negative-heavy",
                        priority_bucket=self._sentiment_priority_bucket(
                            mentions=int(strongest_negative.get("mentions") or 0),
                            positive_count=int(strongest_negative.get("positive_count") or 0),
                            negative_count=int(strongest_negative.get("negative_count") or 0),
                            aspect_score=float(strongest_negative.get("aspect_score") or 0),
                        ),
                    )
                    if strongest_negative
                    else "No strong complaint aspect signal yet."
                ),
                "status": "bad" if strongest_negative else "neutral",
            },
            {
                "id": "sentiment_evidence_strength",
                "label": "Evidence Strength",
                "value": self._evidence_strength_label(review_count),
                "helper_text": self._evidence_strength_helper(review_count),
                "status": "warn" if review_count < 30 else "good",
            },
            {
                "id": "polarized_aspect",
                "label": "Polarized Aspect",
                "value": self._labelize(polarized["aspect"]) if polarized else "None yet",
                "helper_text": (
                    "This aspect has both positive and negative signals."
                    if polarized
                    else "No aspect has enough mixed evidence yet."
                ),
                "status": "warn" if polarized else "neutral",
            },
        ]

    def _competitor_gap_chart(
        self,
        competitor_benchmark: dict | None,
        has_competitor: bool,
    ) -> dict:
        data = []

        if competitor_benchmark:
            data = [
                {
                    "aspect": item["aspect"],
                    "gap": item["gap"],
                    "own_score": item["own_score"],
                    "competitor_score": item["competitor_score"],
                    "confidence_label": item["confidence_label"],
                    "interpretation": item["interpretation"],
                }
                for item in competitor_benchmark.get("benchmark_aspects", [])
            ]

        return {
            "chart_id": "competitor_gap",
            "chart_type": "echarts_diverging_bar",
            "title": "Competitor Gap by Aspect" if has_competitor else "No Competitor Selected",
            "description": (
                "Positive gap means the selected product leads; negative gap means competitor leads or evidence gap exists."
                if has_competitor
                else "This is currently a product-only analysis. Select a competitor only when benchmark comparison is needed."
            ),
            "data": data,
            "encoding": {
                "x": "gap",
                "y": "aspect",
                "value": "gap",
                "category": "aspect",
                "color_by": "gap_direction",
            },
            "recommended_echarts": {
                "series_type": "bar",
                "orientation": "horizontal",
                "interactive": ["tooltip", "dataZoom"],
                "suggested_chart": "diverging_horizontal_bar",
            },
        }

    def _news_signal_chart(self, news_brief: dict) -> dict:
        counter: Counter[str] = Counter()

        for label in news_brief.get("key_technology_signals", []):
            counter[f"technology:{label}"] += 1

        for label in news_brief.get("key_company_signals", []):
            counter[f"company:{label}"] += 1

        for label in news_brief.get("key_region_signals", []):
            counter[f"region:{label}"] += 1

        data = []

        for key, count in counter.most_common(12):
            signal_type, label = key.split(":", 1)
            data.append(
                {
                    "label": label,
                    "value": count,
                    "category": signal_type,
                    "status": signal_type,
                    "helper_text": f"{signal_type} signal",
                }
            )

        return {
            "chart_id": "news_signals",
            "chart_type": "echarts_horizontal_bar",
            "title": "Trusted Market Signals",
            "description": "Shows technology, company, and region signals from trusted market sources.",
            "data": data,
            "encoding": {
                "x": "value",
                "y": "label",
                "value": "value",
                "category": "label",
                "color_by": "category",
            },
            "recommended_echarts": {
                "series_type": "bar",
                "orientation": "horizontal",
                "interactive": ["tooltip", "legend"],
                "suggested_chart": "tag_frequency_bar",
            },
        }

    def _source_tier_chart(self, news_brief: dict) -> dict:
        source_tier_mix = news_brief.get("source_tier_mix", {}) or {}

        data = [
            {
                "label": f"Tier {tier}",
                "value": count,
                "category": "source_tier",
                "status": f"tier_{tier}",
                "helper_text": f"{count} item(s) from source tier {tier}",
            }
            for tier, count in sorted(source_tier_mix.items())
        ]

        return {
            "chart_id": "source_tier_mix",
            "chart_type": "echarts_donut",
            "title": "Source Trust Mix",
            "description": "Shows the trust-tier mix of market/news evidence used for context.",
            "data": data,
            "encoding": {
                "value": "value",
                "category": "label",
                "color_by": "status",
            },
            "recommended_echarts": {
                "series_type": "pie",
                "orientation": None,
                "interactive": ["tooltip", "legend"],
                "suggested_chart": "donut",
            },
        }

    def _quality_cards(
        self,
        product_summary: dict,
        readiness: dict,
    ) -> list[dict]:
        warnings = readiness.get("warnings", [])
        review_count = int(product_summary.get("review_count") or 0)

        return [
            {
                "id": "readiness_status",
                "label": "Readiness",
                "value": self._labelize(readiness.get("readiness_status")),
                "helper_text": "Whether the current workspace is safe to interpret.",
                "status": readiness.get("readiness_status"),
            },
            {
                "id": "warning_count",
                "label": "Warnings",
                "value": len(warnings),
                "helper_text": warnings[0] if warnings else "No readiness warnings.",
                "status": "warn" if warnings else "pass",
            },
            {
                "id": "confidence_note",
                "label": "Confidence",
                "value": "Directional" if review_count < 30 else "Stronger",
                "helper_text": (
                    "Low review volume. Validate before decisions."
                    if review_count < 30
                    else "Review volume supports stronger directional analysis."
                ),
                "status": "directional" if review_count < 30 else "stronger",
            },
        ]

    def _news_signal_chips(self, news_brief: dict) -> list[dict]:
        chips: list[dict] = []

        for label in news_brief.get("key_technology_signals", [])[:8]:
            chips.append(
                {
                    "label": label,
                    "signal_type": "technology",
                    "weight": None,
                }
            )

        for label in news_brief.get("key_company_signals", [])[:6]:
            chips.append(
                {
                    "label": label,
                    "signal_type": "company",
                    "weight": None,
                }
            )

        for label in news_brief.get("key_region_signals", [])[:6]:
            chips.append(
                {
                    "label": label,
                    "signal_type": "region",
                    "weight": None,
                }
            )

        seen = set()
        deduped = []

        for chip in chips:
            key = (chip["label"], chip["signal_type"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(chip)

        return deduped

    def _sorted_aspects(self, aspect_summary: list[dict]) -> list[dict]:
        return sorted(
            aspect_summary,
            key=lambda item: (
                int(item.get("mentions") or 0),
                abs(float(item.get("aspect_score") or 0)),
            ),
            reverse=True,
        )

    def _sentiment_label(
        self,
        positive_count: int,
        negative_count: int,
        neutral_count: int,
        aspect_score: float,
    ) -> str:
        total = positive_count + negative_count + neutral_count

        if total == 0:
            return "no-signal"

        if positive_count > 0 and negative_count > 0:
            if abs(aspect_score) < 25:
                return "mixed"
            return "polarized-positive" if aspect_score > 0 else "polarized-negative"

        if aspect_score >= 25:
            return "positive-heavy"

        if aspect_score <= -25:
            return "negative-heavy"

        if neutral_count > 0:
            return "neutral"

        return "mixed"

    def _sentiment_priority_bucket(
        self,
        mentions: int,
        positive_count: int,
        negative_count: int,
        aspect_score: float,
    ) -> str:
        high_volume = mentions >= 3
        thin = mentions < 2

        if thin:
            if aspect_score >= 25:
                return "small-delight"
            if aspect_score <= -25:
                return "watchlist-complaint"
            return "thin-evidence"

        if high_volume and negative_count > positive_count:
            return "high-volume-complaint"

        if high_volume and positive_count > negative_count:
            return "high-volume-delight"

        if negative_count > positive_count:
            return "complaint-driver"

        if positive_count > negative_count:
            return "delight-driver"

        return "mixed-signal"

    def _priority_level(self, priority_bucket: str) -> str:
        if priority_bucket in {"high-volume-complaint", "complaint-driver"}:
            return "high"

        if priority_bucket in {"watchlist-complaint", "mixed-signal"}:
            return "medium"

        if priority_bucket in {"high-volume-delight", "delight-driver"}:
            return "positive"

        return "low"

    def _aspect_sentiment_interpretation(
        self,
        aspect: str,
        mentions: int,
        sentiment_label: str,
        priority_bucket: str,
    ) -> str:
        readable_aspect = self._labelize(aspect)

        if priority_bucket == "high-volume-complaint":
            return f"{readable_aspect} is a high-volume complaint area and should be treated as a priority pain point."

        if priority_bucket == "complaint-driver":
            return f"{readable_aspect} is currently a complaint driver, but evidence volume is still limited."

        if priority_bucket == "watchlist-complaint":
            return f"{readable_aspect} has negative sentiment, but only {mentions} mention(s). Keep it on the watchlist."

        if priority_bucket == "high-volume-delight":
            return f"{readable_aspect} is a high-volume positive driver and likely a product strength."

        if priority_bucket == "delight-driver":
            return f"{readable_aspect} is currently a positive driver, but evidence volume is still limited."

        if priority_bucket == "small-delight":
            return f"{readable_aspect} has a small positive signal with only {mentions} mention(s)."

        if sentiment_label.startswith("polarized") or priority_bucket == "mixed-signal":
            return f"{readable_aspect} has mixed signals and should be checked with supporting review evidence."

        return f"{readable_aspect} has thin evidence. Avoid drawing a strong conclusion yet."

    def _mood_balance(
        self,
        positive: int,
        negative: int,
        neutral: int,
    ) -> tuple[str, str, str]:
        total = positive + negative + neutral

        if total == 0:
            return (
                "No Signal",
                "No sentiment signal is available yet.",
                "neutral",
            )

        if positive > negative * 1.25:
            return (
                "Positive-Leaning",
                f"{positive} positive vs {negative} negative signal(s).",
                "good",
            )

        if negative > positive * 1.25:
            return (
                "Negative-Leaning",
                f"{negative} negative vs {positive} positive signal(s).",
                "bad",
            )

        return (
            "Mixed",
            f"{positive} positive and {negative} negative signal(s) are currently balanced.",
            "warn",
        )

    def _strongest_positive_aspect(self, aspect_summary: list[dict]) -> dict | None:
        candidates = [
            item for item in aspect_summary
            if int(item.get("positive_count") or 0) > int(item.get("negative_count") or 0)
        ]

        if not candidates:
            return None

        return sorted(
            candidates,
            key=lambda item: (
                int(item.get("positive_count") or 0),
                int(item.get("mentions") or 0),
                float(item.get("aspect_score") or 0),
            ),
            reverse=True,
        )[0]

    def _strongest_negative_aspect(self, aspect_summary: list[dict]) -> dict | None:
        candidates = [
            item for item in aspect_summary
            if int(item.get("negative_count") or 0) > int(item.get("positive_count") or 0)
        ]

        if not candidates:
            return None

        return sorted(
            candidates,
            key=lambda item: (
                int(item.get("negative_count") or 0),
                int(item.get("mentions") or 0),
                abs(float(item.get("aspect_score") or 0)),
            ),
            reverse=True,
        )[0]

    def _polarized_aspect(self, aspect_summary: list[dict]) -> dict | None:
        candidates = [
            item for item in aspect_summary
            if int(item.get("positive_count") or 0) > 0
            and int(item.get("negative_count") or 0) > 0
        ]

        if not candidates:
            return None

        return sorted(
            candidates,
            key=lambda item: int(item.get("mentions") or 0),
            reverse=True,
        )[0]

    def _evidence_strength_label(self, review_count: int) -> str:
        if review_count == 0:
            return "No Evidence"
        if review_count < 30:
            return "Early Signal"
        if review_count < 100:
            return "Directional"
        if review_count < 500:
            return "Stronger Signal"
        return "High Confidence"

    def _evidence_strength_helper(self, review_count: int) -> str:
        if review_count == 0:
            return "Import reviews before interpreting sentiment."
        if review_count < 30:
            return "Small sample. Treat aspect sentiment as early signal."
        if review_count < 100:
            return "Usable sample for directional sentiment analysis."
        if review_count < 500:
            return "Stronger sample for product interpretation."
        return "Large review base supports high-confidence sentiment patterns."

    def _benchmark_spec_table(
        self,
        product_id: str,
        competitor_product_id: str | None,
    ) -> dict | None:
        if not competitor_product_id:
            return None

        selected_name = self._product_display_name(product_id)
        competitor_name = self._product_display_name(competitor_product_id)

        fallback = self._unknown_spec_table(
            selected_name=selected_name,
            competitor_name=competitor_name,
            source="rules",
        )

        if not self._should_use_spec_gemini():
            return fallback

        prompt = self._build_spec_prompt(
            selected_product_id=product_id,
            selected_product_name=selected_name,
            competitor_product_id=competitor_product_id,
            competitor_product_name=competitor_name,
        )

        cache_key = self._cache_key(prompt)
        if cache_key in self._spec_benchmark_cache:
            return self._spec_benchmark_cache[cache_key]

        try:
            raw_text = self.llm_service._call_gemini(prompt)
            parsed = self._extract_json(raw_text)
            table = self._validate_spec_table(
                parsed=parsed,
                selected_name=selected_name,
                competitor_name=competitor_name,
            )
            self._spec_benchmark_cache[cache_key] = table
            return table
        except Exception:
            return fallback


    def _product_display_name(self, product_id: str) -> str:
        try:
            products = self.repository.list_products()
        except Exception:
            return product_id

        for product in products:
            if product.get("product_id") == product_id:
                return str(product.get("product_name") or product_id)

        return product_id


    def _should_use_spec_gemini(self) -> bool:
        if os.getenv("PYTEST_CURRENT_TEST") and os.getenv("IRIP_ENABLE_LLM_IN_TESTS") != "1":
            return False

        mode = os.getenv("IRIP_BENCHMARK_SPEC_MODE", "auto").strip().lower()
        if mode in {"off", "rules", "rule", "false", "0"}:
            return False

        try:
            status = self.llm_service.status()
        except Exception:
            return False

        return bool(status.enabled) and mode in {"auto", "gemini", "llm", "always", "on", "true", "1"}


    def _build_spec_prompt(
        self,
        selected_product_id: str,
        selected_product_name: str,
        competitor_product_id: str,
        competitor_product_name: str,
    ) -> str:
        fields = [
            ("Commercial", "Launch date"),
            ("Commercial", "Current price"),
            ("Commercial", "Available variants"),
            ("Commercial", "Sales channel / availability"),
            ("Design", "Dimensions"),
            ("Design", "Weight"),
            ("Design", "Thickness"),
            ("Design", "Build material"),
            ("Design", "IP rating"),
            ("Design", "Colors"),
            ("Display", "Display size"),
            ("Display", "Display type"),
            ("Display", "Resolution"),
            ("Display", "Refresh rate"),
            ("Display", "Touch sampling rate"),
            ("Display", "Peak brightness"),
            ("Display", "Protection glass"),
            ("Display", "Aspect ratio"),
            ("Display", "Screen-to-body ratio"),
            ("Performance", "Chipset"),
            ("Performance", "CPU"),
            ("Performance", "GPU"),
            ("Performance", "RAM"),
            ("Performance", "RAM type"),
            ("Performance", "Storage"),
            ("Performance", "Storage type"),
            ("Performance", "Expandable storage"),
            ("Battery", "Battery capacity"),
            ("Battery", "Charging wattage"),
            ("Battery", "Charging type"),
            ("Battery", "Charger in box"),
            ("Battery", "Reverse charging"),
            ("Camera", "Rear camera setup"),
            ("Camera", "Primary sensor"),
            ("Camera", "Ultrawide camera"),
            ("Camera", "Telephoto / macro / depth"),
            ("Camera", "Front camera"),
            ("Camera", "OIS / EIS"),
            ("Camera", "Rear video recording"),
            ("Camera", "Front video recording"),
            ("Software", "Android version"),
            ("Software", "Custom UI"),
            ("Software", "OS update promise"),
            ("Software", "Security update promise"),
            ("Software", "AI features"),
            ("Network", "5G support"),
            ("Network", "5G bands"),
            ("Network", "Dual SIM"),
            ("Network", "Wi-Fi"),
            ("Network", "Bluetooth"),
            ("Network", "NFC"),
            ("Network", "USB type"),
            ("Network", "Headphone jack"),
            ("Audio & Sensors", "Speaker type"),
            ("Audio & Sensors", "Microphones"),
            ("Audio & Sensors", "Haptics"),
            ("Audio & Sensors", "Fingerprint sensor"),
            ("Audio & Sensors", "Gyroscope"),
            ("Audio & Sensors", "Compass"),
        ]

        payload = {
            "selected_product": {
                "product_id": selected_product_id,
                "product_name": selected_product_name,
            },
            "competitor_product": {
                "product_id": competitor_product_id,
                "product_name": competitor_product_name,
            },
            "required_fields": [
                {"category": category, "field": field}
                for category, field in fields
            ],
        }

        return f"""
    You are a smartphone product benchmarking analyst.

    Task:
    Create a structured specification comparison table for the two products below.

    Strict rules:
    - Return JSON only.
    - Do not write markdown.
    - Do not invent values.
    - If you are not confident, write "Unknown".
    - Do not compare review sentiment here; this table is only for product specifications.
    - Each row must compare the same specification field across both products.
    - Use short values suitable for a compact dashboard table.
    - confidence must be one of: "verified", "likely", "unknown".
    - source_status must be one of: "model_knowledge", "needs_source", "unknown".
    - winner must be one of: "selected_product", "competitor", "tie", "unknown", "not_applicable".
    - If both values are Unknown, winner must be "unknown".

    Return schema:
    {{
    "selected_product_name": "string",
    "competitor_product_name": "string",
    "source": "gemini",
    "confidence_note": "Short note about source confidence.",
    "rows": [
        {{
        "category": "Display",
        "field": "Display size",
        "selected_product_value": "6.7-inch",
        "competitor_value": "6.8-inch",
        "winner": "competitor",
        "confidence": "likely",
        "source_status": "model_knowledge",
        "why_it_matters": "Larger display can improve media and gaming experience."
        }}
    ],
    "unknown_fields": ["5G bands"]
    }}

    Input:
    {json.dumps(payload, ensure_ascii=False, indent=2)}
    """.strip()


    def _validate_spec_table(
        self,
        parsed: dict[str, Any],
        selected_name: str,
        competitor_name: str,
    ) -> dict:
        rows = parsed.get("rows")
        if not isinstance(rows, list):
            return self._unknown_spec_table(
                selected_name=selected_name,
                competitor_name=competitor_name,
                source="rules",
            )

        cleaned_rows: list[dict] = []

        for row in rows[:64]:
            if not isinstance(row, dict):
                continue

            category = self._clean_spec_cell(row.get("category"), "General", 40)
            field = self._clean_spec_cell(row.get("field"), "Unknown", 60)
            selected_value = self._clean_spec_cell(row.get("selected_product_value"), "Unknown", 90)
            competitor_value = self._clean_spec_cell(row.get("competitor_value"), "Unknown", 90)

            winner = str(row.get("winner") or "unknown").strip().lower()
            if winner not in {"selected_product", "competitor", "tie", "unknown", "not_applicable"}:
                winner = "unknown"

            confidence = str(row.get("confidence") or "unknown").strip().lower()
            if confidence not in {"verified", "likely", "unknown"}:
                confidence = "unknown"

            source_status = str(row.get("source_status") or "unknown").strip().lower()
            if source_status not in {"model_knowledge", "needs_source", "unknown"}:
                source_status = "unknown"

            why_it_matters = self._clean_spec_cell(
                row.get("why_it_matters"),
                "Useful for product comparison.",
                140,
            )

            cleaned_rows.append(
                {
                    "category": category,
                    "field": field,
                    "selected_product_value": selected_value,
                    "competitor_value": competitor_value,
                    "winner": winner,
                    "confidence": confidence,
                    "source_status": source_status,
                    "why_it_matters": why_it_matters,
                }
            )

        unknown_fields = parsed.get("unknown_fields")
        if not isinstance(unknown_fields, list):
            unknown_fields = []

        return {
            "selected_product_name": self._clean_spec_cell(
                parsed.get("selected_product_name"),
                selected_name,
                80,
            ),
            "competitor_product_name": self._clean_spec_cell(
                parsed.get("competitor_product_name"),
                competitor_name,
                80,
            ),
            "source": "gemini",
            "confidence_note": self._clean_spec_cell(
                parsed.get("confidence_note"),
                "Specs are generated from model knowledge and should be verified with official sources.",
                180,
            ),
            "rows": cleaned_rows,
            "unknown_fields": [
                self._clean_spec_cell(item, "Unknown", 60)
                for item in unknown_fields[:20]
            ],
        }


    def _unknown_spec_table(
        self,
        selected_name: str,
        competitor_name: str,
        source: str,
    ) -> dict:
        rows = [
            ("Commercial", "Current price"),
            ("Design", "Dimensions"),
            ("Design", "Weight"),
            ("Display", "Display size"),
            ("Display", "Resolution"),
            ("Display", "Refresh rate"),
            ("Performance", "Chipset"),
            ("Performance", "RAM"),
            ("Performance", "Storage"),
            ("Battery", "Battery capacity"),
            ("Battery", "Charging wattage"),
            ("Camera", "Rear camera setup"),
            ("Camera", "Front camera"),
            ("Software", "Android version"),
            ("Network", "5G support"),
            ("Audio & Sensors", "Speaker type"),
        ]

        return {
            "selected_product_name": selected_name,
            "competitor_product_name": competitor_name,
            "source": source,
            "confidence_note": "Specification data is not available yet. Add catalog/spec sources for verified comparison.",
            "rows": [
                {
                    "category": category,
                    "field": field,
                    "selected_product_value": "Unknown",
                    "competitor_value": "Unknown",
                    "winner": "unknown",
                    "confidence": "unknown",
                    "source_status": "needs_source",
                    "why_it_matters": "Useful for product comparison once verified specs are available.",
                }
                for category, field in rows
            ],
            "unknown_fields": [field for _, field in rows],
        }


    def _extract_json(self, raw_text: str) -> dict[str, Any]:
        cleaned = raw_text.strip()

        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
            cleaned = re.sub(r"```$", "", cleaned).strip()

        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
            if not match:
                raise RuntimeError("Gemini did not return JSON.")
            parsed = json.loads(match.group(0))

        if not isinstance(parsed, dict):
            raise RuntimeError("Gemini JSON root must be an object.")

        return parsed


    def _clean_spec_cell(self, value: Any, fallback: str, max_len: int) -> str:
        text = str(value or "").strip()
        if not text:
            text = fallback

        text = re.sub(r"\s+", " ", text).strip()

        if len(text) > max_len:
            text = text[: max_len - 3].rstrip() + "..."

        return text


    def _cache_key(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _labelize(self, value: str | None) -> str:
        if not value:
            return "Unknown"

        return " ".join(part.capitalize() for part in str(value).replace("_", " ").split())
    
