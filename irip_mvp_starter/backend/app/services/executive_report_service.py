from __future__ import annotations

from app.db.repository import ReviewRepository
from app.services.intelligence_service import IntelligenceService
from app.services.news_brief_service import NewsBriefService


class ExecutiveReportService:
    def __init__(
        self,
        repository: ReviewRepository,
        intelligence_service: IntelligenceService,
        news_brief_service: NewsBriefService,
    ) -> None:
        self.repository = repository
        self.intelligence_service = intelligence_service
        self.news_brief_service = news_brief_service

    def build_report(
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

        product_themes = self.intelligence_service.get_product_themes(
            product_id=product_id,
            start_date=start_date,
            end_date=end_date,
            limit=5,
        )

        product_forecast = self.intelligence_service.get_product_forecast(
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

        key_strengths = self._build_key_strengths(
            product_themes=product_themes,
            competitor_benchmark=competitor_benchmark,
        )

        key_risks = self._build_key_risks(
            product_themes=product_themes,
            product_forecast=product_forecast,
            competitor_benchmark=competitor_benchmark,
        )

        competitor_takeaways = self._build_competitor_takeaways(
            competitor_benchmark=competitor_benchmark,
        )

        market_news_signals = self._build_market_news_signals(news_brief)

        recommended_actions = self._build_recommended_actions(
            product_summary=product_summary,
            key_risks=key_risks,
            competitor_takeaways=competitor_takeaways,
            news_brief=news_brief,
            has_competitor=bool(competitor_product_id),
        )

        executive_summary = self._build_executive_summary(
            product_summary=product_summary,
            key_strengths=key_strengths,
            key_risks=key_risks,
            competitor_takeaways=competitor_takeaways,
            market_news_signals=market_news_signals,
            has_competitor=bool(competitor_product_id),
        )

        sections = [
            {
                "title": "Decision Summary",
                "bullets": executive_summary,
            },
            {
                "title": "Customer Strength Signals",
                "bullets": key_strengths,
            },
            {
                "title": "Customer Risk Signals",
                "bullets": key_risks,
            },
            {
                "title": "Competitor Benchmark",
                "bullets": competitor_takeaways
                if competitor_product_id
                else [
                    "No competitor was selected. This report focuses on the selected product only."
                ],
            },
            {
                "title": "Trusted Market Signals",
                "bullets": market_news_signals
                or ["No high-relevance trusted market signal is available yet."],
            },
            {
                "title": "Recommended Actions",
                "bullets": recommended_actions,
            },
        ]

        evidence_links = self._build_evidence_links(
            product_id=product_id,
            news_brief=news_brief,
        )

        return {
            "report_title": "IRIP Executive Intelligence Report",
            "product_id": product_id,
            "competitor_product_id": competitor_product_id,
            "period": {
                "start_date": start_date,
                "end_date": end_date,
            },
            "confidence_note": self._confidence_note(product_summary, competitor_benchmark),
            "executive_summary": executive_summary,
            "key_strengths": key_strengths,
            "key_risks": key_risks,
            "competitor_takeaways": competitor_takeaways,
            "market_news_signals": market_news_signals,
            "recommended_actions": recommended_actions,
            "sections": sections,
            "evidence_links": evidence_links,
        }

    def _build_executive_summary(
        self,
        product_summary: dict,
        key_strengths: list[str],
        key_risks: list[str],
        competitor_takeaways: list[str],
        market_news_signals: list[str],
        has_competitor: bool,
    ) -> list[str]:
        review_count = int(product_summary.get("review_count") or 0)

        summary: list[str] = []

        if review_count == 0:
            summary.append(
                "No usable review evidence is available for the selected product yet. Import review data before drawing product conclusions."
            )
            return summary

        if review_count < 30:
            summary.append(
                "Current findings should be treated as early directional signals because the review volume is still low."
            )
        else:
            summary.append(
                "Review volume is strong enough to support more confident directional product insights."
            )

        if key_strengths:
            summary.append(f"Primary positive signal: {key_strengths[0]}")

        if key_risks:
            summary.append(f"Primary risk signal: {key_risks[0]}")

        if has_competitor and competitor_takeaways:
            summary.append(f"Competitive takeaway: {competitor_takeaways[0]}")
        elif not has_competitor:
            summary.append(
                "This is a product-only view. Add a competitor only when benchmark comparison is needed."
            )

        if market_news_signals:
            summary.append(
                "Trusted market signals are available and should be used as external context, not as a replacement for review evidence."
            )

        return self._dedupe(summary)[:6]

    def _build_key_strengths(
        self,
        product_themes,
        competitor_benchmark: dict | None,
    ) -> list[str]:
        strengths: list[str] = []

        for theme in product_themes.delight_themes[:3]:
            strengths.append(self._theme_to_product_signal(theme, signal_type="strength"))

        if competitor_benchmark:
            for item in competitor_benchmark.get("top_strengths", [])[:2]:
                interpretation = item.get("interpretation")
                if interpretation:
                    strengths.append(self._clean_sentence(interpretation))

        return self._dedupe(strengths) or [
            "No clear positive customer signal is strong enough yet. More review evidence is needed."
        ]

    def _build_key_risks(
        self,
        product_themes,
        product_forecast,
        competitor_benchmark: dict | None,
    ) -> list[str]:
        risks: list[str] = []

        for theme in product_themes.complaint_themes[:3]:
            risks.append(self._theme_to_product_signal(theme, signal_type="risk"))

        if getattr(product_forecast, "risk_forecast", None):
            risks.extend(self._clean_sentence(item) for item in product_forecast.risk_forecast[:2])

        if competitor_benchmark:
            for item in competitor_benchmark.get("top_weaknesses", [])[:2]:
                interpretation = item.get("interpretation")
                if interpretation:
                    risks.append(self._clean_sentence(interpretation))

        return self._dedupe(risks) or [
            "No clear customer risk signal is strong enough yet. Continue collecting review evidence."
        ]

    def _build_competitor_takeaways(
        self,
        competitor_benchmark: dict | None,
    ) -> list[str]:
        if not competitor_benchmark:
            return []

        takeaways: list[str] = []

        for item in competitor_benchmark.get("benchmark_aspects", [])[:5]:
            interpretation = item.get("interpretation")
            if interpretation:
                takeaways.append(self._clean_sentence(interpretation))

        return self._dedupe(takeaways) or [
            "Competitor benchmark is available, but comparable evidence is still too thin for a strong conclusion."
        ]

    def _build_market_news_signals(self, news_brief: dict) -> list[str]:
        signals: list[str] = []

        for item in news_brief.get("top_items", [])[:5]:
            title = item.get("title") or "Trusted news signal"
            why_it_matters = item.get("why_it_matters") or "Relevant to smartphone/OEM intelligence."
            signals.append(f"{title} — {why_it_matters}")

        return self._dedupe(signals)

    def _build_recommended_actions(
        self,
        product_summary: dict,
        key_risks: list[str],
        competitor_takeaways: list[str],
        news_brief: dict,
        has_competitor: bool,
    ) -> list[str]:
        review_count = int(product_summary.get("review_count") or 0)

        actions: list[str] = []

        if review_count < 30:
            actions.append(
                "Increase the real review sample before using these findings for roadmap or leadership decisions."
            )

        if key_risks:
            actions.append(
                "Validate the top risk signal with more reviews, evidence examples, and product-team review."
            )

        if has_competitor and competitor_takeaways:
            actions.append(
                "Use competitor gap findings to separate product issues from positioning, marketing, and after-sales issues."
            )
        elif not has_competitor:
            actions.append(
                "Keep this as a product-only analysis, or add a competitor only when benchmark comparison is required."
            )

        actions.append(
            "Preserve evidence links with every claim so the report remains auditable."
        )

        actions.extend(news_brief.get("recommended_actions", [])[:2])

        return self._dedupe(actions)[:6]

    def _product_review_bullets(self, product_summary: dict) -> list[str]:
        review_count = int(product_summary.get("review_count") or 0)
        average_rating = product_summary.get("average_rating")
        net_sentiment = product_summary.get("net_sentiment_score")
        top_aspects = product_summary.get("top_aspects", []) or []

        bullets = [
            f"Review volume available for analysis: {review_count}.",
            f"Average user rating: {average_rating if average_rating is not None else 'not available'}.",
            f"Overall sentiment direction: {net_sentiment if net_sentiment is not None else 'not available'}.",
        ]

        if top_aspects:
            aspects = ", ".join(self._labelize(item["aspect"]) for item in top_aspects[:5])
            bullets.append(f"Most discussed aspects: {aspects}.")
        else:
            bullets.append("No dominant aspect has emerged yet.")

        return bullets

    def _forecast_bullets(self, product_forecast) -> list[str]:
        bullets: list[str] = []

        if getattr(product_forecast, "forecast_summary", None):
            bullets.append(self._clean_sentence(product_forecast.forecast_summary))

        if getattr(product_forecast, "risk_forecast", None):
            bullets.extend(self._clean_sentence(item) for item in product_forecast.risk_forecast[:3])

        if getattr(product_forecast, "opportunity_forecast", None):
            bullets.extend(
                self._clean_sentence(item) for item in product_forecast.opportunity_forecast[:3]
            )

        return self._dedupe(bullets) or [
            "Forecast remains directional until review volume and time coverage improve."
        ]

    def _build_evidence_links(
        self,
        product_id: str,
        news_brief: dict,
    ) -> list[dict]:
        links = [
            {
                "label": f"Review evidence for {product_id}",
                "source_type": "review_evidence_endpoint",
                "source_name": "IRIP Review Evidence",
                "evidence_url": None,
                "reference_id": f"/products/{product_id}/evidence",
            }
        ]

        for item in news_brief.get("evidence_links", []):
            links.append(
                {
                    "label": item["title"],
                    "source_type": "trusted_news",
                    "source_name": item["source_name"],
                    "evidence_url": item["evidence_url"],
                    "reference_id": None,
                }
            )

        return links

    def _confidence_note(
        self,
        product_summary: dict,
        competitor_benchmark: dict | None,
    ) -> str:
        review_count = int(product_summary.get("review_count") or 0)

        if review_count == 0:
            return (
                "No confidence yet: the selected product has no usable review evidence in the selected scope."
            )

        if review_count < 30:
            return (
                "Directional confidence: review volume is low, so findings should be treated as early signals and validated with more data."
            )

        if competitor_benchmark and (
            competitor_benchmark.get("own_review_count", 0) < 30
            or competitor_benchmark.get("competitor_review_count", 0) < 30
        ):
            return (
                "Moderate confidence: product evidence exists, but competitor comparison volume is still limited."
            )

        return "Higher confidence: evidence volume is sufficient for stronger directional conclusions."

    def _theme_to_product_signal(self, theme, signal_type: str) -> str:
        aspect = self._labelize(getattr(theme, "aspect", None) or "customer experience")
        review_count = getattr(theme, "review_count", None)
        confidence = getattr(theme, "confidence", None)

        if signal_type == "risk":
            base = f"{aspect} is the clearest customer risk area to validate."
        else:
            base = f"{aspect} is the strongest positive customer signal."

        details: list[str] = []

        if review_count is not None:
            details.append(f"{review_count} mention(s)")

        if confidence is not None:
            details.append(f"confidence {round(float(confidence), 2)}")

        if details:
            return f"{base} Evidence strength: {', '.join(details)}."

        return base

    def _theme_to_bullet(self, theme) -> str:
        return self._theme_to_product_signal(theme, signal_type="strength")

    def _clean_sentence(self, value: str) -> str:
        text = " ".join(str(value).split()).strip()

        text = text.replace("our " + "product", "the selected product")
        text = text.replace("Our " + "product", "The selected product")

        if not text:
            return "Evidence is not strong enough yet."

        if text[-1] not in ".!?":
            text += "."

        return text

    def _labelize(self, value: str | None) -> str:
        if not value:
            return "Unknown"

        return " ".join(part.capitalize() for part in str(value).replace("_", " ").split())

    def _dedupe(self, items: list[str]) -> list[str]:
        seen = set()
        output: list[str] = []

        for item in items:
            cleaned = self._clean_sentence(item)
            key = cleaned.lower()

            if key in seen:
                continue

            seen.add(key)
            output.append(cleaned)

        return output