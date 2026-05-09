from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import mean

from app.db.database import connect
from app.db.repository import ReviewRepository
from app.schemas.intelligence import (
    ForecastAspectItem,
    IntelligenceBriefResponse,
    ProductForecastResponse,
    ProductThemesResponse,
    ThemeEvidenceItem,
    ThemeItem,
)


NEGATIVE_SENTIMENTS = {"negative"}
POSITIVE_SENTIMENTS = {"positive"}

ACTIONABILITY_BY_ASPECT = {
    "battery": "hardware_or_software_optimization",
    "camera": "software_tuning_or_next_gen_camera",
    "display": "hardware_display_or_brightness_planning",
    "performance": "software_optimization",
    "heating": "thermal_optimization",
    "charging": "charger_accessory_or_charging_stack",
    "software": "software_fixable",
    "ui": "software_fixable",
    "connectivity": "modem_network_stack_or_operator_context",
    "audio": "hardware_or_software_audio_tuning",
    "design": "next_gen_industrial_design",
    "value": "pricing_or_marketing_position",
    "after_sales": "cx_process",
}


class IntelligenceService:
    def __init__(self, repository: ReviewRepository) -> None:
        self.repository = repository
        self.database_path = repository.database_path

    def get_product_themes(
        self,
        product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 5,
    ) -> ProductThemesResponse:
        grouped_rows = self._fetch_theme_groups(product_id, start_date, end_date)
        themes: list[ThemeItem] = []

        for row in grouped_rows:
            aspect = row["aspect"]
            sentiment = row["sentiment"]
            mention_count = int(row["mention_count"])
            avg_intensity = round(float(row["avg_intensity"] or 0.0), 3)
            avg_confidence = round(float(row["avg_confidence"] or 0.0), 3)
            theme_type = self._theme_type(sentiment)
            severity_score = self._severity_score(
                sentiment=sentiment,
                mention_count=mention_count,
                avg_intensity=avg_intensity,
                avg_confidence=avg_confidence,
            )

            evidence = self._fetch_theme_evidence(
                product_id=product_id,
                aspect=aspect,
                sentiment=sentiment,
                start_date=start_date,
                end_date=end_date,
                limit=3,
            )

            theme = ThemeItem(
                theme_id=f"{product_id}:{aspect}:{sentiment}",
                theme_name=self._theme_name(aspect, sentiment),
                aspect=aspect,
                theme_type=theme_type,
                sentiment=sentiment,
                mention_count=mention_count,
                avg_intensity=avg_intensity,
                avg_confidence=avg_confidence,
                severity_score=severity_score,
                actionability=self._actionability_for_aspect(aspect),
                interpretation=self._theme_interpretation(
                    aspect=aspect,
                    sentiment=sentiment,
                    mention_count=mention_count,
                    severity_score=severity_score,
                ),
                evidence=evidence,
            )
            themes.append(theme)

        complaint_themes = sorted(
            [theme for theme in themes if theme.theme_type == "complaint"],
            key=lambda theme: (theme.severity_score, theme.mention_count),
            reverse=True,
        )[:limit]

        delight_themes = sorted(
            [theme for theme in themes if theme.theme_type == "delight"],
            key=lambda theme: (theme.severity_score, theme.mention_count),
            reverse=True,
        )[:limit]

        watchlist_themes = sorted(
            [
                theme
                for theme in themes
                if theme.theme_type == "watchlist" or theme.avg_confidence < 0.72
            ],
            key=lambda theme: (theme.severity_score, theme.mention_count),
            reverse=True,
        )[:limit]

        return ProductThemesResponse(
            product_id=product_id,
            period={"start_date": start_date, "end_date": end_date},
            complaint_themes=complaint_themes,
            delight_themes=delight_themes,
            watchlist_themes=watchlist_themes,
        )

    def get_product_forecast(
        self,
        product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> ProductForecastResponse:
        """Build a directional, period-aware perception forecast.

        This is intentionally not positioned as a statistical ML forecast yet.
        It compares the selected/current period against the immediately preceding
        period of the same length where possible.
        """

        all_rows = self._fetch_aspect_rows_with_dates(product_id)

        if not all_rows:
            return ProductForecastResponse(
                product_id=product_id,
                forecast_basis="No dated aspect sentiment rows are available.",
                forecast_window="insufficient_data",
                overall_direction="uncertain",
                confidence_label="low",
                aspects=[],
                caveats=[
                    "No dated aspect sentiment rows are available for this product.",
                    "Import more dated reviews before using forecast signals.",
                ],
            )

        filtered_rows = self._filter_rows_by_period(all_rows, start_date, end_date)

        if not filtered_rows:
            return ProductForecastResponse(
                product_id=product_id,
                forecast_basis=(
                    "No aspect sentiment rows are available for the selected period."
                ),
                forecast_window=self._forecast_window_label(start_date, end_date),
                overall_direction="uncertain",
                confidence_label="low",
                aspects=[],
                caveats=[
                    "No review evidence exists inside the selected period.",
                    "Clear the period filter or choose a wider date range.",
                ],
            )

        current_start, current_end = self._resolve_current_period(
            rows=filtered_rows,
            start_date=start_date,
            end_date=end_date,
        )
        period_days = max((current_end - current_start).days + 1, 1)
        previous_end = current_start - timedelta(days=1)
        previous_start = previous_end - timedelta(days=period_days - 1)

        current_rows = [
            row
            for row in all_rows
            if current_start <= _parse_date(row["review_date"]) <= current_end
        ]
        previous_rows = [
            row
            for row in all_rows
            if previous_start <= _parse_date(row["review_date"]) <= previous_end
        ]

        current_by_aspect = self._group_rows_by_aspect(current_rows)
        previous_by_aspect = self._group_rows_by_aspect(previous_rows)

        aspects: list[ForecastAspectItem] = []

        for aspect in sorted(current_by_aspect):
            current_aspect_rows = current_by_aspect[aspect]
            previous_aspect_rows = previous_by_aspect.get(aspect, [])

            current_score = round(_average_sentiment_score(current_aspect_rows), 1)
            previous_score = (
                round(_average_sentiment_score(previous_aspect_rows), 1)
                if previous_aspect_rows
                else None
            )
            movement = (
                round(current_score - previous_score, 1)
                if previous_score is not None
                else None
            )
            avg_confidence = _average_confidence(current_aspect_rows + previous_aspect_rows)
            confidence_label = _forecast_confidence(
                current_mentions=len(current_aspect_rows),
                previous_mentions=len(previous_aspect_rows),
                avg_confidence=avg_confidence,
            )
            direction = _direction_from_movement(movement, current_score)

            aspects.append(
                ForecastAspectItem(
                    aspect=aspect,
                    current_score=current_score,
                    previous_score=previous_score,
                    movement=movement,
                    direction=direction,
                    current_mentions=len(current_aspect_rows),
                    previous_mentions=len(previous_aspect_rows),
                    confidence_label=confidence_label,
                    explanation=_forecast_explanation(
                        aspect=aspect,
                        direction=direction,
                        movement=movement,
                        current_mentions=len(current_aspect_rows),
                        previous_mentions=len(previous_aspect_rows),
                        confidence_label=confidence_label,
                    ),
                )
            )

        aspects = sorted(
            aspects,
            key=lambda item: (
                item.confidence_label == "high",
                item.confidence_label == "medium",
                item.current_mentions,
                abs(item.current_score),
            ),
            reverse=True,
        )

        return ProductForecastResponse(
            product_id=product_id,
            forecast_basis=(
                "Directional forecast based on selected/current-period aspect sentiment "
                "compared with the immediately preceding period of the same length."
            ),
            forecast_window=(
                f"{current_start.isoformat()} to {current_end.isoformat()} "
                f"vs {previous_start.isoformat()} to {previous_end.isoformat()}"
            ),
            overall_direction=_overall_direction(aspects),
            confidence_label=_overall_confidence(aspects),
            aspects=aspects,
            caveats=[
                "This is a directional perception signal, not a statistical demand forecast.",
                "Low review volume should be treated as directional only.",
                "Provider confidence and evidence volume should be checked before product decisions.",
            ],
        )

    def get_intelligence_brief(
        self,
        product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> IntelligenceBriefResponse:
        summary = self.repository.get_product_summary(product_id, start_date, end_date)
        aspects = self.repository.get_aspect_summary(product_id, start_date, end_date)
        themes = self.get_product_themes(product_id, start_date, end_date, limit=3)
        forecast = self.get_product_forecast(product_id, start_date, end_date)

        top_positive_aspects = [
            item for item in aspects if item["aspect_score"] > 0 and item["mentions"] > 0
        ]
        top_negative_aspects = [
            item for item in aspects if item["aspect_score"] < 0 and item["mentions"] > 0
        ]

        top_positive_aspects = sorted(
            top_positive_aspects,
            key=lambda item: (item["aspect_score"], item["mentions"]),
            reverse=True,
        )[:3]
        top_negative_aspects = sorted(
            top_negative_aspects,
            key=lambda item: (item["aspect_score"], -item["mentions"]),
        )[:3]

        executive_summary = self._build_executive_summary(
            product_id=product_id,
            summary=summary,
            top_positive_aspects=top_positive_aspects,
            top_negative_aspects=top_negative_aspects,
            forecast=forecast,
        )

        top_strengths = [
            f"{item['aspect']} is currently a strength with score {round(item['aspect_score'], 1)} from {item['mentions']} mention(s)."
            for item in top_positive_aspects
        ]

        top_risks = [
            f"{item['aspect']} is currently a risk with score {round(item['aspect_score'], 1)} from {item['mentions']} mention(s)."
            for item in top_negative_aspects
        ]

        recommended_actions = self._recommended_actions(themes)

        return IntelligenceBriefResponse(
            product_id=product_id,
            period={"start_date": start_date, "end_date": end_date},
            executive_summary=executive_summary,
            top_strengths=top_strengths,
            top_risks=top_risks,
            recommended_actions=recommended_actions,
            evidence_note="Every theme is linked to representative source reviews in /products/{product_id}/themes.",
            confidence_note=(
                "This is an MVP deterministic brief. Treat low-volume findings as directional until more reviews are imported."
            ),
        )

    def _fetch_theme_groups(
        self,
        product_id: str,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict]:
        where_sql, params = self._date_filter_sql(product_id, start_date, end_date)
        sql = f"""
            SELECT
                a.aspect,
                a.sentiment,
                COUNT(*) AS mention_count,
                AVG(a.intensity) AS avg_intensity,
                AVG(a.confidence) AS avg_confidence
            FROM aspect_sentiments a
            JOIN reviews_raw r ON r.review_id = a.review_id
            WHERE {where_sql}
            GROUP BY a.aspect, a.sentiment
            ORDER BY mention_count DESC, avg_confidence DESC
        """
        with connect(self.database_path) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def _fetch_theme_evidence(
        self,
        product_id: str,
        aspect: str,
        sentiment: str,
        start_date: str | None,
        end_date: str | None,
        limit: int,
    ) -> list[ThemeEvidenceItem]:
        where_sql, params = self._date_filter_sql(product_id, start_date, end_date)
        params = [*params, aspect, sentiment, limit]
        sql = f"""
            SELECT
                r.review_id,
                r.source,
                r.rating,
                r.review_date,
                r.raw_text,
                a.evidence_span,
                a.confidence,
                a.provider
            FROM aspect_sentiments a
            JOIN reviews_raw r ON r.review_id = a.review_id
            WHERE {where_sql}
              AND a.aspect = ?
              AND a.sentiment = ?
            ORDER BY a.confidence DESC, a.intensity DESC
            LIMIT ?
        """
        with connect(self.database_path) as connection:
            rows = connection.execute(sql, params).fetchall()

        return [
            ThemeEvidenceItem(
                review_id=row["review_id"],
                source=row["source"],
                rating=row["rating"],
                review_date=row["review_date"],
                raw_text=row["raw_text"],
                evidence_span=row["evidence_span"],
                confidence=row["confidence"],
                provider=row["provider"],
            )
            for row in rows
        ]

    def _fetch_aspect_rows_with_dates(self, product_id: str) -> list[dict]:
        sql = """
            SELECT
                a.aspect,
                a.sentiment,
                a.intensity,
                a.confidence,
                r.review_date
            FROM aspect_sentiments a
            JOIN reviews_raw r ON r.review_id = a.review_id
            WHERE a.product_id = ?
              AND r.review_date IS NOT NULL
              AND r.review_date != ''
        """
        with connect(self.database_path) as connection:
            rows = connection.execute(sql, (product_id,)).fetchall()
        return [dict(row) for row in rows]

    def _date_filter_sql(
        self,
        product_id: str,
        start_date: str | None,
        end_date: str | None,
    ) -> tuple[str, list[object]]:
        clauses = ["a.product_id = ?"]
        params: list[object] = [product_id]
        if start_date:
            clauses.append("r.review_date >= ?")
            params.append(start_date)
        if end_date:
            clauses.append("r.review_date <= ?")
            params.append(end_date)
        return " AND ".join(clauses), params

    def _filter_rows_by_period(
        self,
        rows: list[dict],
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict]:
        filtered = rows

        if start_date:
            filtered = [
                row for row in filtered if _parse_date(row["review_date"]) >= _parse_date(start_date)
            ]

        if end_date:
            filtered = [
                row for row in filtered if _parse_date(row["review_date"]) <= _parse_date(end_date)
            ]

        return filtered

    def _resolve_current_period(
        self,
        rows: list[dict],
        start_date: str | None,
        end_date: str | None,
    ) -> tuple[date, date]:
        row_dates = [_parse_date(row["review_date"]) for row in rows]
        resolved_start = _parse_date(start_date) if start_date else min(row_dates)
        resolved_end = _parse_date(end_date) if end_date else max(row_dates)

        if resolved_start > resolved_end:
            resolved_start, resolved_end = resolved_end, resolved_start

        return resolved_start, resolved_end

    def _forecast_window_label(self, start_date: str | None, end_date: str | None) -> str:
        if start_date or end_date:
            return f"{start_date or 'beginning'} to {end_date or 'latest'}"
        return "all_available_dated_reviews"

    def _group_rows_by_aspect(self, rows: list[dict]) -> dict[str, list[dict]]:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            grouped[row["aspect"]].append(row)
        return grouped

    def _theme_type(self, sentiment: str) -> str:
        if sentiment in NEGATIVE_SENTIMENTS:
            return "complaint"
        if sentiment in POSITIVE_SENTIMENTS:
            return "delight"
        return "watchlist"

    def _theme_name(self, aspect: str, sentiment: str) -> str:
        if sentiment == "negative":
            return f"{aspect.replace('_', ' ').title()} complaint theme"
        if sentiment == "positive":
            return f"{aspect.replace('_', ' ').title()} delight theme"
        return f"{aspect.replace('_', ' ').title()} watchlist theme"

    def _severity_score(
        self,
        sentiment: str,
        mention_count: int,
        avg_intensity: float,
        avg_confidence: float,
    ) -> float:
        base = mention_count * avg_intensity * avg_confidence
        if sentiment == "negative":
            base *= 1.25
        return round(base, 3)

    def _actionability_for_aspect(self, aspect: str) -> str:
        return ACTIONABILITY_BY_ASPECT.get(aspect, "analyst_review_required")

    def _theme_interpretation(
        self,
        aspect: str,
        sentiment: str,
        mention_count: int,
        severity_score: float,
    ) -> str:
        if sentiment == "negative":
            return (
                f"{aspect} is appearing as a complaint theme in {mention_count} mention(s). "
                f"Severity score is {severity_score}; prioritize if this grows with more data."
            )
        if sentiment == "positive":
            return (
                f"{aspect} is appearing as a delight driver in {mention_count} mention(s). "
                "This may be useful for product positioning or marketing evidence."
            )
        return (
            f"{aspect} has mixed/neutral signals. Keep it on watchlist until more evidence is available."
        )

    def _build_executive_summary(
        self,
        product_id: str,
        summary: dict,
        top_positive_aspects: list[dict],
        top_negative_aspects: list[dict],
        forecast: ProductForecastResponse,
    ) -> str:
        review_count = summary.get("review_count", 0)
        avg_rating = summary.get("average_rating")
        net_sentiment = summary.get("net_sentiment_score", 0)

        strength_text = (
            f"Top strength is {top_positive_aspects[0]['aspect']}."
            if top_positive_aspects
            else "No strong positive aspect has enough evidence yet."
        )
        risk_text = (
            f"Top risk is {top_negative_aspects[0]['aspect']}."
            if top_negative_aspects
            else "No major negative aspect has enough evidence yet."
        )

        return (
            f"{product_id} currently has {review_count} processed review(s)"
            f"{f' with average rating {round(avg_rating, 2)}' if avg_rating is not None else ''}. "
            f"The net sentiment score is {round(net_sentiment, 1)}. "
            f"{strength_text} {risk_text} "
            f"Near-term perception direction is {forecast.overall_direction} with {forecast.confidence_label} confidence."
        )

    def _recommended_actions(self, themes: ProductThemesResponse) -> list[str]:
        actions: list[str] = []

        for theme in themes.complaint_themes[:3]:
            actions.append(
                f"Investigate {theme.aspect}: {theme.actionability}. Evidence count: {theme.mention_count}."
            )

        for theme in themes.delight_themes[:2]:
            actions.append(
                f"Use {theme.aspect} as a potential positioning proof point if evidence volume increases."
            )

        if not actions:
            actions.append("Import more review data before making product decisions.")

        return actions


def _parse_date(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _sentiment_value(row: dict) -> float:
    intensity = float(row["intensity"] or 0.0)
    sentiment = row["sentiment"]
    if sentiment == "positive":
        return intensity * 100
    if sentiment == "negative":
        return -intensity * 100
    return 0.0


def _average_sentiment_score(rows: list[dict]) -> float:
    if not rows:
        return 0.0
    return mean(_sentiment_value(row) for row in rows)


def _average_confidence(rows: list[dict]) -> float:
    values = [float(row["confidence"]) for row in rows if row.get("confidence") is not None]
    if not values:
        return 0.0
    return mean(values)


def _direction_from_movement(movement: float | None, current_score: float) -> str:
    if movement is None:
        if current_score >= 15:
            return "likely_positive_but_unproven"
        if current_score <= -15:
            return "likely_negative_but_unproven"
        return "uncertain"

    if movement >= 8:
        return "likely_improving"
    if movement <= -8:
        return "likely_declining"
    return "likely_stable"


def _forecast_confidence(
    current_mentions: int,
    previous_mentions: int,
    avg_confidence: float,
) -> str:
    total = current_mentions + previous_mentions
    if total >= 50 and avg_confidence >= 0.82:
        return "high"
    if total >= 10 and avg_confidence >= 0.7:
        return "medium"
    return "low"


def _forecast_explanation(
    aspect: str,
    direction: str,
    movement: float | None,
    current_mentions: int,
    previous_mentions: int,
    confidence_label: str,
) -> str:
    if movement is None:
        return (
            f"{aspect} has {current_mentions} current-period mention(s) but no previous-period baseline. "
            f"Direction is {direction}; confidence is {confidence_label}."
        )

    return (
        f"{aspect} moved by {movement} points versus the previous period "
        f"({previous_mentions} previous mention(s), {current_mentions} current mention(s)). "
        f"Direction is {direction}; confidence is {confidence_label}."
    )


def _overall_direction(aspects: list[ForecastAspectItem]) -> str:
    if not aspects:
        return "uncertain"

    improving = sum(1 for item in aspects if "improving" in item.direction)
    declining = sum(1 for item in aspects if "declining" in item.direction)

    if declining > improving:
        return "likely_declining"
    if improving > declining:
        return "likely_improving"
    return "likely_stable_or_mixed"


def _overall_confidence(aspects: list[ForecastAspectItem]) -> str:
    if not aspects:
        return "low"

    labels = [item.confidence_label for item in aspects]
    if labels.count("high") >= max(1, len(labels) // 2):
        return "high"
    if labels.count("medium") + labels.count("high") >= max(1, len(labels) // 2):
        return "medium"
    return "low"