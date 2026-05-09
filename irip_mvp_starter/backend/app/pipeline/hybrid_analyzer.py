from __future__ import annotations

import os
from typing import Protocol

from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.llm import LlmReviewExtractionRequest, LlmReviewExtractionResponse
from app.schemas.review import (
    AspectSentiment,
    ReviewAnalysis,
    ReviewInput,
    Sentiment,
    SignalType,
)
from app.services.llm_service import LlmService


class AnalyzerProtocol(Protocol):
    def analyze(self, review: ReviewInput) -> ReviewAnalysis:
        ...


class HybridReviewAnalyzer:
    """Rules + LLM router for review analysis.

    Modes:
    - off: rules only
    - selective: rules first, LLM only for complex/low-confidence cases
    - always: LLM first for every review, fallback to rules on failure
    """

    def __init__(
        self,
        rule_analyzer: ReviewAnalyzer,
        llm_service: LlmService,
        mode_override: str | None = None,
    ) -> None:
        self.rule_analyzer = rule_analyzer
        self.llm_service = llm_service
        self.mode_override = mode_override    

    
    def analyze(self, review: ReviewInput) -> ReviewAnalysis:
        mode = self.mode_override or os.getenv("IRIP_LLM_MODE", "selective")
        rules_analysis = self.rule_analyzer.analyze(review)

        if mode == "off":
            rules_analysis.processing_notes.append(
                "Hybrid router: IRIP_LLM_MODE=off, used rules only."
            )
            return rules_analysis

        if mode == "always":
            return self._analyze_with_llm_or_fallback(
                review=review,
                fallback=rules_analysis,
                reason="IRIP_LLM_MODE=always",
            )

        if mode != "selective":
            rules_analysis.processing_notes.append(
                f"Hybrid router: unknown IRIP_LLM_MODE={mode}, used rules only."
            )
            return rules_analysis

        if self._should_route_to_llm(review, rules_analysis):
            return self._analyze_with_llm_or_fallback(
                review=review,
                fallback=rules_analysis,
                reason="selective routing triggered",
            )

        rules_analysis.processing_notes.append(
            "Hybrid router: selective mode kept rules output."
        )
        return rules_analysis

    def _should_route_to_llm(
        self,
        review: ReviewInput,
        rules_analysis: ReviewAnalysis,
    ) -> bool:
        if not self.llm_service.status().enabled:
            return False

        if rules_analysis.sarcasm_flag:
            return True

        if rules_analysis.contradiction_flag:
            return True

        if not rules_analysis.aspect_sentiments:
            return True

        avg_confidence = sum(
            aspect.confidence for aspect in rules_analysis.aspect_sentiments
        ) / len(rules_analysis.aspect_sentiments)

        if avg_confidence < 0.72:
            return True

        text = review.raw_text.lower()

        complex_markers = [
            "wah",
            "kya phone",
            "tawa",
            "haath garam",
            "10 min",
            "but",
            "lekin",
            "par",
            "overall",
            "mixed",
            "not bad",
            "not good",
            "theek",
            "aisa bhi nahi",
            "expected better",
        ]

        if any(marker in text for marker in complex_markers):
            return True

        if len(text.split()) >= 12 and any(token in text for token in ["hai", "nahi", "bahut"]):
            return True

        return False

    def _analyze_with_llm_or_fallback(
        self,
        review: ReviewInput,
        fallback: ReviewAnalysis,
        reason: str,
    ) -> ReviewAnalysis:
        status = self.llm_service.status()

        if not status.enabled:
            fallback.processing_notes.append(
                f"Hybrid router: wanted LLM for {reason}, but provider disabled: {status.reason}"
            )
            return fallback

        try:
            llm_response = self.llm_service.extract_review_intelligence(
                LlmReviewExtractionRequest(
                    product_id=review.product_id,
                    product_name=review.product_name,
                    raw_text=review.raw_text,
                    rating=review.rating,
                    source=review.source,
                )
            )

            return self._convert_llm_response_to_review_analysis(
                review=review,
                fallback=fallback,
                llm_response=llm_response,
                reason=reason,
            )
        except Exception as exc:
            fallback.processing_notes.append(
                f"Hybrid router: LLM failed for {reason}; used rules fallback. Error: {exc}"
            )
            return fallback

    def _convert_llm_response_to_review_analysis(
        self,
        review: ReviewInput,
        fallback: ReviewAnalysis,
        llm_response: LlmReviewExtractionResponse,
        reason: str,
    ) -> ReviewAnalysis:
        signal_types = self._convert_signal_types(
            llm_response=llm_response,
            fallback=fallback,
        )

        aspect_sentiments: list[AspectSentiment] = []

        for item in llm_response.aspects:
            sentiment = _to_sentiment(item.sentiment)
            if sentiment is None:
                continue

            aspect_sentiments.append(
                AspectSentiment(
                    aspect=item.aspect,
                    sub_aspect=item.sub_aspect,
                    sentiment=sentiment,
                    intensity=item.intensity,
                    confidence=item.confidence,
                    evidence_span=item.evidence_span,
                    provider=f"{llm_response.provider}:{llm_response.model}",
                )
            )

        if not aspect_sentiments:
            fallback.processing_notes.append(
                "Hybrid router: LLM returned no usable aspects; used rules fallback."
            )
            return fallback

        quality_score = max(
            0.0,
            min(
                1.0,
                round(
                    (fallback.quality_score * 0.45)
                    + (llm_response.confidence * 0.55),
                    3,
                ),
            ),
        )

        processing_notes = [
            *fallback.processing_notes,
            (
                f"Hybrid router: used LLM provider={llm_response.provider}, "
                f"model={llm_response.model}; reason={reason}."
            ),
        ]

        return ReviewAnalysis(
            review_id=fallback.review_id,
            product_id=review.product_id,
            clean_text=fallback.clean_text,
            language_profile=llm_response.language_profile or fallback.language_profile,
            signal_types=signal_types,
            aspect_sentiments=aspect_sentiments,
            quality_score=quality_score,
            contradiction_flag=llm_response.contradiction_flag or fallback.contradiction_flag,
            sarcasm_flag=llm_response.sarcasm_flag or fallback.sarcasm_flag,
            processing_notes=processing_notes,
        )

    def _convert_signal_types(
        self,
        llm_response: LlmReviewExtractionResponse,
        fallback: ReviewAnalysis,
    ) -> list[SignalType]:
        """Convert LLM booleans to whatever SignalType enum exists in this project.

        This avoids hard-coding enum names like DELIVERY/SERVICE if the current
        schema uses PRODUCT/DELIVERY_SERVICE/etc.
        """

        available = {item.value: item for item in SignalType}
        signal_types: list[SignalType] = []

        if llm_response.product_signal and "product" in available:
            signal_types.append(available["product"])

        if llm_response.delivery_signal:
            for candidate in ["delivery", "delivery_service", "service_delivery"]:
                if candidate in available:
                    signal_types.append(available[candidate])
                    break

        if llm_response.service_signal:
            for candidate in ["service", "after_sales", "delivery_service"]:
                if candidate in available:
                    signal_types.append(available[candidate])
                    break

        if signal_types:
            return signal_types

        return fallback.signal_types


def _to_sentiment(value: str) -> Sentiment | None:
    normalized = value.strip().lower()

    for item in Sentiment:
        if item.value == normalized:
            return item

    return None