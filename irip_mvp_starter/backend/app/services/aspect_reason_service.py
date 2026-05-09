from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from typing import Any

from app.db.repository import ReviewRepository
from app.services.llm_service import LlmService


class AspectReasonService:
    """Build evidence-backed aspect reason cards for the Sentiment tab.

    Product purpose:
    - Tell the user WHY an aspect is positive/negative/mixed.
    - Example: "Users mostly complain about fast battery drain."
    - Avoid generic text like "Users mostly like camera quality" when evidence
      only says "camera mast" without a clear sub-feature.

    Architecture:
    - Uses stored aspect_sentiments and evidence spans.
    - Uses Gemini when available for deeper reason extraction.
    - Falls back to deterministic smartphone rules when Gemini is unavailable,
      disabled, fails, or returns invalid JSON.

    This service does not fake aspects.
    It only creates cards for aspects already present in aspect_summary.
    """

    DEFAULT_THEME_BY_ASPECT: dict[str, str] = {
        "battery": "battery backup",
        "camera": "camera feedback",
        "display": "display feedback",
        "audio": "audio feedback",
        "performance": "performance",
        "heating": "heating",
        "charging": "charging experience",
        "software": "software experience",
        "connectivity": "network/connectivity",
        "network": "network signal",
        "design": "design/build",
        "value": "value for money",
        "after_sales": "after-sales service",
        "service": "service experience",
    }

    BROAD_THEME_BY_ASPECT: dict[str, str] = {
        "battery": "battery",
        "camera": "camera",
        "display": "display",
        "audio": "audio",
        "performance": "performance",
        "heating": "heating",
        "charging": "charging",
        "software": "software",
        "connectivity": "network/connectivity",
        "network": "network",
        "design": "design/build",
        "value": "price/value",
        "after_sales": "service",
        "service": "service",
    }

    GENERIC_THEME_LABELS = {
        "camera quality",
        "display quality",
        "speaker quality",
        "audio quality",
        "battery quality",
        "performance",
        "software experience",
        "display feedback",
        "camera feedback",
        "audio feedback",
        "battery feedback",
        "product quality",
        "overall quality",
    }

    ASPECT_THEME_RULES: dict[str, list[tuple[str, list[str]]]] = {
        "battery": [
            ("battery backup", ["backup", "back up", "battery life", "backup bekar"]),
            ("fast battery drain", ["drain", "draining", "jaldi khatam", "khatam", "battery khatam"]),
            ("charging experience", ["charge", "charging", "charger", "fast charge", "slow charge"]),
            ("battery heating", ["heat", "heating", "garam", "warm", "tawa"]),
        ],
        "camera": [
            ("photo clarity", ["clarity", "clear photo", "photo clear", "sharp", "detail", "details"]),
            ("photo quality", ["photo", "photos", "pic", "pics", "picture", "image"]),
            ("selfie camera", ["selfie", "front camera", "front"]),
            ("low-light camera", ["night", "low light", "dark"]),
            ("video recording", ["video", "recording"]),
            ("camera value for price", ["price", "budget", "range", "paisa vasool", "worth"]),
        ],
        "display": [
            ("display brightness", ["brightness", "bright", "sunlight", "dhoop"]),
            ("display colours", ["color", "colour", "colors", "colours"]),
            ("screen clarity", ["screen clarity", "display clarity", "clear screen"]),
            ("touch response", ["touch", "smooth", "refresh", "hz"]),
            ("display look", ["looks", "look", "premium", "beautiful"]),
        ],
        "audio": [
            ("low speaker volume", ["volume low", "low volume", "kam volume", "sound low", "speaker low"]),
            ("speaker loudness", ["loud", "volume", "speaker volume"]),
            ("speaker quality", ["speaker", "sound", "audio"]),
            ("call audio", ["call", "voice", "mic", "microphone"]),
            ("earphone support", ["earphone", "headphone", "jack", "3.5"]),
        ],
        "performance": [
            ("lag/performance issues", ["lag", "hang", "stuck", "slow"]),
            ("speed/smoothness", ["speed", "fast", "smooth"]),
            ("gaming performance", ["game", "gaming", "fps", "bgmi", "free fire", "cod"]),
            ("RAM/app switching", ["ram", "multitask", "multitasking", "app switch"]),
        ],
        "heating": [
            ("heating", ["heat", "heating", "hot", "garam", "warm", "tawa", "temperature"]),
            ("heating while gaming", ["gaming heat", "game heat", "gaming heating", "game garam"]),
        ],
        "charging": [
            ("charging speed", ["fast charge", "slow charge", "charging speed"]),
            ("charger/adapter", ["adapter", "charger"]),
            ("charging experience", ["charge", "charging"]),
        ],
        "software": [
            ("software/UI", ["ui", "software", "os", "interface", "launcher"]),
            ("software updates", ["update", "updates", "security patch"]),
            ("software bugs", ["bug", "bugs", "crash", "crashing", "freeze"]),
            ("ads/bloatware", ["ads", "ad", "bloatware", "preinstalled"]),
        ],
        "connectivity": [
            ("network signal", ["network", "signal", "5g", "4g", "internet"]),
            ("Wi-Fi/connectivity", ["wifi", "wi-fi"]),
            ("call quality", ["call", "voice", "calling", "call drop"]),
        ],
        "network": [
            ("network signal", ["network", "signal", "5g", "4g", "internet"]),
            ("Wi-Fi/connectivity", ["wifi", "wi-fi"]),
            ("call quality", ["call", "voice", "calling", "call drop"]),
        ],
        "design": [
            ("design/look", ["design", "look", "looks", "premium", "style"]),
            ("build quality", ["build", "body", "durable"]),
            ("weight/hand feel", ["weight", "heavy", "light", "hand"]),
        ],
        "value": [
            ("value for money", ["value", "paisa vasool", "worth", "price", "budget", "sasta", "mehnga"]),
        ],
        "after_sales": [
            ("after-sales service", ["service", "warranty", "repair", "center", "centre"]),
        ],
        "service": [
            ("service experience", ["service", "warranty", "repair", "center", "centre"]),
        ],
    }

    POSITIVE_WORDS = {
        "good",
        "great",
        "excellent",
        "best",
        "nice",
        "mast",
        "zabardast",
        "awesome",
        "super",
        "dhansu",
        "paisa vasool",
        "smooth",
        "clear",
        "fast",
        "amazing",
        "perfect",
        "love",
        "liked",
        "premium",
        "bright",
    }

    NEGATIVE_WORDS = {
        "bad",
        "poor",
        "worst",
        "bekar",
        "bakwas",
        "faltu",
        "waste",
        "problem",
        "issue",
        "slow",
        "lag",
        "hang",
        "heat",
        "heating",
        "garam",
        "drain",
        "khatam",
        "disappointed",
        "not good",
        "low",
    }

    STOPWORDS = {
        "this",
        "that",
        "with",
        "phone",
        "mobile",
        "very",
        "hai",
        "but",
        "and",
        "the",
        "for",
        "not",
        "all",
        "can",
        "are",
        "was",
        "its",
        "use",
        "used",
        "also",
        "one",
        "two",
        "more",
        "from",
        "have",
        "has",
        "had",
        "user",
        "users",
        "review",
        "reviews",
    }

    ALLOWED_REACTIONS = {"positive", "negative", "mixed", "neutral", "unclear"}

    _MEMORY_CACHE: dict[str, list[dict]] = {}

    def __init__(self, repository: ReviewRepository, llm_service: LlmService | None = None) -> None:
        self.repository = repository
        self.llm_service = llm_service or LlmService()

    def build_cards(
        self,
        product_id: str,
        aspect_summary: list[dict],
        start_date: str | None = None,
        end_date: str | None = None,
        limit_per_aspect: int = 12,
    ) -> list[dict]:
        aspect_items = [item for item in aspect_summary if str(item.get("aspect") or "").strip()]

        evidence_by_aspect: dict[str, list[dict]] = {}
        fallback_cards: list[dict] = []

        for aspect_item in aspect_items:
            aspect = str(aspect_item.get("aspect") or "").strip().lower()
            evidence_rows = self.repository.list_evidence(
                product_id=product_id,
                aspect=aspect,
                sentiment=None,
                limit=limit_per_aspect,
                start_date=start_date,
                end_date=end_date,
            )
            evidence_by_aspect[aspect] = evidence_rows
            fallback_cards.append(
                self._build_rule_card(
                    aspect_item=aspect_item,
                    evidence_rows=evidence_rows,
                )
            )

        if not fallback_cards:
            return []

        llm_cards = self._build_llm_cards_if_available(
            product_id=product_id,
            aspect_items=aspect_items,
            evidence_by_aspect=evidence_by_aspect,
            fallback_cards=fallback_cards,
        )

        if not llm_cards:
            return fallback_cards

        fallback_by_aspect = {card["aspect"]: card for card in fallback_cards}
        merged_cards = []

        for card in fallback_cards:
            aspect = card["aspect"]
            merged_cards.append(llm_cards.get(aspect) or fallback_by_aspect[aspect])

        return merged_cards

    def _build_llm_cards_if_available(
        self,
        product_id: str,
        aspect_items: list[dict],
        evidence_by_aspect: dict[str, list[dict]],
        fallback_cards: list[dict],
    ) -> dict[str, dict]:
        if not self._should_use_gemini():
            return {}

        prompt = self._build_gemini_prompt(
            product_id=product_id,
            aspect_items=aspect_items,
            evidence_by_aspect=evidence_by_aspect,
            fallback_cards=fallback_cards,
        )

        cache_key = self._cache_key(prompt)
        if cache_key in self._MEMORY_CACHE:
            cached_cards = self._MEMORY_CACHE[cache_key]
        else:
            try:
                raw_text = self.llm_service._call_gemini(prompt)
                parsed = self._extract_json(raw_text)
                cached_cards = parsed.get("aspect_reason_cards", [])
                if not isinstance(cached_cards, list):
                    cached_cards = []
                self._MEMORY_CACHE[cache_key] = cached_cards
            except Exception:
                return {}

        fallback_by_aspect = {card["aspect"]: card for card in fallback_cards}
        evidence_text_by_aspect = {
            aspect: self._normalized_text(
                [
                    str(row.get("evidence_span") or row.get("clean_text") or row.get("raw_text") or "")
                    for row in rows
                ]
            )
            for aspect, rows in evidence_by_aspect.items()
        }

        validated: dict[str, dict] = {}

        for raw_card in cached_cards:
            if not isinstance(raw_card, dict):
                continue

            aspect = str(raw_card.get("aspect") or "").strip().lower()
            if aspect not in fallback_by_aspect:
                continue

            validated_card = self._validate_llm_card(
                raw_card=raw_card,
                fallback_card=fallback_by_aspect[aspect],
                evidence_text=evidence_text_by_aspect.get(aspect, ""),
            )
            validated[aspect] = validated_card

        return validated

    def _should_use_gemini(self) -> bool:
        if os.getenv("PYTEST_CURRENT_TEST") and os.getenv("IRIP_ENABLE_LLM_IN_TESTS") != "1":
            return False

        mode = os.getenv("IRIP_ASPECT_REASON_MODE", "auto").strip().lower()

        if mode in {"off", "rules", "rule", "false", "0"}:
            return False

        try:
            status = self.llm_service.status()
        except Exception:
            return False

        if not status.enabled:
            return False

        return mode in {"auto", "gemini", "llm", "always", "on", "true", "1"}

    def _build_gemini_prompt(
        self,
        product_id: str,
        aspect_items: list[dict],
        evidence_by_aspect: dict[str, list[dict]],
        fallback_cards: list[dict],
    ) -> str:
        fallback_by_aspect = {card["aspect"]: card for card in fallback_cards}
        aspects_payload = []

        for item in aspect_items:
            aspect = str(item.get("aspect") or "").strip().lower()
            fallback = fallback_by_aspect.get(aspect, {})
            evidence_rows = evidence_by_aspect.get(aspect, [])

            evidence = []
            for row in evidence_rows[:8]:
                evidence_text = str(
                    row.get("evidence_span")
                    or row.get("clean_text")
                    or row.get("raw_text")
                    or ""
                ).strip()

                if not evidence_text:
                    continue

                evidence.append(
                    {
                        "sentiment": row.get("sentiment"),
                        "evidence_span": evidence_text[:260],
                        "rating": row.get("rating"),
                    }
                )

            aspects_payload.append(
                {
                    "aspect": aspect,
                    "mention_count": int(item.get("mentions") or 0),
                    "positive_count": int(item.get("positive_count") or 0),
                    "negative_count": int(item.get("negative_count") or 0),
                    "neutral_count": int(item.get("neutral_count") or 0),
                    "fallback_one_liner": fallback.get("one_liner"),
                    "fallback_terms": fallback.get("evidence_terms", []),
                    "evidence": evidence,
                }
            )

        payload = {
            "product_id": product_id,
            "task": "Generate specific aspect reason cards from review evidence only.",
            "aspects": aspects_payload,
        }

        return f"""
You are an Indian smartphone review intelligence analyst for an OEM.

Your job:
For each aspect, explain WHAT EXACTLY users are mentioning inside that aspect.

Strict rules:
- Use ONLY the evidence provided below.
- Do NOT invent new aspects.
- Do NOT invent sub-features not supported by evidence.
- Do NOT simply repeat the aspect name.
- Avoid lazy/generic phrases like "camera quality", "display quality", "speaker quality" unless evidence clearly contains no more specific reason.
- If the evidence only says broad praise/complaint like "camera mast" or "camera good", say:
  "Users are giving broad positive camera feedback, but no specific camera sub-feature is clear yet."
- If the evidence clearly says a specific reason, say it directly:
  "Users mostly complain about fast battery drain."
  "Users mostly complain about low speaker volume."
  "Users mostly like display brightness."
  "Users mostly like selfie clarity."
  "Users mostly mention lag while gaming."
- Keep one_liner simple, natural, and max 18 words.
- Do NOT mention "evidence provided", "according to reviews", or "based on data".
- Do NOT use only slang words like "mast" or "bekar" as the reason theme.
- Output valid JSON only. No markdown.

Return schema:
{{
  "aspect_reason_cards": [
    {{
      "aspect": "same aspect from input",
      "reaction": "positive | negative | mixed | neutral | unclear",
      "one_liner": "Users mostly complain about fast battery drain.",
      "reason_theme": "fast battery drain",
      "specificity": "specific | broad",
      "evidence_terms": ["battery drain", "backup"],
      "confidence_label": "early | directional | stronger"
    }}
  ]
}}

Input evidence:
{json.dumps(payload, ensure_ascii=False, indent=2)}
""".strip()

    def _validate_llm_card(self, raw_card: dict, fallback_card: dict, evidence_text: str) -> dict:
        aspect = fallback_card["aspect"]
        reaction = str(raw_card.get("reaction") or fallback_card.get("reaction") or "unclear").strip().lower()

        if reaction not in self.ALLOWED_REACTIONS:
            reaction = fallback_card.get("reaction") or "unclear"

        reason_theme = str(raw_card.get("reason_theme") or "").strip()
        specificity = str(raw_card.get("specificity") or "").strip().lower()

        one_liner = str(raw_card.get("one_liner") or "").strip()
        if not one_liner:
            one_liner = fallback_card["one_liner"]

        one_liner = self._clean_one_liner(one_liner, fallback_card["one_liner"])

        evidence_terms = raw_card.get("evidence_terms")
        if not isinstance(evidence_terms, list):
            evidence_terms = []

        evidence_terms = [str(term).strip() for term in evidence_terms if str(term).strip()][:6]

        if not evidence_terms:
            evidence_terms = fallback_card.get("evidence_terms", [])[:6]

        one_liner = self._guard_against_generic_liner(
            aspect=aspect,
            reaction=reaction,
            one_liner=one_liner,
            reason_theme=reason_theme,
            specificity=specificity,
            evidence_terms=evidence_terms,
            evidence_text=evidence_text,
        )

        confidence_label = str(
            raw_card.get("confidence_label")
            or fallback_card.get("confidence_label")
            or "early"
        ).strip().lower()

        if confidence_label not in {"no_evidence", "early", "directional", "stronger"}:
            confidence_label = fallback_card.get("confidence_label") or "early"

        return {
            **fallback_card,
            "reaction": reaction,
            "one_liner": one_liner,
            "evidence_terms": evidence_terms,
            "confidence_label": confidence_label,
            "llm_generated": True,
            "reason_source": "gemini",
        }

    def _guard_against_generic_liner(
        self,
        aspect: str,
        reaction: str,
        one_liner: str,
        reason_theme: str,
        specificity: str,
        evidence_terms: list[str],
        evidence_text: str,
    ) -> str:
        lowered_liner = one_liner.lower()
        lowered_theme = reason_theme.lower().strip()
        term_text = " ".join(evidence_terms).lower()
        aspect_label = self.BROAD_THEME_BY_ASPECT.get(aspect, aspect).strip()

        detected_specific = self._first_specific_theme_from_text(aspect, evidence_text)

        if detected_specific:
            return self._specific_one_liner(reaction=reaction, theme=detected_specific)

        generic_theme = (
            lowered_theme in self.GENERIC_THEME_LABELS
            or any(label in lowered_liner for label in self.GENERIC_THEME_LABELS)
            or specificity == "broad"
        )

        has_only_sentiment_terms = self._has_only_sentiment_or_aspect_terms(
            aspect=aspect,
            term_text=term_text,
            evidence_text=evidence_text,
        )

        if generic_theme or has_only_sentiment_terms:
            return self._broad_one_liner(aspect=aspect_label, reaction=reaction)

        return one_liner

    def _first_specific_theme_from_text(self, aspect: str, evidence_text: str) -> str | None:
        if not evidence_text:
            return None

        for theme_label, triggers in self.ASPECT_THEME_RULES.get(aspect, []):
            for trigger in triggers:
                if trigger.lower() in evidence_text:
                    return theme_label

        return None

    def _specific_one_liner(self, reaction: str, theme: str) -> str:
        if reaction == "positive":
            return f"Users mostly like {theme}."
        if reaction == "negative":
            return f"Users mostly complain about {theme}."
        if reaction == "mixed":
            return f"Users are split on {theme}."
        if reaction == "neutral":
            return f"Users mention {theme}, but sentiment is mostly neutral."
        return f"Users mention {theme}, but sentiment is not clear yet."

    def _broad_one_liner(self, aspect: str, reaction: str) -> str:
        if reaction == "positive":
            return f"Users are giving broad positive {aspect} feedback; no specific sub-feature is clear yet."
        if reaction == "negative":
            return f"Users are giving broad negative {aspect} feedback; no specific sub-feature is clear yet."
        if reaction == "mixed":
            return f"Users are split on {aspect}, but no specific sub-feature is clear yet."
        if reaction == "neutral":
            return f"Users mention {aspect}, but no specific sub-feature is clear yet."
        return f"Users mention {aspect}, but the reason is not clear yet."

    def _has_only_sentiment_or_aspect_terms(self, aspect: str, term_text: str, evidence_text: str) -> bool:
        normalized_terms = set(re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", term_text.lower()))
        normalized_evidence = set(re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", evidence_text.lower()))

        allowed_broad = {aspect}
        allowed_broad.update(aspect.replace("_", " ").split())
        allowed_broad.update(self.POSITIVE_WORDS)
        allowed_broad.update(self.NEGATIVE_WORDS)

        if normalized_terms and normalized_terms.issubset(allowed_broad):
            return True

        evidence_without_noise = normalized_evidence - self.STOPWORDS
        if evidence_without_noise and evidence_without_noise.issubset(allowed_broad):
            return True

        return False

    def _clean_one_liner(self, one_liner: str, fallback: str) -> str:
        cleaned = re.sub(r"\s+", " ", one_liner).strip()

        if len(cleaned) > 150:
            cleaned = cleaned[:147].rstrip() + "..."

        banned_fragments = [
            "based on the evidence provided",
            "the evidence suggests",
            "according to the reviews",
            "as per the reviews",
            "based on data",
        ]

        lowered = cleaned.lower()
        if any(fragment in lowered for fragment in banned_fragments):
            return fallback

        if not cleaned.endswith((".", "!", "?")):
            cleaned += "."

        return cleaned

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

    def _build_rule_card(self, aspect_item: dict, evidence_rows: list[dict]) -> dict:
        aspect = str(aspect_item.get("aspect") or "unknown").strip().lower()
        mentions = int(aspect_item.get("mentions") or 0)
        positive_count = int(aspect_item.get("positive_count") or 0)
        negative_count = int(aspect_item.get("negative_count") or 0)
        neutral_count = int(aspect_item.get("neutral_count") or 0)

        dominant_sentiment = self._dominant_sentiment(
            positive_count=positive_count,
            negative_count=negative_count,
            neutral_count=neutral_count,
        )

        evidence_spans = [
            str(row.get("evidence_span") or row.get("clean_text") or row.get("raw_text") or "").strip()
            for row in evidence_rows
            if str(row.get("evidence_span") or row.get("clean_text") or row.get("raw_text") or "").strip()
        ]

        evidence_text = self._normalized_text(evidence_spans)
        theme_label = self._first_specific_theme_from_text(aspect, evidence_text)

        if not theme_label:
            theme_matches = self._extract_theme_matches(aspect=aspect, evidence_spans=evidence_spans)
            fallback_terms = self._extract_fallback_terms(aspect=aspect, evidence_spans=evidence_spans)
            theme_label = self._theme_label(
                aspect=aspect,
                theme_matches=theme_matches,
                fallback_terms=fallback_terms,
            )

        sentiment_terms = self._extract_sentiment_terms(evidence_spans)
        fallback_terms = self._extract_fallback_terms(aspect=aspect, evidence_spans=evidence_spans)
        theme_matches = self._extract_theme_matches(aspect=aspect, evidence_spans=evidence_spans)

        evidence_terms = self._dedupe_preserve_order(
            [theme_label] + theme_matches + sentiment_terms + fallback_terms
        )[:6]

        if theme_label in self.DEFAULT_THEME_BY_ASPECT.values() or theme_label in self.GENERIC_THEME_LABELS:
            one_liner = self._broad_one_liner(
                aspect=self.BROAD_THEME_BY_ASPECT.get(aspect, aspect),
                reaction=dominant_sentiment,
            )
        else:
            one_liner = self._one_liner(
                aspect=aspect,
                theme_label=theme_label,
                dominant_sentiment=dominant_sentiment,
                mentions=mentions,
            )

        return {
            "aspect": aspect,
            "reaction": dominant_sentiment,
            "mention_count": mentions,
            "positive_count": positive_count,
            "negative_count": negative_count,
            "neutral_count": neutral_count,
            "one_liner": one_liner,
            "evidence_terms": evidence_terms,
            "evidence_examples": evidence_spans[:3],
            "confidence_label": self._confidence_label(mentions),
            "llm_generated": False,
            "reason_source": "rules",
        }

    def _dominant_sentiment(
        self,
        positive_count: int,
        negative_count: int,
        neutral_count: int,
    ) -> str:
        if positive_count > negative_count and positive_count >= neutral_count:
            return "positive"
        if negative_count > positive_count and negative_count >= neutral_count:
            return "negative"
        if neutral_count > positive_count and neutral_count > negative_count:
            return "neutral"
        if positive_count > 0 and negative_count > 0:
            return "mixed"
        return "unclear"

    def _extract_theme_matches(self, aspect: str, evidence_spans: list[str]) -> list[str]:
        text = self._normalized_text(evidence_spans)
        counter: Counter[str] = Counter()

        for theme_label, triggers in self.ASPECT_THEME_RULES.get(aspect, []):
            for trigger in triggers:
                if trigger.lower() in text:
                    counter[theme_label] += 1

        return [term for term, _ in counter.most_common(8)]

    def _extract_sentiment_terms(self, evidence_spans: list[str]) -> list[str]:
        text = self._normalized_text(evidence_spans)
        counter: Counter[str] = Counter()

        for word in self.POSITIVE_WORDS:
            if word in text:
                counter[word] += 1

        for word in self.NEGATIVE_WORDS:
            if word in text:
                counter[word] += 1

        return [term for term, _ in counter.most_common(8)]

    def _extract_fallback_terms(self, aspect: str, evidence_spans: list[str]) -> list[str]:
        counter: Counter[str] = Counter()

        for span in evidence_spans[:4]:
            for token in re.findall(r"[a-zA-Z\u0900-\u097f][a-zA-Z\u0900-\u097f0-9_-]{2,}", span.lower()):
                if token == aspect or token in self.STOPWORDS:
                    continue
                if token in self.POSITIVE_WORDS or token in self.NEGATIVE_WORDS:
                    continue
                counter[token] += 1

        return [term for term, _ in counter.most_common(6)]

    def _theme_label(
        self,
        aspect: str,
        theme_matches: list[str],
        fallback_terms: list[str],
    ) -> str:
        if theme_matches:
            return theme_matches[0]

        if aspect in self.DEFAULT_THEME_BY_ASPECT:
            return self.DEFAULT_THEME_BY_ASPECT[aspect]

        if fallback_terms:
            return fallback_terms[0]

        return self._labelize(aspect).lower()

    def _one_liner(
        self,
        aspect: str,
        theme_label: str,
        dominant_sentiment: str,
        mentions: int,
    ) -> str:
        readable_aspect = self._labelize(aspect)
        readable_theme = theme_label.strip() or readable_aspect.lower()

        if dominant_sentiment == "positive":
            return f"Users mostly like {readable_theme}."

        if dominant_sentiment == "negative":
            return f"Users mostly complain about {readable_theme}."

        if dominant_sentiment == "mixed":
            return f"Users are split on {readable_theme}."

        if dominant_sentiment == "neutral":
            return f"Users mention {readable_theme}, but sentiment is mostly neutral."

        if mentions <= 1:
            return f"{readable_aspect} has only one clear mention so far."

        return f"Users mention {readable_theme}, but the sentiment direction is not clear yet."

    def _confidence_label(self, mentions: int) -> str:
        if mentions == 0:
            return "no_evidence"
        if mentions < 3:
            return "early"
        if mentions < 10:
            return "directional"
        return "stronger"

    def _normalized_text(self, evidence_spans: list[str]) -> str:
        text = " ".join(evidence_spans).lower()
        text = re.sub(r"[^a-z0-9\u0900-\u097f ._-]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _dedupe_preserve_order(self, values: list[str]) -> list[str]:
        seen = set()
        deduped = []

        for value in values:
            cleaned = str(value or "").strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(cleaned)

        return deduped

    def _cache_key(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _labelize(self, value: str | None) -> str:
        if not value:
            return "Unknown"
        return " ".join(part.capitalize() for part in str(value).replace("_", " ").split())