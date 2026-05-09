from __future__ import annotations

import re

from app.schemas.review import AspectSentiment, Sentiment
from app.services.lexicon_service import LivingLexiconService

WINDOW_CHARS = 72


SUPPLEMENTAL_ASPECT_TERMS: dict[str, list[dict]] = {
    "heating": [
        {"term": "heating", "default_prior": "negative", "confidence": 0.94},
        {"term": "heat", "default_prior": "negative", "confidence": 0.9},
        {"term": "hot", "default_prior": "negative", "confidence": 0.86},
        {"term": "garam", "default_prior": "negative", "confidence": 0.92},
        {"term": "tawa", "default_prior": "negative", "confidence": 0.9},
        {"term": "overheat", "default_prior": "negative", "confidence": 0.94},
        {"term": "thermal", "default_prior": "negative", "confidence": 0.82},
    ],
    "software": [
        {"term": "software", "default_prior": None, "confidence": 0.92},
        {"term": "ui", "default_prior": None, "confidence": 0.88},
        {"term": "bug", "default_prior": "negative", "confidence": 0.9},
        {"term": "bugs", "default_prior": "negative", "confidence": 0.9},
        {"term": "bloatware", "default_prior": "negative", "confidence": 0.88},
        {"term": "update", "default_prior": None, "confidence": 0.82},
        {"term": "ads", "default_prior": "negative", "confidence": 0.82},
    ],
    "performance": [
        {"term": "performance", "default_prior": None, "confidence": 0.94},
        {"term": "smooth", "default_prior": "positive", "confidence": 0.88},
        {"term": "fast open", "default_prior": "positive", "confidence": 0.86},
        {"term": "apps fast", "default_prior": "positive", "confidence": 0.86},
        {"term": "multitasking", "default_prior": None, "confidence": 0.84},
        {"term": "gaming", "default_prior": None, "confidence": 0.82},
        {"term": "speed", "default_prior": None, "confidence": 0.8},
        {"term": "processor", "default_prior": None, "confidence": 0.82},
    ],
    "audio": [
        {"term": "speaker", "default_prior": None, "confidence": 0.92},
        {"term": "sound", "default_prior": None, "confidence": 0.86},
        {"term": "audio", "default_prior": None, "confidence": 0.9},
        {"term": "volume", "default_prior": None, "confidence": 0.84},
        {"term": "mic", "default_prior": None, "confidence": 0.84},
        {"term": "microphone", "default_prior": None, "confidence": 0.88},
        {"term": "call quality", "default_prior": None, "confidence": 0.9},
        {"term": "loud", "default_prior": "positive", "confidence": 0.84},
    ],
    "connectivity": [
        {"term": "network", "default_prior": None, "confidence": 0.92},
        {"term": "internet", "default_prior": None, "confidence": 0.84},
        {"term": "wifi", "default_prior": None, "confidence": 0.84},
        {"term": "wi-fi", "default_prior": None, "confidence": 0.84},
        {"term": "bluetooth", "default_prior": None, "confidence": 0.82},
        {"term": "5g", "default_prior": None, "confidence": 0.82},
        {"term": "signal", "default_prior": None, "confidence": 0.86},
        {"term": "call drop", "default_prior": "negative", "confidence": 0.9},
        {"term": "drop", "default_prior": "negative", "confidence": 0.78},
    ],
    "design": [
        {"term": "design", "default_prior": None, "confidence": 0.9},
        {"term": "premium", "default_prior": "positive", "confidence": 0.86},
        {"term": "feel", "default_prior": None, "confidence": 0.76},
        {"term": "grip", "default_prior": None, "confidence": 0.78},
        {"term": "weight", "default_prior": None, "confidence": 0.76},
        {"term": "back panel", "default_prior": None, "confidence": 0.82},
        {"term": "build", "default_prior": None, "confidence": 0.82},
    ],
    "display": [
        {"term": "display", "default_prior": None, "confidence": 0.94},
        {"term": "screen", "default_prior": None, "confidence": 0.9},
        {"term": "brightness", "default_prior": None, "confidence": 0.86},
        {"term": "bright", "default_prior": "positive", "confidence": 0.84},
        {"term": "outdoor", "default_prior": None, "confidence": 0.76},
        {"term": "touch", "default_prior": None, "confidence": 0.82},
    ],
}

POSITIVE_PATTERNS = [
    ("excellent", 0.86, 0.9),
    ("zabardast", 0.92, 0.9),
    ("mast", 0.86, 0.9),
    ("good", 0.72, 0.72),
    ("best", 0.78, 0.76),
    ("nice", 0.68, 0.68),
    ("great", 0.78, 0.74),
    ("love", 0.82, 0.72),
    ("super", 0.78, 0.72),
    ("bright", 0.72, 0.72),
    ("clearly", 0.68, 0.68),
    ("clear", 0.72, 0.74),
    ("smooth", 0.78, 0.76),
    ("fast", 0.74, 0.72),
    ("premium", 0.76, 0.74),
    ("acha", 0.72, 0.72),
    ("achha", 0.72, 0.72),
    ("loud", 0.72, 0.7),
]

NEGATIVE_PATTERNS = [
    ("terrible", 0.9, 0.86),
    ("bakwas", 0.93, 0.92),
    ("bekar", 0.86, 0.9),
    ("bad", 0.78, 0.76),
    ("poor", 0.78, 0.76),
    ("worst", 0.92, 0.86),
    ("slow", 0.74, 0.72),
    ("drain", 0.8, 0.76),
    ("problem", 0.72, 0.68),
    ("issue", 0.72, 0.68),
    ("hot", 0.78, 0.72),
    ("heat", 0.82, 0.78),
    ("heating", 0.86, 0.84),
    ("garam", 0.82, 0.82),
    ("tawa", 0.86, 0.8),
    ("lag", 0.76, 0.72),
    ("bug", 0.78, 0.76),
    ("bugs", 0.78, 0.76),
    ("unstable", 0.78, 0.74),
    ("drop", 0.76, 0.72),
    ("not good", 0.82, 0.8),
]


class AspectRuleExtractor:
    """Explainable baseline extractor for Hinglish/English smartphone reviews.

    The goal of this baseline is not to beat LLMs. It should be deterministic,
    debuggable, and strong enough to catch common Indian smartphone review signals
    while avoiding obvious false positives.
    """

    def __init__(self, lexicon: LivingLexiconService) -> None:
        self.lexicon = lexicon

    def extract(self, text: str) -> list[AspectSentiment]:
        lowered = text.lower()
        candidates = self._find_aspect_candidates(text)

        results: list[AspectSentiment] = []
        seen_aspects: set[str] = set()

        for candidate in sorted(candidates, key=lambda item: (item["index"], item["aspect"])):
            aspect = candidate["aspect"]

            if aspect in seen_aspects:
                continue

            if self._should_skip_candidate(aspect=aspect, term=candidate["term"], lowered=lowered):
                continue

            index = candidate["index"]
            term = candidate["term"]
            start = max(0, index - WINDOW_CHARS)
            end = min(len(text), index + len(term) + WINDOW_CHARS)
            evidence = text[start:end].strip()

            sentiment, intensity, confidence = self._infer_sentiment(
                evidence=evidence,
                aspect_local_index=index - start,
                default_prior=candidate.get("default_prior"),
            )

            candidate_confidence = float(candidate["confidence"])
            results.append(
                AspectSentiment(
                    aspect=aspect,
                    sub_aspect=None,
                    sentiment=sentiment,
                    intensity=intensity,
                    confidence=round(min(confidence, candidate_confidence), 3),
                    evidence_span=evidence,
                    provider="aspect_rules_v1",
                )
            )
            seen_aspects.add(aspect)

        return results

    def _find_aspect_candidates(self, text: str) -> list[dict]:
        lowered = text.lower()
        candidates: list[dict] = []

        for aspect_entry in self.lexicon.get_aspect_terms():
            if not aspect_entry.aspect:
                continue

            term = (
                aspect_entry.normalized_term.lower()
                if aspect_entry.normalized_term
                else aspect_entry.term.lower()
            )
            index = self._find_term(lowered, term)

            if index >= 0:
                candidates.append(
                    {
                        "aspect": aspect_entry.aspect,
                        "term": term,
                        "index": index,
                        "confidence": aspect_entry.confidence,
                        "default_prior": aspect_entry.sentiment_prior,
                    }
                )

        for aspect, terms in SUPPLEMENTAL_ASPECT_TERMS.items():
            for item in terms:
                term = item["term"].lower()
                index = self._find_term(lowered, term)

                if index >= 0:
                    candidates.append(
                        {
                            "aspect": aspect,
                            "term": term,
                            "index": index,
                            "confidence": item["confidence"],
                            "default_prior": item["default_prior"],
                        }
                    )

        return candidates

    def _find_term(self, lowered: str, term: str) -> int:
        pattern = re.compile(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])")
        match = pattern.search(lowered)
        return match.start() if match else -1

    def _should_skip_candidate(self, aspect: str, term: str, lowered: str) -> bool:
        # Avoid false performance extraction when lag is clearly UI/software related.
        if aspect == "performance" and term == "lag":
            software_context = any(word in lowered for word in ["ui", "software", "bug", "bugs"])
            if software_context:
                return True

        # "gaming" can be performance context, but when the sentence is mainly
        # about heat/garam/tawa/overheating, do not create a separate performance issue.
        if aspect == "performance" and term == "gaming":
            heating_context = any(
                word in lowered
                for word in ["heat", "heating", "hot", "garam", "tawa", "overheat", "thermal"]
            )
            if heating_context:
                return True

        # "feel" alone can be vague; require design/build/premium/hand context.
        if aspect == "design" and term == "feel":
            design_context = any(
                word in lowered
                for word in ["design", "premium", "haath", "hand", "grip", "build", "back panel"]
            )
            if not design_context:
                return True

        # "drop" alone is only connectivity when network/call/signal context exists.
        if aspect == "connectivity" and term == "drop":
            network_context = any(
                word in lowered for word in ["network", "call", "signal", "internet", "wifi", "wi-fi"]
            )
            if not network_context:
                return True

        return False

    def _infer_sentiment(
        self,
        evidence: str,
        aspect_local_index: int,
        default_prior: str | None,
    ) -> tuple[Sentiment, float, float]:
        lowered = evidence.lower()
        candidates: list[tuple[int, int, float, Sentiment, float, float]] = []

        for entry in self.lexicon.get_sentiment_terms():
            term_index = self._find_term(lowered, entry.term.lower())
            if term_index >= 0 and entry.sentiment_prior:
                distance = abs(term_index - aspect_local_index)
                side_priority = 0 if term_index >= aspect_local_index else 1
                candidates.append(
                    (
                        side_priority,
                        distance,
                        -float(entry.intensity),
                        Sentiment(entry.sentiment_prior),
                        float(entry.intensity),
                        float(entry.confidence),
                    )
                )

        for pattern, intensity, confidence in NEGATIVE_PATTERNS:
            term_index = self._find_term(lowered, pattern)
            if term_index >= 0:
                distance = abs(term_index - aspect_local_index)
                side_priority = 0 if term_index >= aspect_local_index else 1
                candidates.append(
                    (
                        side_priority,
                        distance,
                        -intensity,
                        Sentiment.negative,
                        intensity,
                        confidence,
                    )
                )

        for pattern, intensity, confidence in POSITIVE_PATTERNS:
            term_index = self._find_term(lowered, pattern)
            if term_index >= 0:
                distance = abs(term_index - aspect_local_index)
                side_priority = 0 if term_index >= aspect_local_index else 1
                candidates.append(
                    (
                        side_priority,
                        distance,
                        -intensity,
                        Sentiment.positive,
                        intensity,
                        confidence,
                    )
                )

        if candidates:
            candidates.sort(key=lambda item: (item[0], item[1], item[2]))
            _, _, _, sentiment, intensity, confidence = candidates[0]
            return sentiment, intensity, confidence

        if default_prior:
            return Sentiment(default_prior), 0.72, 0.7

        return Sentiment.neutral, 0.5, 0.48