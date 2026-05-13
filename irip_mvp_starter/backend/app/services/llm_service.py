from __future__ import annotations

import json
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any

from dotenv import load_dotenv

from app.schemas.llm import (
    LlmAspectSentiment,
    LlmProviderStatus,
    LlmReviewExtractionRequest,
    LlmReviewExtractionResponse,
)

DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"

# Gemini free-tier hard limit: 15 requests per 60-second rolling window.
_GEMINI_RPM_LIMIT = 15
_GEMINI_WINDOW_SECONDS = 60.0

# Which HTTP status codes are transient and worth retrying.
_RETRYABLE_CODES: frozenset[int] = frozenset({429, 500, 503})

# Retry / backoff constants.
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0   # seconds — doubles on each attempt
_BACKOFF_MAX = 60.0   # hard ceiling


class _SlidingWindowRateLimiter:
    """Thread-safe sliding-window rate limiter.

    Tracks timestamps of the last N successful slot acquisitions within a
    rolling time window.  When the window is full, callers block until the
    oldest slot expires.

    The lock is released *before* sleeping so other threads can continue
    checking the window state rather than queueing behind a held lock.
    """

    def __init__(
        self,
        rate: int = _GEMINI_RPM_LIMIT,
        window: float = _GEMINI_WINDOW_SECONDS,
    ) -> None:
        self._rate = rate
        self._window = window
        self._call_times: list[float] = []
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a call slot is available, then claim it."""
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - self._window
                # Drop calls that have scrolled outside the window.
                self._call_times = [t for t in self._call_times if t > cutoff]
                if len(self._call_times) < self._rate:
                    self._call_times.append(now)
                    return
                # Calculate how long until the oldest call leaves the window.
                sleep_for = (self._call_times[0] + self._window) - now + 0.05
            # Release the lock before sleeping so other threads aren't stalled.
            time.sleep(max(0.0, sleep_for))


# Module-level singleton — shared across every LlmService instance in this
# process.  On Render free tier there is exactly one process, so this single
# limiter correctly serialises all Gemini traffic to ≤15 RPM.
_gemini_rate_limiter = _SlidingWindowRateLimiter()


class LlmService:
    """Provider-agnostic LLM service.

    MVP provider: Gemini 2.5 Flash via direct REST API.
    Future providers can be plugged in without changing the route contract.

    Rate-limiting strategy
    ----------------------
    All Gemini calls go through _call_gemini(), which:
      1. Acquires a slot from the sliding-window limiter (≤15 RPM).
      2. Executes the HTTP request.
      3. On a transient error (429 / 500 / 503), waits with full-jitter
         exponential backoff and retries up to _MAX_RETRIES times.

    This means a batch import of 200 reviews will automatically pace itself
    at ≤15 calls per minute and survive any transient Gemini hiccups without
    losing data — the hybrid_analyzer.py rules-based fallback still fires if
    all retries are exhausted.
    """

    def __init__(self) -> None:
        load_dotenv()
        self.provider = os.getenv("IRIP_LLM_PROVIDER", "gemini").strip().lower()
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.gemini_model = os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip()

    def status(self) -> LlmProviderStatus:
        load_dotenv(override=False)
        self.provider = os.getenv("IRIP_LLM_PROVIDER", "gemini").strip().lower()
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        self.gemini_model = os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip()
        mode = os.getenv("IRIP_LLM_MODE", "selective").strip().lower()

        if self.provider != "gemini":
            return LlmProviderStatus(
                provider=self.provider,
                enabled=False,
                model=None,
                mode=mode,
                reason="Only gemini provider is implemented in this MVP slice.",
            )

        if not self.gemini_api_key:
            return LlmProviderStatus(
                provider="gemini",
                enabled=False,
                model=self.gemini_model,
                mode=mode,
                reason="GEMINI_API_KEY is not set in backend environment.",
            )

        return LlmProviderStatus(
            provider="gemini",
            enabled=True,
            model=self.gemini_model,
            mode=mode,
            reason=None,
        )

    def set_mode(self, mode: str) -> str:
        normalized = mode.strip().lower()
        allowed = {"off", "selective", "always"}
        if normalized not in allowed:
            raise ValueError("Invalid LLM mode. Allowed values: off, selective, always.")
        os.environ["IRIP_LLM_MODE"] = normalized
        return normalized

    def generate_narrative(self, prompt: str) -> str:
        """Call Gemini for free-text prose output."""
        if not self.gemini_api_key:
            raise RuntimeError("GEMINI_API_KEY is not configured.")
        return self._call_gemini(
            prompt=prompt,
            generation_config={"temperature": 0.4, "maxOutputTokens": 1024},
        )

    def extract_review_intelligence(
        self,
        request: LlmReviewExtractionRequest,
    ) -> LlmReviewExtractionResponse:
        status = self.status()
        if not status.enabled:
            raise RuntimeError(status.reason or "LLM provider is not enabled.")

        prompt = _build_review_extraction_prompt(request)
        raw_text = self._call_gemini(
            prompt=prompt,
            generation_config={"temperature": 0.1, "responseMimeType": "application/json"},
        )
        parsed = _extract_json_object(raw_text)

        aspects = [
            LlmAspectSentiment(
                aspect=str(item.get("aspect", "")).strip().lower(),
                sub_aspect=_optional_str(item.get("sub_aspect")),
                sentiment=str(item.get("sentiment", "neutral")).strip().lower(),
                intensity=_bounded_float(item.get("intensity"), default=0.5),
                confidence=_bounded_float(item.get("confidence"), default=0.5),
                evidence_span=str(item.get("evidence_span", "")).strip(),
                reasoning_note=_optional_str(item.get("reasoning_note")),
            )
            for item in parsed.get("aspects", [])
            if isinstance(item, dict) and item.get("aspect") and item.get("evidence_span")
        ]

        return LlmReviewExtractionResponse(
            provider="gemini",
            model=self.gemini_model,
            product_id=request.product_id,
            overall_sentiment=str(parsed.get("overall_sentiment", "mixed")).strip().lower(),
            language_profile=parsed.get("language_profile", {})
            if isinstance(parsed.get("language_profile"), dict)
            else {},
            product_signal=bool(parsed.get("product_signal", True)),
            delivery_signal=bool(parsed.get("delivery_signal", False)),
            service_signal=bool(parsed.get("service_signal", False)),
            sarcasm_flag=bool(parsed.get("sarcasm_flag", False)),
            contradiction_flag=bool(parsed.get("contradiction_flag", False)),
            aspects=aspects,
            confidence=_bounded_float(parsed.get("confidence"), default=0.6),
            raw_model_text=raw_text,
        )

    # ------------------------------------------------------------------ #
    #  Internal — rate-limited, retry-backed Gemini HTTP call             #
    # ------------------------------------------------------------------ #

    def _call_gemini(
        self,
        prompt: str,
        generation_config: dict[str, Any] | None = None,
    ) -> str:
        """Execute a Gemini generateContent call with rate limiting and backoff.

        Algorithm
        ---------
        For each attempt (up to _MAX_RETRIES + 1 total):
          1. Block in _SlidingWindowRateLimiter.acquire() until a slot is free.
          2. Fire the HTTP request.
          3. On success, return the text immediately.
          4. On a retryable status (429 / 500 / 503), wait with full-jitter
             exponential backoff:  delay = min(base * 2^attempt + U(0, base), max)
          5. On any other error, raise immediately (no retry).
        """
        endpoint = (
            "https://generativelanguage.googleapis.com/v1beta/"
            f"models/{self.gemini_model}:generateContent?key={self.gemini_api_key}"
        )
        payload: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        }
        if generation_config:
            payload["generationConfig"] = generation_config

        request_data = json.dumps(payload).encode("utf-8")
        timeout = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "120"))

        for attempt in range(_MAX_RETRIES + 1):
            _gemini_rate_limiter.acquire()

            req = urllib.request.Request(
                endpoint,
                data=request_data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as response:
                    data: dict[str, Any] = json.loads(response.read().decode("utf-8"))
                try:
                    return data["candidates"][0]["content"]["parts"][0]["text"]
                except (KeyError, IndexError, TypeError) as exc:
                    raise RuntimeError(f"Unexpected Gemini response shape: {data}") from exc

            except urllib.error.HTTPError as exc:
                status_code = exc.code
                # Read the body now — the HTTPError object is not re-readable.
                error_body = exc.read().decode("utf-8", errors="replace")

                if status_code in _RETRYABLE_CODES and attempt < _MAX_RETRIES:
                    # Full-jitter exponential backoff:
                    #   base * 2^0 + jitter → base * 2^1 + jitter → …
                    jitter = random.uniform(0.0, _BACKOFF_BASE)
                    delay = min(_BACKOFF_BASE * (2 ** attempt) + jitter, _BACKOFF_MAX)
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"Gemini API HTTP {status_code}: {error_body}"
                ) from exc

            except Exception as exc:
                raise RuntimeError(f"Gemini API request failed: {exc}") from exc

        raise RuntimeError(
            f"Gemini API: exhausted {_MAX_RETRIES} retries on transient errors."
        )


# ------------------------------------------------------------------ #
#  Prompt builder                                                      #
# ------------------------------------------------------------------ #

def _build_review_extraction_prompt(request: LlmReviewExtractionRequest) -> str:
    return f"""
You are an Indian smartphone review intelligence analyst for an OEM.

Analyze this messy Indian e-commerce smartphone review. The review may contain:
- Hinglish
- Romanized Hindi
- mixed Hindi-English
- slang
- emoji
- spelling noise
- sarcasm
- delivery/seller/service complaints mixed with product feedback

Product ID: {request.product_id}
Product name: {request.product_name or "unknown"}
Source: {request.source or "unknown"}
Star rating: {request.rating if request.rating is not None else "unknown"}

Review:
\"\"\"{request.raw_text}\"\"\"

Return ONLY valid JSON. No markdown. No explanation outside JSON.

Schema:
{{
  "overall_sentiment": "positive | negative | neutral | mixed",
  "language_profile": {{
    "primary_language": "en_only | hi_only | hi_en_mixed | other",
    "script": "roman | devanagari | mixed | other",
    "hinglish_detected": true,
    "confidence": 0.0
  }},
  "product_signal": true,
  "delivery_signal": false,
  "service_signal": false,
  "sarcasm_flag": false,
  "contradiction_flag": false,
  "aspects": [
    {{
      "aspect": "battery | camera | display | performance | heating | charging | software | connectivity | audio | design | value | after_sales",
      "sub_aspect": "short optional sub aspect",
      "sentiment": "positive | negative | neutral | mixed",
      "intensity": 0.0,
      "confidence": 0.0,
      "evidence_span": "exact phrase from review",
      "reasoning_note": "short reason"
    }}
  ],
  "confidence": 0.0
}}

Important interpretation rules:
- "mast", "zabardast", "dhansu", "paisa vasool" are positive.
- "bekar", "bakwas", "faltu", "waste", "ghatiya" are negative.
- "garam", "heat", "heating", "tawa" usually indicate heating complaint.
- Separate product feedback from delivery/seller/service.
- If star rating and text disagree, set contradiction_flag true.
- If there is no product feedback, product_signal should be false.
""".strip()


# ------------------------------------------------------------------ #
#  Parsing helpers                                                     #
# ------------------------------------------------------------------ #

def _extract_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()

    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not match:
            raise RuntimeError(f"LLM did not return JSON: {text}")
        parsed = json.loads(match.group(0))

    if not isinstance(parsed, dict):
        raise RuntimeError(f"LLM JSON root must be an object: {parsed}")

    return parsed


def _bounded_float(value: object, default: float) -> float:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, number))


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
