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

# ── Per-key quota ───────────────────────────────────────────────────────────
# Gemini free tier: 15 RPM per API key.
_GEMINI_RPM_LIMIT = 15
_GEMINI_WINDOW_SECONDS = 60.0

# ── Cooldown after 429 ─────────────────────────────────────────────────────
_COOLDOWN_SECONDS = 60.0

# ── Retry config ───────────────────────────────────────────────────────────
# 500/503 are server-side transient errors — back off and retry on any key.
# 429 is a quota error — rotate immediately, no sleep needed.
_RETRYABLE_SERVER_CODES: frozenset[int] = frozenset({500, 503})
_BACKOFF_BASE = 2.0    # seconds; doubles per attempt
_BACKOFF_MAX = 60.0    # hard ceiling


# ═══════════════════════════════════════════════════════════════════════════
# Per-key sliding-window rate limiter
# ═══════════════════════════════════════════════════════════════════════════

class _SlidingWindowRateLimiter:
    """Thread-safe sliding-window rate limiter for a single API key.

    Tracks the timestamps of the last N call slots within a rolling window.
    When the window is full, the caller blocks until the oldest slot expires.
    The lock is released *before* sleeping so other threads aren't stalled.
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
        """Block until a rate-limit slot is available, then claim it."""
        while True:
            with self._lock:
                now = time.monotonic()
                cutoff = now - self._window
                self._call_times = [t for t in self._call_times if t > cutoff]
                if len(self._call_times) < self._rate:
                    self._call_times.append(now)
                    return
                sleep_for = (self._call_times[0] + self._window) - now + 0.05
            time.sleep(max(0.0, sleep_for))


# ═══════════════════════════════════════════════════════════════════════════
# Multi-key round-robin pool with per-key cooldown
# ═══════════════════════════════════════════════════════════════════════════

class GeminiAllKeysCoolingError(RuntimeError):
    """Raised when every key in the pool is in 429 cooldown.

    The caller (HybridReviewAnalyzer) catches this as a generic Exception
    and falls back to rule-based analysis — no stack trace in prod logs.
    The UI RateLimitBanner becomes visible only when this error propagates
    to a TanStack Query, meaning the entire pool is truly exhausted.
    """


class _GeminiKeyPool:
    """Round-robin pool of Gemini API keys with per-key 429 cooldown.

    Each key has:
      - A _SlidingWindowRateLimiter (≤15 RPM per key, per Gemini free tier).
      - A cool_until timestamp (0.0 = available immediately).

    acquire() selects the next available key in round-robin order, advancing
    a cursor so load is spread evenly.  mark_cooldown() freezes a key for
    _COOLDOWN_SECONDS after a 429.

    Thread safety: all mutations are protected by a single lock.
    """

    def __init__(self, keys: list[str]) -> None:
        self._keys = keys
        self._cool_until: dict[str, float] = {k: 0.0 for k in keys}
        self._limiters: dict[str, _SlidingWindowRateLimiter] = {
            k: _SlidingWindowRateLimiter() for k in keys
        }
        self._cursor = 0
        self._lock = threading.Lock()

    # ── Public API ─────────────────────────────────────────────────────────

    def acquire(self) -> tuple[str, _SlidingWindowRateLimiter]:
        """Return the next (key, limiter) in round-robin order.

        Skips keys whose cooldown has not expired.
        Raises GeminiAllKeysCoolingError if every key is cooling.
        """
        with self._lock:
            now = time.monotonic()
            n = len(self._keys)
            for offset in range(n):
                idx = (self._cursor + offset) % n
                key = self._keys[idx]
                if now >= self._cool_until[key]:
                    self._cursor = (idx + 1) % n
                    return key, self._limiters[key]
            soonest = min(self._cool_until[k] for k in self._keys)
            raise GeminiAllKeysCoolingError(
                f"All {n} Gemini key(s) cooling. "
                f"Next available in {max(0.0, soonest - now):.0f}s."
            )

    def mark_cooldown(self, key: str, duration: float = _COOLDOWN_SECONDS) -> None:
        """Mark a key as cooling for `duration` seconds after a 429."""
        with self._lock:
            self._cool_until[key] = time.monotonic() + duration

    def soonest_available_at(self) -> float:
        """Monotonic timestamp when the first key comes out of cooldown."""
        with self._lock:
            if not self._keys:
                return time.monotonic()
            return min(self._cool_until[k] for k in self._keys)

    # ── Introspection (used by status()) ───────────────────────────────────

    @property
    def size(self) -> int:
        return len(self._keys)

    def available_count(self) -> int:
        now = time.monotonic()
        with self._lock:
            return sum(1 for k in self._keys if now >= self._cool_until[k])

    def cooling_count(self) -> int:
        return self.size - self.available_count()


# ═══════════════════════════════════════════════════════════════════════════
# LlmService
# ═══════════════════════════════════════════════════════════════════════════

class LlmService:
    """Provider-agnostic LLM service with multi-key round-robin failover.

    Key resolution (highest priority first)
    ----------------------------------------
    1. GEMINI_API_KEYS  — comma-separated list: "AIza...1,AIza...2,AIza...3"
    2. GEMINI_API_KEY   — single key (backward-compatible)

    Failover flow for _call_gemini()
    ----------------------------------
    Per attempt (max = pool.size × 3, min 6):

      Step 1 — pool.acquire()
        → Returns the next available key in round-robin order.
        → If all keys are cooling → sleep until the soonest recovers,
          then continue (this counts as an attempt, not a retry).

      Step 2 — per-key rate limiter (≤15 RPM)
        → Blocks if this key has already fired 15 calls this minute.

      Step 3 — HTTP POST to Gemini
        Success  → return text.
        429      → mark_cooldown(key); continue immediately (no sleep).
        500/503  → exponential backoff + jitter; continue on any key.
        Other    → raise immediately (non-retryable).

    The RateLimitBanner in the UI only becomes visible when GeminiAllKeysCoolingError
    propagates all the way to the TanStack Query layer — i.e. the pool is
    truly exhausted.  For a single-key deployment the banner appears whenever
    that key is cooling.
    """

    def __init__(self) -> None:
        load_dotenv()
        self.provider = os.getenv("IRIP_LLM_PROVIDER", "gemini").strip().lower()
        self.gemini_model = os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL).strip()
        self._key_pool = self._build_key_pool()

    # ── Public API ─────────────────────────────────────────────────────────

    def status(self) -> LlmProviderStatus:
        # Re-read mode/provider/model for hot-reload; do NOT rebuild the pool
        # (rebuilding would destroy live cooldown state).
        load_dotenv(override=False)
        self.provider = os.getenv("IRIP_LLM_PROVIDER", "gemini").strip().lower()
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

        pool = self._key_pool
        if pool.size == 0:
            return LlmProviderStatus(
                provider="gemini",
                enabled=False,
                model=self.gemini_model,
                mode=mode,
                reason=(
                    "No Gemini API keys configured. "
                    "Set GEMINI_API_KEYS (comma-separated) or GEMINI_API_KEY."
                ),
            )

        available = pool.available_count()
        cooling = pool.cooling_count()

        if available == 0:
            return LlmProviderStatus(
                provider="gemini",
                enabled=False,
                model=self.gemini_model,
                mode=mode,
                reason=f"All {pool.size} key(s) are in 429 cooldown.",
            )

        reason: str | None = None
        if cooling > 0:
            reason = (
                f"{available}/{pool.size} key(s) available "
                f"({cooling} cooling after 429)."
            )

        return LlmProviderStatus(
            provider="gemini",
            enabled=True,
            model=self.gemini_model,
            mode=mode,
            reason=reason,
        )

    def set_mode(self, mode: str) -> str:
        normalized = mode.strip().lower()
        if normalized not in {"off", "selective", "always"}:
            raise ValueError("Invalid LLM mode. Allowed values: off, selective, always.")
        os.environ["IRIP_LLM_MODE"] = normalized
        return normalized

    def generate_narrative(self, prompt: str) -> str:
        """Call Gemini for free-text prose output (narratives, summaries)."""
        if self._key_pool.size == 0:
            raise RuntimeError(
                "No Gemini API keys configured. "
                "Set GEMINI_API_KEYS or GEMINI_API_KEY."
            )
        return self._call_gemini(
            prompt=prompt,
            generation_config={"temperature": 0.4, "maxOutputTokens": 1024},
        )

    def extract_review_intelligence(
        self,
        request: LlmReviewExtractionRequest,
    ) -> LlmReviewExtractionResponse:
        s = self.status()
        if not s.enabled:
            raise RuntimeError(s.reason or "LLM provider is not enabled.")

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

    # ── Internal ───────────────────────────────────────────────────────────

    def _build_key_pool(self) -> _GeminiKeyPool:
        """Parse GEMINI_API_KEYS (plural CSV) then fall back to GEMINI_API_KEY."""
        plural_raw = os.getenv("GEMINI_API_KEYS", "").strip()
        if plural_raw:
            candidates = [k.strip() for k in plural_raw.split(",") if k.strip()]
        else:
            single = os.getenv("GEMINI_API_KEY", "").strip()
            candidates = [single] if single else []

        # Deduplicate while preserving insertion order.
        seen: set[str] = set()
        unique: list[str] = []
        for k in candidates:
            if k not in seen:
                seen.add(k)
                unique.append(k)

        return _GeminiKeyPool(unique)

    def _call_gemini(
        self,
        prompt: str,
        generation_config: dict[str, Any] | None = None,
    ) -> str:
        """Rate-limited, multi-key Gemini call with rotation and backoff.

        429  → mark_cooldown(key), rotate immediately.
        500/503 → exponential backoff + jitter, retry on any available key.
        All cooling → sleep until soonest recovery, then retry.
        """
        endpoint_tpl = (
            "https://generativelanguage.googleapis.com/v1beta/"
            "models/{model}:generateContent?key={key}"
        )
        payload: dict[str, Any] = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        }
        if generation_config:
            payload["generationConfig"] = generation_config

        request_data = json.dumps(payload).encode("utf-8")
        timeout = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "120"))

        # Scale max attempts with pool size so every key gets ≥3 chances.
        max_attempts = max(self._key_pool.size * 3, 6)

        for attempt in range(max_attempts):

            # ── 1. Pick the next available key ──────────────────────────
            try:
                key, limiter = self._key_pool.acquire()
            except GeminiAllKeysCoolingError:
                # All keys are cooling — wait for the first one to recover,
                # then loop back and try acquire() again.
                wait = max(0.1, self._key_pool.soonest_available_at() - time.monotonic())
                time.sleep(min(wait, _BACKOFF_MAX))
                continue

            # ── 2. Respect per-key rate limit (≤15 RPM) ─────────────────
            limiter.acquire()

            # ── 3. HTTP call ─────────────────────────────────────────────
            url = endpoint_tpl.format(model=self.gemini_model, key=key)
            req = urllib.request.Request(
                url,
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
                    raise RuntimeError(
                        f"Unexpected Gemini response shape: {data}"
                    ) from exc

            except urllib.error.HTTPError as exc:
                status_code = exc.code
                # Read body before any sleep — HTTPError is not re-readable.
                error_body = exc.read().decode("utf-8", errors="replace")

                if status_code == 429:
                    # ── 4. Cool this key; next iteration picks a fresh one ─
                    self._key_pool.mark_cooldown(key)
                    continue

                if status_code in _RETRYABLE_SERVER_CODES:
                    # ── 5. Server hiccup — back off, then retry any key ───
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
            f"Gemini API: exhausted {max_attempts} attempts across "
            f"{self._key_pool.size} key(s). All keys may be rate-limited."
        )


# ═══════════════════════════════════════════════════════════════════════════
# Prompt builder
# ═══════════════════════════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════════════════════════
# Parsing helpers
# ═══════════════════════════════════════════════════════════════════════════

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
