from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any

from app.services.llm_service import LlmService


class BenchmarkSummaryService:
    """Generate a concise Gemini-backed benchmark summary from review-gap data.

    Rules:
    - Never mention internal scores, point gaps, or numeric gap values.
    - Never invent hardware specs.
    - Explain which product users appear to prefer and why, using only review benchmark signals.
    - Keep it short and product-manager readable.
    """

    _MEMORY_CACHE: dict[str, dict] = {}

    def __init__(self, llm_service: LlmService | None = None) -> None:
        self.llm_service = llm_service or LlmService()

    def build_summary(
        self,
        product_id: str,
        competitor_product_id: str | None,
        competitor_benchmark: dict | None,
        selected_reason_cards: list[dict] | None = None,
    ) -> dict | None:
        if not competitor_product_id or not competitor_benchmark:
            return None

        rows = competitor_benchmark.get("benchmark_aspects", []) or []
        selected_reason_cards = selected_reason_cards or []

        if not rows:
            return {
                "headline": "No comparable benchmark evidence is available yet.",
                "selected_product_summary": "Selected product comparison needs more review evidence.",
                "competitor_summary": "Competitor comparison needs more review evidence.",
                "risk_summary": "Import more comparable reviews before reading this benchmark.",
                "bullets": [],
                "source": "rules",
            }

        fallback = self._rule_summary(rows, selected_reason_cards)

        if not self._should_use_gemini():
            return fallback

        prompt = self._build_prompt(
            product_id=product_id,
            competitor_product_id=competitor_product_id,
            rows=rows,
            selected_reason_cards=selected_reason_cards,
            fallback=fallback,
        )

        cache_key = self._cache_key(prompt)
        if cache_key in self._MEMORY_CACHE:
            return self._MEMORY_CACHE[cache_key]

        try:
            raw_text = self.llm_service._call_gemini(prompt)
            parsed = self._extract_json(raw_text)
            summary = self._validate_summary(parsed, fallback)
            self._MEMORY_CACHE[cache_key] = summary
            return summary
        except Exception:
            return fallback

    def _should_use_gemini(self) -> bool:
        if os.getenv("PYTEST_CURRENT_TEST") and os.getenv("IRIP_ENABLE_LLM_IN_TESTS") != "1":
            return False

        mode = os.getenv("IRIP_BENCHMARK_SUMMARY_MODE", "auto").strip().lower()
        if mode in {"off", "rules", "rule", "false", "0"}:
            return False

        try:
            status = self.llm_service.status()
        except Exception:
            return False

        return bool(status.enabled) and mode in {"auto", "gemini", "llm", "always", "on", "true", "1"}

    def _build_prompt(
        self,
        product_id: str,
        competitor_product_id: str,
        rows: list[dict],
        selected_reason_cards: list[dict],
        fallback: dict,
    ) -> str:
        payload_rows = []
        for row in rows:
            payload_rows.append(
                {
                    "aspect": row.get("aspect"),
                    "selected_product_signal": self._signal_label(row),
                    "confidence_label": row.get("confidence_label"),
                    "interpretation": self._remove_scores(str(row.get("interpretation") or "")),
                }
            )

        reason_payload = []
        for card in selected_reason_cards:
            reason_payload.append(
                {
                    "aspect": card.get("aspect"),
                    "reaction": card.get("reaction"),
                    "one_liner": card.get("one_liner"),
                    "evidence_terms": card.get("evidence_terms", []),
                    "confidence_label": card.get("confidence_label"),
                }
            )

        payload = {
            "selected_product_id": product_id,
            "competitor_product_id": competitor_product_id,
            "benchmark_rows": payload_rows,
            "selected_product_reason_cards": reason_payload,
            "fallback_summary": fallback,
        }

        return f"""
You are an OEM smartphone review intelligence analyst.

Task:
Write a concise benchmark summary comparing the selected product and competitor using ONLY the data below.

Strict rules:
- Do NOT mention scores, points, numeric gaps, or internal scoring.
- Do NOT invent specs, prices, RAM, battery capacity, chipset, OS, or hardware facts.
- Use only review sentiment benchmark rows and selected-product reason cards.
- Explain which product users appear to like more by aspect.
- Explain WHY where evidence supports it.
- If the reason is not available, say the preference is directional from review sentiment.
- Treat evidence gaps clearly; do not call them confirmed wins/losses.
- Use "selected product" and "competitor"; do not say "our product".
- Keep every field short, clear, and professional.
- Output valid JSON only. No markdown.

Return schema:
{{
  "headline": "One short sentence describing the comparison.",
  "selected_product_summary": "Where users appear to prefer the selected product and why.",
  "competitor_summary": "Where users appear to prefer the competitor and why.",
  "risk_summary": "Main caution or evidence gap.",
  "bullets": [
    "Short useful bullet without scores.",
    "Short useful bullet without scores."
  ]
}}

Data:
{json.dumps(payload, ensure_ascii=False, indent=2)}
""".strip()

    def _validate_summary(self, parsed: dict[str, Any], fallback: dict) -> dict:
        headline = self._clean_text(parsed.get("headline"), fallback["headline"], 170)
        selected_product_summary = self._clean_text(
            parsed.get("selected_product_summary"),
            fallback["selected_product_summary"],
            210,
        )
        competitor_summary = self._clean_text(
            parsed.get("competitor_summary"),
            fallback["competitor_summary"],
            210,
        )
        risk_summary = self._clean_text(parsed.get("risk_summary"), fallback["risk_summary"], 210)

        raw_bullets = parsed.get("bullets")
        bullets: list[str] = []
        if isinstance(raw_bullets, list):
            for item in raw_bullets[:3]:
                cleaned = self._clean_text(item, "", 145)
                if cleaned:
                    bullets.append(cleaned)

        if not bullets:
            bullets = fallback.get("bullets", [])[:3]

        return {
            "headline": headline,
            "selected_product_summary": selected_product_summary,
            "competitor_summary": competitor_summary,
            "risk_summary": risk_summary,
            "bullets": bullets,
            "source": "gemini",
        }

    def _rule_summary(self, rows: list[dict], selected_reason_cards: list[dict]) -> dict:
        selected_leads: list[str] = []
        competitor_leads: list[str] = []
        evidence_gaps: list[str] = []

        reason_by_aspect = {
            str(card.get("aspect") or "").lower(): str(card.get("one_liner") or "").strip()
            for card in selected_reason_cards
        }

        selected_reasons: list[str] = []

        for row in rows:
            aspect_raw = str(row.get("aspect") or "").lower()
            aspect = self._labelize(aspect_raw)
            gap = float(row.get("gap") or 0)
            confidence = str(row.get("confidence_label") or "").lower()

            if "insufficient" in confidence or "gap" in confidence:
                evidence_gaps.append(aspect)
            elif gap > 0:
                selected_leads.append(aspect)
                reason = reason_by_aspect.get(aspect_raw)
                if reason:
                    selected_reasons.append(reason)
            elif gap < 0:
                competitor_leads.append(aspect)

        headline_parts: list[str] = []

        if selected_leads:
            headline_parts.append(f"users prefer the selected product for {self._join_short(selected_leads)}")
        if competitor_leads:
            headline_parts.append(f"users prefer the competitor for {self._join_short(competitor_leads)}")
        if evidence_gaps:
            headline_parts.append(f"{self._join_short(evidence_gaps)} need more comparable evidence")

        headline = "Benchmark indicates that " + "; ".join(headline_parts) + "." if headline_parts else "Benchmark evidence is still thin."

        selected_summary = (
            f"Users appear to prefer the selected product for {self._join_short(selected_leads)}."
            if selected_leads
            else "No clear selected-product preference is visible yet."
        )
        if selected_reasons:
            selected_summary = f"{selected_summary} {selected_reasons[0]}"

        competitor_summary = (
            f"Users appear to prefer the competitor for {self._join_short(competitor_leads)}."
            if competitor_leads
            else "No clear competitor preference is visible yet."
        )

        risk_summary = (
            f"{self._join_short(evidence_gaps)} have evidence gaps, so they should not be read as confirmed wins or losses."
            if evidence_gaps
            else "Read this benchmark as directional until more comparable reviews are available."
        )

        bullets = []
        if selected_leads:
            bullets.append(f"Selected product preference: {self._join_short(selected_leads)}.")
        if competitor_leads:
            bullets.append(f"Competitor preference: {self._join_short(competitor_leads)}.")
        if evidence_gaps:
            bullets.append(f"Evidence gap: {self._join_short(evidence_gaps)}.")

        return {
            "headline": headline,
            "selected_product_summary": selected_summary,
            "competitor_summary": competitor_summary,
            "risk_summary": risk_summary,
            "bullets": bullets,
            "source": "rules",
        }

    def _signal_label(self, row: dict) -> str:
        confidence = str(row.get("confidence_label") or "").lower()
        gap = float(row.get("gap") or 0)

        if "insufficient" in confidence or "gap" in confidence:
            return "evidence_gap"
        if abs(gap) < 5:
            return "near_parity"
        if gap > 0:
            return "selected_product_preferred"
        return "competitor_preferred"

    def _remove_scores(self, text: str) -> str:
        text = re.sub(r"\b\d+(?:\.\d+)?\s*points?\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\bby\s+\d+(?:\.\d+)?\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip()
        return text

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

    def _clean_text(self, value: Any, fallback: str, max_len: int) -> str:
        text = str(value or "").strip()
        if not text:
            text = fallback

        text = self._remove_scores(text)
        text = re.sub(r"\s+", " ", text).strip()
        text = text.replace("our product", "selected product").replace("Our product", "Selected product")

        if len(text) > max_len:
            text = text[: max_len - 3].rstrip() + "..."

        return text

    def _join_short(self, values: list[str]) -> str:
        cleaned = [value for value in values if value]
        if not cleaned:
            return "none"
        if len(cleaned) == 1:
            return cleaned[0]
        if len(cleaned) == 2:
            return f"{cleaned[0]} and {cleaned[1]}"
        return ", ".join(cleaned[:2]) + f", and {len(cleaned) - 2} more"

    def _labelize(self, value: Any) -> str:
        text = str(value or "unknown").replace("_", " ").strip()
        return " ".join(part.capitalize() for part in text.split())

    def _cache_key(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()