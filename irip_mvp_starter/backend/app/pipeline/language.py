import re

DEVANAGARI_RE = re.compile(r"[\u0900-\u097F]")
HINGLISH_HINTS = {
    "hai", "nahi", "bahut", "ekdum", "mast", "bekar", "bakwas", "paisa",
    "vasool", "garam", "jaldi", "khatam", "theek", "acha", "bura", "bhai",
}


def detect_language_profile(text: str) -> dict:
    tokens = {token.lower() for token in re.findall(r"[A-Za-z\u0900-\u097F]+", text)}
    has_devanagari = bool(DEVANAGARI_RE.search(text))
    hinglish_hits = sorted(tokens.intersection(HINGLISH_HINTS))

    if has_devanagari and hinglish_hits:
        primary = "hi_en_mixed"
        script = "mixed"
        confidence = 0.82
    elif has_devanagari:
        primary = "hi_or_indic"
        script = "devanagari"
        confidence = 0.78
    elif hinglish_hits:
        primary = "hi_en_mixed"
        script = "roman"
        confidence = min(0.95, 0.62 + 0.04 * len(hinglish_hits))
    else:
        primary = "en_or_unknown"
        script = "latin"
        confidence = 0.62

    return {
        "primary_language": primary,
        "script": script,
        "hinglish_hints": hinglish_hits,
        "confidence": round(confidence, 2),
    }
