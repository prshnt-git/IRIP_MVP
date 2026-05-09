import html
import re
import unicodedata

ZERO_WIDTH_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
WHITESPACE_RE = re.compile(r"\s+")
HTML_TAG_RE = re.compile(r"<[^>]+>")


def clean_review_text(raw_text: str) -> str:
    """Clean review text while preserving emojis and Indian language signal."""
    text = html.unescape(raw_text)
    text = unicodedata.normalize("NFC", text)
    text = ZERO_WIDTH_RE.sub("", text)
    text = HTML_TAG_RE.sub(" ", text)
    text = text.replace("READ MORE", " ").replace("Certified Buyer", " ")
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text
