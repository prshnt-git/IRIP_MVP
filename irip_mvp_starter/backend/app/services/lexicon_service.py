from collections import defaultdict
import re
from app.resources.seed_lexicon import SEED_LEXICON
from app.schemas.lexicon import LivingLexiconEntry, LexiconTermType


class LivingLexiconService:
    """In-memory living lexicon for MVP.

    Later this will move to PostgreSQL with analyst approval workflows. Keeping it
    in one service now prevents hardcoded scattered dictionaries.
    """

    def __init__(self, entries: list[LivingLexiconEntry] | None = None) -> None:
        self.entries = entries or SEED_LEXICON
        self._by_term = {entry.term.lower(): entry for entry in self.entries}
        self._by_type: dict[LexiconTermType, list[LivingLexiconEntry]] = defaultdict(list)
        for entry in self.entries:
            self._by_type[entry.term_type].append(entry)

    def find_terms(self, text: str) -> list[LivingLexiconEntry]:
        lowered = text.lower()
        return [entry for term, entry in self._by_term.items() if term in lowered]

    def normalize_text(self, text: str) -> tuple[str, list[str]]:
        normalized = text
        notes: list[str] = []
        for entry in self.entries:
            if entry.term_type != LexiconTermType.spelling_variant:
                continue
            pattern = re.compile(re.escape(entry.term), flags=re.IGNORECASE)
            if pattern.search(normalized):
                normalized = pattern.sub(entry.normalized_term, normalized)
                notes.append(f"normalized '{entry.term}' → '{entry.normalized_term}'")
        return normalized, notes

    def get_aspect_terms(self) -> list[LivingLexiconEntry]:
        return self._by_type[LexiconTermType.aspect] + self._by_type[LexiconTermType.spelling_variant]

    def get_delivery_service_terms(self) -> list[LivingLexiconEntry]:
        return self._by_type[LexiconTermType.delivery_service]

    def get_sentiment_terms(self) -> list[LivingLexiconEntry]:
        return self._by_type[LexiconTermType.sentiment]

    def get_sarcasm_terms(self) -> list[LivingLexiconEntry]:
        return self._by_type[LexiconTermType.sarcasm_marker]
