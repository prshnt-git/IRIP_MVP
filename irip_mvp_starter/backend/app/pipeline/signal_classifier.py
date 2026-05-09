from app.schemas.review import SignalType
from app.services.lexicon_service import LivingLexiconService


class SignalClassifier:
    def __init__(self, lexicon: LivingLexiconService) -> None:
        self.lexicon = lexicon

    def classify(self, text: str) -> list[SignalType]:
        lowered = text.lower()
        signals: set[SignalType] = set()

        if any(entry.term.lower() in lowered for entry in self.lexicon.get_aspect_terms()):
            signals.add(SignalType.product)

        delivery_terms = [entry.term.lower() for entry in self.lexicon.get_delivery_service_terms()]
        if any(term in lowered for term in delivery_terms):
            if "packag" in lowered or "box" in lowered:
                signals.add(SignalType.packaging)
            if "seller" in lowered:
                signals.add(SignalType.seller)
            if "return" in lowered or "refund" in lowered or "warranty" in lowered or "service" in lowered:
                signals.add(SignalType.service)
            if "delivery" in lowered or "courier" in lowered:
                signals.add(SignalType.delivery)

        return sorted(signals or {SignalType.unclear}, key=lambda value: value.value)
