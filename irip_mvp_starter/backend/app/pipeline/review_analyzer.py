from app.pipeline.aspect_rules import AspectRuleExtractor
from app.pipeline.cleaning import clean_review_text
from app.pipeline.language import detect_language_profile
from app.pipeline.quality import compute_quality_score, detect_rating_text_contradiction
from app.pipeline.signal_classifier import SignalClassifier
from app.schemas.review import ReviewAnalysis, ReviewInput
from app.schemas.router import TaskName
from app.services.lexicon_service import LivingLexiconService
from app.services.resource_router import ResourceRouter


class ReviewAnalyzer:
    def __init__(self, lexicon: LivingLexiconService | None = None, router: ResourceRouter | None = None) -> None:
        self.lexicon = lexicon or LivingLexiconService()
        self.router = router or ResourceRouter()
        self.signal_classifier = SignalClassifier(self.lexicon)
        self.aspect_extractor = AspectRuleExtractor(self.lexicon)

    def analyze(self, review: ReviewInput) -> ReviewAnalysis:
        notes: list[str] = []
        clean_text = clean_review_text(review.raw_text)
        normalized_text, normalization_notes = self.lexicon.normalize_text(clean_text)
        notes.extend(normalization_notes)

        language_profile = detect_language_profile(normalized_text)
        signal_types = self.signal_classifier.classify(normalized_text)

        routing_decision = self.router.decide(TaskName.aspect_sentiment)
        notes.append(routing_decision.reason)

        aspect_sentiments = self.aspect_extractor.extract(normalized_text)
        contradiction_flag = detect_rating_text_contradiction(review, aspect_sentiments)
        if contradiction_flag:
            notes.append("rating-text contradiction detected")

        sarcasm_flag = any(entry.term.lower() in normalized_text.lower() for entry in self.lexicon.get_sarcasm_terms())
        if sarcasm_flag:
            notes.append("sarcasm marker detected; confidence should be treated cautiously")

        quality_score = compute_quality_score(
            review=review,
            signal_types=signal_types,
            aspects=aspect_sentiments,
            language_confidence=float(language_profile.get("confidence", 0.5)),
        )

        return ReviewAnalysis(
            review_id=review.review_id,
            product_id=review.product_id,
            clean_text=normalized_text,
            language_profile=language_profile,
            signal_types=signal_types,
            aspect_sentiments=aspect_sentiments,
            quality_score=quality_score,
            contradiction_flag=contradiction_flag,
            sarcasm_flag=sarcasm_flag,
            processing_notes=notes,
        )
