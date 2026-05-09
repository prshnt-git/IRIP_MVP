from app.schemas.review import AspectSentiment, ReviewInput, Sentiment, SignalType


def detect_rating_text_contradiction(review: ReviewInput, aspects: list[AspectSentiment]) -> bool:
    if review.rating is None or not aspects:
        return False
    sentiments = {aspect.sentiment for aspect in aspects}
    if review.rating >= 4 and Sentiment.negative in sentiments and Sentiment.positive not in sentiments:
        return True
    if review.rating <= 2 and Sentiment.positive in sentiments and Sentiment.negative not in sentiments:
        return True
    return False


def compute_quality_score(review: ReviewInput, signal_types: list[SignalType], aspects: list[AspectSentiment], language_confidence: float) -> float:
    score = 0.35
    if review.verified_purchase:
        score += 0.15
    if len(review.raw_text.strip()) >= 20:
        score += 0.12
    if review.helpful_votes and review.helpful_votes > 0:
        score += min(0.1, review.helpful_votes / 100)
    if SignalType.product in signal_types:
        score += 0.12
    if aspects:
        avg_aspect_conf = sum(item.confidence for item in aspects) / len(aspects)
        score += 0.18 * avg_aspect_conf
    score += 0.08 * language_confidence

    if SignalType.delivery in signal_types or SignalType.service in signal_types:
        score -= 0.08

    return round(max(0.0, min(1.0, score)), 3)
