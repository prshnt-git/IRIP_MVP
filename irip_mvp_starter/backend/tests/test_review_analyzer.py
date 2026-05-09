from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput, Sentiment, SignalType


def test_hinglish_mixed_review_extracts_camera_positive_and_battery_negative():
    analyzer = ReviewAnalyzer()
    review = ReviewInput(
        product_id="phone_a",
        rating=3,
        raw_text="Bhai camera mast hai but battery backup bekar hai",
        verified_purchase=True,
    )

    result = analyzer.analyze(review)
    by_aspect = {item.aspect: item for item in result.aspect_sentiments}

    assert result.language_profile["primary_language"] == "hi_en_mixed"
    assert SignalType.product in result.signal_types
    assert by_aspect["camera"].sentiment == Sentiment.positive
    assert by_aspect["battery"].sentiment == Sentiment.negative
    assert result.quality_score > 0.6


def test_delivery_noise_is_classified_without_breaking_product_signal():
    analyzer = ReviewAnalyzer()
    review = ReviewInput(
        product_id="phone_a",
        rating=4,
        raw_text="Phone display good hai but delivery late thi and packaging damaged tha",
    )

    result = analyzer.analyze(review)

    assert SignalType.product in result.signal_types
    assert SignalType.delivery in result.signal_types
    assert SignalType.packaging in result.signal_types
    assert any(item.aspect == "display" for item in result.aspect_sentiments)


def test_rating_text_contradiction_flag_for_high_rating_negative_text():
    analyzer = ReviewAnalyzer()
    review = ReviewInput(
        product_id="phone_a",
        rating=5,
        raw_text="Battery backup bakwas hai aur phone garam hota hai",
    )

    result = analyzer.analyze(review)

    assert result.contradiction_flag is True
    assert any("contradiction" in note for note in result.processing_notes)


def test_spelling_variant_normalization_for_camera():
    analyzer = ReviewAnalyzer()
    review = ReviewInput(product_id="phone_a", raw_text="Camra zabardast hai")

    result = analyzer.analyze(review)

    assert "camera" in result.clean_text.lower()
    assert any(item.aspect == "camera" and item.sentiment == Sentiment.positive for item in result.aspect_sentiments)
