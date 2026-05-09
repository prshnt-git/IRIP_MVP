from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.review import ReviewInput


def _analyze(raw_text: str, rating: float = 3):
    analyzer = ReviewAnalyzer()
    review = ReviewInput(
        review_id="test",
        product_id="phone_a",
        product_name="Demo Phone A",
        source="flipkart",
        rating=rating,
        review_date="2026-04-01",
        raw_text=raw_text,
        verified_purchase=True,
    )
    return analyzer.analyze(review)


def _aspect_map(raw_text: str, rating: float = 3):
    result = _analyze(raw_text, rating=rating)
    return {item.aspect: item.sentiment.value for item in result.aspect_sentiments}


def test_heating_phrase_is_detected():
    aspects = _aspect_map("Phone 10 min gaming ke baad garam tawa ban jata hai", rating=2)

    assert aspects["heating"] == "negative"


def test_ui_lag_maps_to_software_not_performance():
    aspects = _aspect_map("UI lag karta hai aur software bugs bahut hain", rating=2)

    assert aspects["software"] == "negative"
    assert "performance" not in aspects


def test_design_premium_feel_is_positive():
    aspects = _aspect_map("Design premium lagta hai, haath me feel acha hai", rating=4)

    assert aspects["design"] == "positive"
    assert "performance" not in aspects


def test_audio_speaker_call_quality_is_positive():
    aspects = _aspect_map("Speaker loud hai aur call quality clear hai", rating=4)

    assert aspects["audio"] == "positive"


def test_connectivity_network_drop_is_negative():
    aspects = _aspect_map("Network bar bar drop hota hai, internet unstable hai", rating=2)

    assert aspects["connectivity"] == "negative"


def test_display_bright_is_positive():
    aspects = _aspect_map("Display bright hai, outdoor me clearly dikhta hai", rating=4)

    assert aspects["display"] == "positive"


def test_performance_smooth_is_positive():
    aspects = _aspect_map("Performance smooth hai, apps fast open hote hain", rating=5)

    assert aspects["performance"] == "positive"

def test_gaming_heating_context_does_not_create_performance_false_positive():
    aspects = _aspect_map("Phone 10 min gaming ke baad garam tawa ban jata hai", rating=2)

    assert aspects["heating"] == "negative"
    assert "performance" not in aspects