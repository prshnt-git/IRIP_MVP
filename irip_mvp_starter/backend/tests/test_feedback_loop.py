from app.db.repository import ReviewRepository


def test_save_feedback_and_provider_quality(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")

    item = repo.save_extraction_feedback(
        review_id="r1",
        product_id="phone_a",
        aspect="battery",
        predicted_sentiment="negative",
        provider="gemini:fake-model",
        is_correct=True,
        corrected_aspect=None,
        corrected_sentiment=None,
        note="Looks correct",
    )

    assert item["id"] > 0
    assert item["is_correct"] is True

    feedback = repo.list_extraction_feedback(product_id="phone_a")
    assert len(feedback) == 1
    assert feedback[0]["provider"] == "gemini:fake-model"

    quality = repo.get_provider_quality()
    assert quality[0]["provider"] == "gemini:fake-model"
    assert quality[0]["total_feedback"] == 1
    assert quality[0]["correct_count"] == 1
    assert quality[0]["accuracy"] == 1.0