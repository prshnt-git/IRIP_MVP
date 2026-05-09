from app.schemas.llm import LlmReviewExtractionRequest
from app.services.llm_service import LlmService


class FakeLlmService(LlmService):
    def __init__(self) -> None:
        super().__init__()
        self.provider = "gemini"
        self.gemini_api_key = "fake-key"
        self.gemini_model = "fake-model"

    def status(self):
        # Keep fake model stable for this unit test.
        from app.schemas.llm import LlmProviderStatus

        return LlmProviderStatus(
            provider="gemini",
            enabled=True,
            model="fake-model",
            mode="selective",
            reason=None,
        )

    def _call_gemini(self, prompt: str) -> str:
        assert "Camera mast hai" in prompt
        return """
        {
          "overall_sentiment": "mixed",
          "language_profile": {
            "primary_language": "hi_en_mixed",
            "script": "roman",
            "hinglish_detected": true,
            "confidence": 0.92
          },
          "product_signal": true,
          "delivery_signal": false,
          "service_signal": false,
          "sarcasm_flag": false,
          "contradiction_flag": false,
          "aspects": [
            {
              "aspect": "camera",
              "sub_aspect": "camera_quality",
              "sentiment": "positive",
              "intensity": 0.86,
              "confidence": 0.9,
              "evidence_span": "Camera mast hai",
              "reasoning_note": "mast indicates positive sentiment"
            },
            {
              "aspect": "battery",
              "sub_aspect": "battery_life",
              "sentiment": "negative",
              "intensity": 0.82,
              "confidence": 0.88,
              "evidence_span": "battery bekar",
              "reasoning_note": "bekar indicates negative sentiment"
            }
          ],
          "confidence": 0.89
        }
        """


def test_llm_review_extraction_with_fake_provider():
    service = FakeLlmService()
    result = service.extract_review_intelligence(
        LlmReviewExtractionRequest(
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=3,
            raw_text="Camera mast hai but battery bekar",
        )
    )

    assert result.provider == "gemini"
    assert result.model == "fake-model"
    assert result.overall_sentiment == "mixed"
    assert result.language_profile["primary_language"] == "hi_en_mixed"
    assert len(result.aspects) == 2
    assert result.aspects[0].aspect == "camera"
    assert result.aspects[1].aspect == "battery"