from app.pipeline.hybrid_analyzer import HybridReviewAnalyzer
from app.pipeline.review_analyzer import ReviewAnalyzer
from app.schemas.llm import (
    LlmAspectSentiment,
    LlmProviderStatus,
    LlmReviewExtractionResponse,
)
from app.schemas.review import ReviewInput


class FakeEnabledLlmService:
    def status(self):
        return LlmProviderStatus(
            provider="gemini",
            enabled=True,
            model="fake-model",
            reason=None,
        )

    def extract_review_intelligence(self, request):
        return LlmReviewExtractionResponse(
            provider="gemini",
            model="fake-model",
            product_id=request.product_id,
            overall_sentiment="mixed",
            language_profile={
                "primary_language": "hi_en_mixed",
                "script": "roman",
                "hinglish_detected": True,
                "confidence": 0.95,
            },
            product_signal=True,
            delivery_signal=False,
            service_signal=False,
            sarcasm_flag=False,
            contradiction_flag=False,
            aspects=[
                LlmAspectSentiment(
                    aspect="camera",
                    sub_aspect="camera_quality",
                    sentiment="positive",
                    intensity=0.88,
                    confidence=0.91,
                    evidence_span="camera mast hai",
                    reasoning_note="mast is positive",
                ),
                LlmAspectSentiment(
                    aspect="heating",
                    sub_aspect="gaming",
                    sentiment="negative",
                    intensity=0.9,
                    confidence=0.9,
                    evidence_span="haath garam tawa ban jata hai",
                    reasoning_note="heating metaphor",
                ),
            ],
            confidence=0.9,
            raw_model_text=None,
        )


class FakeDisabledLlmService:
    def status(self):
        return LlmProviderStatus(
            provider="gemini",
            enabled=False,
            model="fake-model",
            reason="missing key",
        )


def test_hybrid_analyzer_always_uses_llm(monkeypatch):
    monkeypatch.setenv("IRIP_LLM_MODE", "always")

    analyzer = HybridReviewAnalyzer(
        rule_analyzer=ReviewAnalyzer(),
        llm_service=FakeEnabledLlmService(),
    )

    result = analyzer.analyze(
        ReviewInput(
            review_id="r1",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=3,
            raw_text="Wah kya phone hai, camera mast hai but haath garam tawa ban jata hai",
            verified_purchase=True,
        )
    )

    providers = {item.provider for item in result.aspect_sentiments}
    assert "gemini:fake-model" in providers
    assert any(item.aspect == "heating" for item in result.aspect_sentiments)
    assert any("used LLM" in note for note in result.processing_notes)


def test_hybrid_analyzer_off_uses_rules(monkeypatch):
    monkeypatch.setenv("IRIP_LLM_MODE", "off")

    analyzer = HybridReviewAnalyzer(
        rule_analyzer=ReviewAnalyzer(),
        llm_service=FakeEnabledLlmService(),
    )

    result = analyzer.analyze(
        ReviewInput(
            review_id="r1",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=3,
            raw_text="Bhai camera mast hai but battery backup bekar hai",
            verified_purchase=True,
        )
    )

    providers = {item.provider for item in result.aspect_sentiments}
    assert "aspect_rules_v1" in providers
    assert not any(provider.startswith("gemini") for provider in providers)


def test_hybrid_analyzer_selective_falls_back_when_llm_disabled(monkeypatch):
    monkeypatch.setenv("IRIP_LLM_MODE", "selective")

    analyzer = HybridReviewAnalyzer(
        rule_analyzer=ReviewAnalyzer(),
        llm_service=FakeDisabledLlmService(),
    )

    result = analyzer.analyze(
        ReviewInput(
            review_id="r1",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=3,
            raw_text="Wah kya phone hai, camera mast hai but battery backup bekar hai",
            verified_purchase=True,
        )
    )

    providers = {item.provider for item in result.aspect_sentiments}
    assert "aspect_rules_v1" in providers
    assert not any(provider.startswith("gemini") for provider in providers)


def test_hybrid_analyzer_selective_routes_complex_review_to_llm(monkeypatch):
    monkeypatch.setenv("IRIP_LLM_MODE", "selective")

    analyzer = HybridReviewAnalyzer(
        rule_analyzer=ReviewAnalyzer(),
        llm_service=FakeEnabledLlmService(),
    )

    result = analyzer.analyze(
        ReviewInput(
            review_id="r1",
            product_id="phone_a",
            product_name="Demo Phone A",
            source="flipkart",
            rating=3,
            raw_text="Wah kya phone hai, camera mast hai but haath garam tawa ban jata hai",
            verified_purchase=True,
        )
    )

    providers = {item.provider for item in result.aspect_sentiments}
    assert "gemini:fake-model" in providers