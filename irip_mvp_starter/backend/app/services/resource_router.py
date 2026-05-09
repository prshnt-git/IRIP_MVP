from app.schemas.router import ProviderConfig, ProviderKind, RoutingDecision, TaskName


DEFAULT_PROVIDERS: dict[TaskName, list[ProviderConfig]] = {
    TaskName.language_detection: [
        ProviderConfig(provider_id="local_script_rules_v1", kind=ProviderKind.local_rules, min_confidence_to_accept=0.65, notes="Free baseline: script + Hinglish token hints"),
    ],
    TaskName.text_normalization: [
        ProviderConfig(provider_id="living_lexicon_v1", kind=ProviderKind.local_rules, min_confidence_to_accept=0.7, notes="Free baseline using approved lexicon entries"),
    ],
    TaskName.signal_classification: [
        ProviderConfig(provider_id="signal_rules_v1", kind=ProviderKind.local_rules, min_confidence_to_accept=0.72, notes="Separates product vs delivery/service noise"),
    ],
    TaskName.aspect_sentiment: [
        ProviderConfig(provider_id="aspect_rules_v1", kind=ProviderKind.local_rules, min_confidence_to_accept=0.68, notes="Explainable free aspect/sentiment extraction"),
        ProviderConfig(provider_id="gemini_flash_structured_v1", kind=ProviderKind.llm_api, enabled=False, cost_tier="low", min_confidence_to_accept=0.82, notes="Optional fallback for ambiguous Hinglish/sarcasm"),
    ],
    TaskName.summarization: [
        ProviderConfig(provider_id="structured_summary_rules_v1", kind=ProviderKind.local_rules, min_confidence_to_accept=0.7),
        ProviderConfig(provider_id="gemini_pro_grounded_v1", kind=ProviderKind.llm_api, enabled=False, cost_tier="medium", min_confidence_to_accept=0.9),
    ],
    TaskName.embeddings: [
        ProviderConfig(provider_id="local_multilingual_minilm_placeholder", kind=ProviderKind.local_model, enabled=False, cost_tier="free", notes="Enable after dependency added"),
    ],
    TaskName.reranking: [
        ProviderConfig(provider_id="keyword_evidence_ranker_v1", kind=ProviderKind.local_rules, min_confidence_to_accept=0.65),
    ],
}


class ResourceRouter:
    """Provider-agnostic task router.

    It does not execute providers. It selects the best enabled provider for each task.
    This keeps the platform future-proof as better free APIs/models appear.
    """

    def __init__(self, providers: dict[TaskName, list[ProviderConfig]] | None = None) -> None:
        self.providers = providers or DEFAULT_PROVIDERS

    def decide(self, task: TaskName, require_llm: bool = False) -> RoutingDecision:
        candidates = self.providers.get(task, [])
        enabled = [provider for provider in candidates if provider.enabled]
        if require_llm:
            enabled = [provider for provider in enabled if provider.kind == ProviderKind.llm_api]

        if not enabled:
            raise ValueError(f"No enabled provider configured for task: {task}")

        selected = enabled[0]
        fallbacks = [provider.provider_id for provider in enabled[1:]]
        reason = f"Selected {selected.provider_id} for {task.value}; cost_tier={selected.cost_tier}."
        return RoutingDecision(task=task, selected_provider_id=selected.provider_id, reason=reason, fallback_provider_ids=fallbacks)

    def list_providers(self) -> dict[str, list[ProviderConfig]]:
        return {task.value: providers for task, providers in self.providers.items()}
