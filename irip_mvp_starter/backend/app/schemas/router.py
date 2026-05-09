from enum import Enum
from pydantic import BaseModel, Field


class TaskName(str, Enum):
    language_detection = "language_detection"
    text_normalization = "text_normalization"
    signal_classification = "signal_classification"
    aspect_sentiment = "aspect_sentiment"
    summarization = "summarization"
    embeddings = "embeddings"
    reranking = "reranking"


class ProviderKind(str, Enum):
    local_rules = "local_rules"
    local_model = "local_model"
    llm_api = "llm_api"
    external_api = "external_api"


class ProviderConfig(BaseModel):
    provider_id: str
    kind: ProviderKind
    enabled: bool = True
    cost_tier: str = "free"
    min_confidence_to_accept: float = Field(default=0.7, ge=0, le=1)
    notes: str | None = None


class RoutingDecision(BaseModel):
    task: TaskName
    selected_provider_id: str
    reason: str
    fallback_provider_ids: list[str] = []
