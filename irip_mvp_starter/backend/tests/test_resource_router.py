from app.schemas.router import TaskName
from app.services.resource_router import ResourceRouter


def test_router_selects_free_local_provider_for_aspect_sentiment():
    router = ResourceRouter()
    decision = router.decide(TaskName.aspect_sentiment)

    assert decision.selected_provider_id == "aspect_rules_v1"
    assert "cost_tier=free" in decision.reason
