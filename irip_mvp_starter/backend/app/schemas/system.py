from __future__ import annotations

from pydantic import BaseModel


class SystemVersionResponse(BaseModel):
    product_name: str
    product_version: str
    api_version: str
    stage: str
    description: str
    locked_capabilities: list[str]
    next_recommended_focus: list[str]


class ReadinessCheckItem(BaseModel):
    name: str
    status: str
    detail: str


class SystemReadinessResponse(BaseModel):
    product_version: str
    readiness_status: str
    checks: list[ReadinessCheckItem]
    warnings: list[str]
    recommended_next_actions: list[str]