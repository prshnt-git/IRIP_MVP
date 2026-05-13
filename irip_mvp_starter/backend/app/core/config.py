from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for IRIP.

    The MVP must run without paid APIs. API keys are optional and only used by
    fallback providers when explicitly configured.
    """

    app_name: str = "IRIP MVP API"
    app_version: str = "0.1.0"
    environment: str = "local"
    gemini_api_key: str | None = None
    # Comma-separated list of Gemini API keys for round-robin rotation.
    # Takes priority over gemini_api_key when set.
    # Example: GEMINI_API_KEYS=AIza...key1,AIza...key2,AIza...key3
    gemini_api_keys: str | None = None
    database_path: str = "data/irip_mvp.db"
    # PostgreSQL connection URL for Supabase (Phase 4 migration).
    # Format: postgresql+psycopg2://user:password@host:5432/dbname
    # When set, get_engine() and get_session() in database.py use PostgreSQL.
    # When unset, the existing SQLite path is used unchanged.
    database_url: str | None = None

    # LLM pipeline settings — mirrored from env vars used across the codebase.
    irip_llm_mode: str = "selective"
    irip_llm_provider: str = "gemini"

    # Secret keys for protected endpoints.
    pipeline_secret_key: str = ""
    debug_secret_key: str = ""

    # Data acquisition settings. These do not enable scraping by themselves;
    # they let us plug in compliant marketplace/API adapters later without
    # changing the core import, normalization, or deduplication pipeline.
    enable_marketplace_acquisition: bool = False
    amazon_provider_api_key: str | None = None
    flipkart_provider_api_key: str | None = None
    third_party_review_provider: str | None = None
    third_party_review_api_key: str | None = None
    review_import_max_mb: int = 5
    catalog_import_max_mb: int = 2

    # Brand ownership defaults for Transsion-focused analysis.
    own_brand_names_csv: str = "tecno,infinix,itel"
    competitor_brand_names_csv: str = (
        "samsung,xiaomi,redmi,poco,realme,vivo,oppo,motorola,oneplus,iqoo,nothing,lava"
    )

    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    @property
    def own_brand_names(self) -> set[str]:
        return _csv_to_normalized_set(self.own_brand_names_csv)

    @property
    def competitor_brand_names(self) -> set[str]:
        return _csv_to_normalized_set(self.competitor_brand_names_csv)


def _csv_to_normalized_set(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip().lower() for item in value.split(",") if item.strip()}


@lru_cache
def get_settings() -> Settings:
    return Settings()
