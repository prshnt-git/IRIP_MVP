from __future__ import annotations

import json
from urllib.parse import urlparse

from app.db.database import connect


TRUSTED_NEWS_SOURCES = [
    {
        "source_id": "qualcomm_newsroom",
        "source_name": "Qualcomm Newsroom",
        "source_tier": 1,
        "source_type": "official_company",
        "allowed_domains": ["qualcomm.com"],
        "default_tags": ["chipset", "edge_ai", "smartphone"],
    },
    {
        "source_id": "mediatek_newsroom",
        "source_name": "MediaTek Newsroom",
        "source_tier": 1,
        "source_type": "official_company",
        "allowed_domains": ["mediatek.com"],
        "default_tags": ["chipset", "smartphone", "edge_ai"],
    },
    {
        "source_id": "android_developers",
        "source_name": "Android Developers Blog",
        "source_tier": 1,
        "source_type": "official_platform",
        "allowed_domains": ["android-developers.googleblog.com", "googleblog.com"],
        "default_tags": ["android", "software", "ai"],
    },
    {
        "source_id": "google_ai",
        "source_name": "Google AI",
        "source_tier": 1,
        "source_type": "official_research",
        "allowed_domains": ["ai.google.dev", "blog.google", "googleblog.com"],
        "default_tags": ["ai", "models", "developer"],
    },
    {
        "source_id": "samsung_newsroom",
        "source_name": "Samsung Newsroom",
        "source_tier": 1,
        "source_type": "official_company",
        "allowed_domains": ["news.samsung.com", "samsung.com"],
        "default_tags": ["smartphone", "oem", "ai"],
    },
    {
        "source_id": "reuters_technology",
        "source_name": "Reuters Technology",
        "source_tier": 2,
        "source_type": "professional_news",
        "allowed_domains": ["reuters.com"],
        "default_tags": ["market", "technology"],
    },
    {
        "source_id": "gsarena",
        "source_name": "GSMArena",
        "source_tier": 2,
        "source_type": "specialist_media",
        "allowed_domains": ["gsmarena.com"],
        "default_tags": ["smartphone", "launch", "specs"],
    },
    {
        "source_id": "the_verge",
        "source_name": "The Verge",
        "source_tier": 2,
        "source_type": "technology_media",
        "allowed_domains": ["theverge.com"],
        "default_tags": ["technology", "consumer"],
    },
]


class TrustedNewsSourceService:
    def __init__(self, database_path) -> None:
        self.database_path = database_path
        self.seed_sources()

    def seed_sources(self) -> None:
        with connect(self.database_path) as connection:
            for source in TRUSTED_NEWS_SOURCES:
                connection.execute(
                    """
                    INSERT INTO trusted_news_sources (
                        source_id,
                        source_name,
                        source_tier,
                        source_type,
                        allowed_domains_json,
                        default_tags_json,
                        is_active
                    ) VALUES (?, ?, ?, ?, ?, ?, 1)
                    ON CONFLICT(source_id) DO UPDATE SET
                        source_name = excluded.source_name,
                        source_tier = excluded.source_tier,
                        source_type = excluded.source_type,
                        allowed_domains_json = excluded.allowed_domains_json,
                        default_tags_json = excluded.default_tags_json,
                        is_active = 1,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        source["source_id"],
                        source["source_name"],
                        source["source_tier"],
                        source["source_type"],
                        json.dumps(source["allowed_domains"]),
                        json.dumps(source["default_tags"]),
                    ),
                )

    def list_sources(self) -> list[dict]:
        with connect(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    source_id,
                    source_name,
                    source_tier,
                    source_type,
                    allowed_domains_json,
                    default_tags_json,
                    is_active
                FROM trusted_news_sources
                ORDER BY source_tier ASC, source_name ASC
                """
            ).fetchall()

        return [self._row_to_dict(row) for row in rows]

    def get_source(self, source_id: str) -> dict | None:
        with connect(self.database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    source_id,
                    source_name,
                    source_tier,
                    source_type,
                    allowed_domains_json,
                    default_tags_json,
                    is_active
                FROM trusted_news_sources
                WHERE source_id = ?
                """,
                (source_id,),
            ).fetchone()

        return self._row_to_dict(row) if row else None

    def validate_url_for_source(self, source: dict, url: str) -> bool:
        hostname = (urlparse(url).hostname or "").lower()
        if not hostname:
            return False

        return any(
            hostname == domain or hostname.endswith(f".{domain}")
            for domain in source["allowed_domains"]
        )

    def _row_to_dict(self, row) -> dict:
        return {
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "source_tier": int(row["source_tier"]),
            "source_type": row["source_type"],
            "allowed_domains": json.loads(row["allowed_domains_json"] or "[]"),
            "default_tags": json.loads(row["default_tags_json"] or "[]"),
            "is_active": bool(row["is_active"]),
        }