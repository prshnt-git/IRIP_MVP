from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from app.db.database import connect
from app.services.trusted_news_sources import TrustedNewsSourceService


SMARTPHONE_TERMS = {
    "smartphone",
    "android",
    "phone",
    "mobile",
    "handset",
    "device",
    "oem",
    "camera",
    "battery",
    "display",
    "charging",
    "chipset",
    "mediatek",
    "qualcomm",
    "snapdragon",
    "dimensity",
    "samsung",
    "xiaomi",
    "oppo",
    "vivo",
    "honor",
    "transsion",
    "tecno",
    "infinix",
    "itel",
}

AI_TERMS = {
    "ai",
    "artificial intelligence",
    "genai",
    "generative ai",
    "llm",
    "on-device",
    "edge ai",
    "npu",
    "assistant",
    "gemini",
    "camera ai",
    "multimodal",
}

MARKET_TERMS = {
    "shipment",
    "market share",
    "launch",
    "pricing",
    "sales",
    "forecast",
    "growth",
    "emerging market",
    "india",
    "africa",
    "middle east",
    "latin america",
}

OEM_COMPANY_TERMS = {
    "samsung",
    "xiaomi",
    "oppo",
    "vivo",
    "honor",
    "transsion",
    "tecno",
    "infinix",
    "itel",
    "realme",
    "motorola",
    "apple",
}

TRANSION_RELEVANT_REGIONS = {
    "india",
    "africa",
    "middle east",
    "mea",
    "latin america",
    "latam",
    "pakistan",
    "bangladesh",
    "nigeria",
    "kenya",
    "egypt",
}

HIGH_VALUE_TECH_TERMS = {
    "on-device",
    "edge ai",
    "npu",
    "llm",
    "multimodal",
    "camera ai",
    "generative ai",
    "chipset",
    "snapdragon",
    "dimensity",
    "android",
    "ai assistant",
}

class NewsIngestionService:
    def __init__(
        self,
        database_path,
        source_service: TrustedNewsSourceService,
    ) -> None:
        self.database_path = database_path
        self.source_service = source_service

    def ingest_rss_url(
        self,
        source_id: str,
        rss_url: str,
        discovered_via: str = "rss",
        max_items: int = 20,
    ) -> dict:
        source = self._require_source(source_id)

        if not self.source_service.validate_url_for_source(source, rss_url):
            raise ValueError(
                f"RSS URL domain is not trusted for source_id={source_id}."
            )

        try:
            with urlopen(rss_url, timeout=20) as response:
                rss_xml = response.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError, UnicodeDecodeError) as error:
            raise ValueError(f"Unable to fetch RSS URL: {error}") from error

        return self.ingest_rss_xml(
            source_id=source_id,
            rss_xml=rss_xml,
            discovered_via=discovered_via,
            max_items=max_items,
        )

    def ingest_rss_xml(
        self,
        source_id: str,
        rss_xml: str,
        discovered_via: str = "rss_fixture",
        max_items: int = 20,
    ) -> dict:
        source = self._require_source(source_id)
        parsed_items = self._parse_rss_xml(rss_xml)[:max_items]

        inserted: list[dict] = []
        skipped_duplicate_count = 0
        rejected_count = 0

        with connect(self.database_path) as connection:
            for item in parsed_items:
                canonical_url = item["canonical_url"]

                if not canonical_url or not self.source_service.validate_url_for_source(
                    source, canonical_url
                ):
                    rejected_count += 1
                    continue

                tags = self._classify_item(
                    title=item["title"],
                    summary=item.get("summary") or "",
                    default_tags=source["default_tags"],
                )

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO news_items (
                    source_id,
                    source_name,
                    source_tier,
                    title,
                    canonical_url,
                    published_at,
                    summary,
                    discovered_via,
                    topic_tags_json,
                    company_tags_json,
                    technology_tags_json,
                    region_tags_json,
                    relevance_score,
                    priority_label,
                    why_it_matters,
                    evidence_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                (
                    source["source_id"],
                    source["source_name"],
                    source["source_tier"],
                    item["title"],
                    canonical_url,
                    item.get("published_at"),
                    item.get("summary"),
                    discovered_via,
                    json.dumps(tags["topic_tags"]),
                    json.dumps(tags["company_tags"]),
                    json.dumps(tags["technology_tags"]),
                    json.dumps(tags["region_tags"]),
                    tags["relevance_score"],
                    tags["priority_label"],
                    tags["why_it_matters"],
                    canonical_url,
                ),
                )

                if cursor.rowcount:
                    row = connection.execute(
                        """
                        SELECT *
                        FROM news_items
                        WHERE canonical_url = ?
                        """,
                        (canonical_url,),
                    ).fetchone()
                    inserted.append(self._row_to_dict(row))
                else:
                    skipped_duplicate_count += 1

        return {
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "inserted_count": len(inserted),
            "skipped_duplicate_count": skipped_duplicate_count,
            "rejected_count": rejected_count,
            "items": inserted,
        }

    def list_news_items(
        self,
        source_id: str | None = None,
        min_relevance_score: float | None = None,
        limit: int = 50,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []

        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)

        if min_relevance_score is not None:
            clauses.append("relevance_score >= ?")
            params.append(min_relevance_score)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT *
                FROM news_items
                {where_sql}
                ORDER BY relevance_score DESC, published_at DESC, ingested_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [self._row_to_dict(row) for row in rows]

    def rescore_news_items(
        self,
        source_id: str | None = None,
        limit: int = 500,
    ) -> dict:
        clauses = []
        params: list[object] = []

        if source_id:
            clauses.append("source_id = ?")
            params.append(source_id)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        updated_count = 0

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    id,
                    source_id,
                    source_name,
                    source_tier,
                    title,
                    canonical_url,
                    published_at,
                    summary,
                    discovered_via,
                    topic_tags_json,
                    company_tags_json,
                    technology_tags_json,
                    region_tags_json,
                    relevance_score,
                    priority_label,
                    why_it_matters,
                    evidence_url,
                    ingested_at
                FROM news_items
                {where_sql}
                ORDER BY ingested_at DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

            for row in rows:
                source = self.source_service.get_source(row["source_id"])
                if source is None:
                    continue

                tags = self._classify_item(
                    title=row["title"],
                    summary=row["summary"] or "",
                    default_tags=source["default_tags"],
                )

                connection.execute(
                    """
                    UPDATE news_items
                    SET
                        topic_tags_json = ?,
                        company_tags_json = ?,
                        technology_tags_json = ?,
                        region_tags_json = ?,
                        relevance_score = ?,
                        priority_label = ?,
                        why_it_matters = ?
                    WHERE id = ?
                    """,
                    (
                        json.dumps(tags["topic_tags"]),
                        json.dumps(tags["company_tags"]),
                        json.dumps(tags["technology_tags"]),
                        json.dumps(tags["region_tags"]),
                        tags["relevance_score"],
                        tags["priority_label"],
                        tags["why_it_matters"],
                        row["id"],
                    ),
                )
                updated_count += 1

        return {
            "updated_count": updated_count,
            "source_id": source_id,
            "limit": limit,
        }

    def _require_source(self, source_id: str) -> dict:
        source = self.source_service.get_source(source_id)

        if source is None or not source["is_active"]:
            raise ValueError(f"Unknown or inactive trusted news source: {source_id}")

        return source

    def _parse_rss_xml(self, rss_xml: str) -> list[dict]:
        root = ET.fromstring(rss_xml)
        items = root.findall(".//item")

        parsed: list[dict] = []

        for item in items:
            title = self._text(item, "title")
            link = self._text(item, "link")
            description = self._text(item, "description")
            pub_date = self._text(item, "pubDate")

            if not title or not link:
                continue

            parsed.append(
                {
                    "title": self._clean_text(title),
                    "canonical_url": link.strip(),
                    "summary": self._clean_text(description) if description else None,
                    "published_at": self._parse_date(pub_date),
                }
            )

        return parsed

    def _text(self, item, tag: str) -> str | None:
        element = item.find(tag)
        if element is None or element.text is None:
            return None
        return element.text.strip()

    def _clean_text(self, value: str) -> str:
        value = unescape(value)
        value = re.sub(r"<[^>]+>", " ", value)
        value = re.sub(r"\s+", " ", value)
        return value.strip()

    def _parse_date(self, value: str | None) -> str | None:
        if not value:
            return None

        try:
            parsed = parsedate_to_datetime(value)
            return parsed.date().isoformat()
        except (TypeError, ValueError, IndexError):
            return None

    def _classify_item(
        self,
        title: str,
        summary: str,
        default_tags: list[str],
    ) -> dict:
        text = f"{title} {summary}".lower()

        topic_tags = set(default_tags)
        company_tags: set[str] = set()
        technology_tags: set[str] = set()
        region_tags: set[str] = set()
        reasons: list[str] = []

        relevance_score = 0.0

        for term in SMARTPHONE_TERMS:
            if term in text:
                relevance_score += 7
                topic_tags.add("smartphone")

                if term in OEM_COMPANY_TERMS or term in {"qualcomm", "mediatek"}:
                    company_tags.add(term)

                if term in {"chipset", "snapdragon", "dimensity", "npu"}:
                    technology_tags.add(term)

        for term in AI_TERMS:
            if term in text:
                relevance_score += 10
                topic_tags.add("ai")
                technology_tags.add(term.replace(" ", "_"))

        for term in MARKET_TERMS:
            if term in text:
                relevance_score += 5
                topic_tags.add("market")

        for term in OEM_COMPANY_TERMS:
            if term in text:
                company_tags.add(term)
                topic_tags.add("oem")
                relevance_score += 8

        for term in HIGH_VALUE_TECH_TERMS:
            if term in text:
                technology_tags.add(term.replace(" ", "_"))
                relevance_score += 12

        for term in TRANSION_RELEVANT_REGIONS:
            if term in text:
                region_tags.add(term.replace(" ", "_"))
                relevance_score += 7

        # Official/primary sources should be trusted more, but not blindly.
        source_trust_bonus = 0
        if "official" in " ".join(default_tags).lower():
            source_trust_bonus = 0

        # Strong MICI combinations.
        has_ai = "ai" in topic_tags or any("ai" in item for item in technology_tags)
        has_smartphone = "smartphone" in topic_tags
        has_chipset = bool({"chipset", "snapdragon", "dimensity", "npu"} & technology_tags)
        has_oem = bool(company_tags & OEM_COMPANY_TERMS)
        has_region = bool(region_tags)

        if has_ai and has_smartphone:
            relevance_score += 18
            reasons.append("AI + smartphone signal")

        if has_chipset and (has_ai or has_smartphone):
            relevance_score += 15
            reasons.append("chipset/platform signal relevant to OEM product planning")

        if has_oem:
            relevance_score += 10
            reasons.append("OEM/competitor ecosystem signal")

        if has_region:
            relevance_score += 8
            reasons.append("emerging-market / regional relevance")

        # Default tags should not overpower content, but can add context.
        if "edge_ai" in default_tags:
            relevance_score += 4
        if "smartphone" in default_tags:
            relevance_score += 4
        if "market" in default_tags:
            relevance_score += 3

        relevance_score = min(round(relevance_score + source_trust_bonus, 2), 100.0)
        priority_label = self._priority_label(relevance_score)
        why_it_matters = self._why_it_matters(
            title=title,
            topic_tags=topic_tags,
            company_tags=company_tags,
            technology_tags=technology_tags,
            region_tags=region_tags,
            reasons=reasons,
            relevance_score=relevance_score,
        )

        return {
            "topic_tags": sorted(topic_tags),
            "company_tags": sorted(company_tags),
            "technology_tags": sorted(technology_tags),
            "region_tags": sorted(region_tags),
            "relevance_score": relevance_score,
            "priority_label": priority_label,
            "why_it_matters": why_it_matters,
        }


    def _priority_label(self, relevance_score: float) -> str:
        if relevance_score >= 70:
            return "high"
        if relevance_score >= 35:
            return "medium"
        return "low"

    def _why_it_matters(
        self,
        title: str,
        topic_tags: set[str],
        company_tags: set[str],
        technology_tags: set[str],
        region_tags: set[str],
        reasons: list[str],
        relevance_score: float,
    ) -> str:
        if not reasons:
            if relevance_score < 20:
                return "Low immediate MICI relevance; keep only for background awareness."
            return "Relevant technology or market signal; review for possible OEM implications."

        reason_text = "; ".join(reasons[:3])

        if company_tags:
            company_text = f"Company signal: {', '.join(sorted(company_tags)[:4])}."
        else:
            company_text = "No direct company signal detected."

        if technology_tags:
            tech_text = f"Technology signal: {', '.join(sorted(technology_tags)[:5])}."
        else:
            tech_text = "No specific technology signal detected."

        if region_tags:
            region_text = f"Region signal: {', '.join(sorted(region_tags)[:4])}."
        else:
            region_text = "No specific priority-region signal detected."

        return (
            f"{reason_text}. {company_text} {tech_text} {region_text} "
            "Use this as an evidence-linked item for smartphone/OEM market intelligence."
        )

    def _row_to_dict(self, row) -> dict:
        return {
            "id": int(row["id"]),
            "source_id": row["source_id"],
            "source_name": row["source_name"],
            "source_tier": int(row["source_tier"]),
            "title": row["title"],
            "canonical_url": row["canonical_url"],
            "published_at": row["published_at"],
            "summary": row["summary"],
            "discovered_via": row["discovered_via"],
            "topic_tags": json.loads(row["topic_tags_json"] or "[]"),
            "company_tags": json.loads(row["company_tags_json"] or "[]"),
            "technology_tags": json.loads(row["technology_tags_json"] or "[]"),
            "region_tags": json.loads(row["region_tags_json"] or "[]"),
            "relevance_score": float(row["relevance_score"] or 0.0),
            "priority_label": row["priority_label"],
            "why_it_matters": row["why_it_matters"],
            "evidence_url": row["evidence_url"],
            "ingested_at": row["ingested_at"],
        }