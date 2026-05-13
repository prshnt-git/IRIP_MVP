from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from pathlib import Path
from typing import Iterable

from app.db.database import connect, init_db
from app.resources.seed_lexicon import SEED_LEXICON
from app.schemas.review import ReviewAnalysis, ReviewInput, Sentiment


class ReviewRepository:
    """Small SQLite repository for the MVP.

    The repository intentionally stores raw and processed outputs separately so we
    can reprocess historical reviews when the router, lexicon, or models improve.
    """

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = Path(database_path)
        init_db(self.database_path)
        self.seed_lexicon()

    def seed_lexicon(self) -> None:
        with connect(self.database_path) as connection:
            for entry in SEED_LEXICON:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO living_lexicon (
                        term, normalized_term, term_type, language_type, aspect,
                        sentiment_prior, intensity, confidence, source,
                        approved_by_human, examples_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry.term.lower(),
                        entry.normalized_term,
                        entry.term_type.value,
                        entry.language_type,
                        entry.aspect,
                        entry.sentiment_prior,
                        entry.intensity,
                        entry.confidence,
                        entry.source,
                        int(entry.approved_by_human),
                        json.dumps(entry.examples),
                    ),
                )

    def save_review_analysis(
        self,
        review: ReviewInput,
        analysis: ReviewAnalysis,
        source_metadata: dict | None = None,
    ) -> dict:
        """Persist a review analysis with source-aware duplicate protection.

        The canonical review is stored once in reviews_raw/reviews_processed.
        If the same review is later discovered through another CSV/provider/page,
        we attach that source in review_sources instead of counting the review again.
        """

        metadata = source_metadata or {}
        review_id = analysis.review_id or review.review_id
        if not review_id:
            raise ValueError("Cannot persist analysis without review_id")

        raw_text = review.raw_text or ""
        exact_text_hash = _hash_text(raw_text)
        normalized_text_hash = _hash_text(_normalize_review_text(raw_text))
        product_review_fingerprint = _hash_text(
            "|".join(
                [
                    review.product_id,
                    str(review.rating or ""),
                    str(review.review_date or ""),
                    _normalize_review_text(raw_text),
                ]
            )
        )

        marketplace = _first_present(
            metadata.get("marketplace"),
            getattr(review, "marketplace", None),
            review.source,
        )
        marketplace_product_id = _first_present(
            metadata.get("marketplace_product_id"),
            getattr(review, "marketplace_product_id", None),
        )
        marketplace_review_id = _first_present(
            metadata.get("marketplace_review_id"),
            getattr(review, "marketplace_review_id", None),
        )
        source_url = _first_present(
            metadata.get("source_url"),
            getattr(review, "source_url", None),
        )
        reviewer_hash = _first_present(
            metadata.get("reviewer_hash"),
            getattr(review, "reviewer_hash", None),
        )
        review_title = _first_present(
            metadata.get("review_title"),
            getattr(review, "review_title", None),
        )
        discovered_via = _first_present(metadata.get("discovered_via"), review.source, "manual_import")
        source_review_key = _build_source_review_key(
            source=review.source,
            marketplace=marketplace,
            product_id=review.product_id,
            marketplace_product_id=marketplace_product_id,
            marketplace_review_id=marketplace_review_id,
            reviewer_hash=reviewer_hash,
            review_date=review.review_date,
            normalized_text_hash=normalized_text_hash,
        )

        with connect(self.database_path) as connection:
            existing_source = connection.execute(
                """
                SELECT canonical_review_id
                FROM review_sources
                WHERE source_review_key = ?
                LIMIT 1
                """,
                (source_review_key,),
            ).fetchone()

            if existing_source is not None:
                canonical_review_id = existing_source["canonical_review_id"]
                self._attach_review_source(
                    connection=connection,
                    canonical_review_id=canonical_review_id,
                    product_id=review.product_id,
                    source_review_key=source_review_key,
                    source=review.source,
                    marketplace=marketplace,
                    marketplace_product_id=marketplace_product_id,
                    marketplace_review_id=marketplace_review_id,
                    source_url=source_url,
                    reviewer_hash=reviewer_hash,
                    discovered_via=discovered_via,
                )
                return {
                    "status": "duplicate_source_attached",
                    "review_id": canonical_review_id,
                    "canonical_review_id": canonical_review_id,
                    "duplicate": True,
                    "duplicate_type": "source_review_key",
                }

            duplicate = connection.execute(
                """
                SELECT review_id, product_id
                FROM reviews_raw
                WHERE product_review_fingerprint = ?
                   OR (product_id = ? AND normalized_text_hash = ? AND COALESCE(rating, -1) = COALESCE(?, -1))
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (
                    product_review_fingerprint,
                    review.product_id,
                    normalized_text_hash,
                    review.rating,
                ),
            ).fetchone()

            if duplicate is not None and duplicate["review_id"] != review_id:
                canonical_review_id = duplicate["review_id"]
                self._attach_review_source(
                    connection=connection,
                    canonical_review_id=canonical_review_id,
                    product_id=review.product_id,
                    source_review_key=source_review_key,
                    source=review.source,
                    marketplace=marketplace,
                    marketplace_product_id=marketplace_product_id,
                    marketplace_review_id=marketplace_review_id,
                    source_url=source_url,
                    reviewer_hash=reviewer_hash,
                    discovered_via=discovered_via,
                )
                connection.execute(
                    """
                    INSERT OR IGNORE INTO review_duplicate_candidates (
                        incoming_review_id, canonical_review_id, product_id,
                        duplicate_type, confidence, reason
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        review_id,
                        canonical_review_id,
                        review.product_id,
                        "normalized_text_product_rating",
                        0.98,
                        "Same normalized text/rating/product already exists; source attached to canonical review.",
                    ),
                )
                return {
                    "status": "duplicate_review_attached",
                    "review_id": canonical_review_id,
                    "canonical_review_id": canonical_review_id,
                    "duplicate": True,
                    "duplicate_type": "normalized_text_product_rating",
                }

            connection.execute(
                """
                INSERT INTO products (product_id, product_name)
                VALUES (?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_name = COALESCE(excluded.product_name, products.product_name),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (review.product_id, review.product_name),
            )

            connection.execute(
                """
                INSERT INTO reviews_raw (
                    review_id, product_id, product_name, source, marketplace,
                    marketplace_product_id, marketplace_review_id, source_review_key,
                    source_url, reviewer_hash, review_title, rating, review_date,
                    raw_text, exact_text_hash, normalized_text_hash,
                    product_review_fingerprint, duplicate_status, canonical_review_id,
                    verified_purchase, helpful_votes, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'canonical', ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(review_id) DO UPDATE SET
                    product_id = excluded.product_id,
                    product_name = COALESCE(excluded.product_name, reviews_raw.product_name),
                    source = COALESCE(excluded.source, reviews_raw.source),
                    marketplace = COALESCE(excluded.marketplace, reviews_raw.marketplace),
                    marketplace_product_id = COALESCE(excluded.marketplace_product_id, reviews_raw.marketplace_product_id),
                    marketplace_review_id = COALESCE(excluded.marketplace_review_id, reviews_raw.marketplace_review_id),
                    source_review_key = COALESCE(excluded.source_review_key, reviews_raw.source_review_key),
                    source_url = COALESCE(excluded.source_url, reviews_raw.source_url),
                    reviewer_hash = COALESCE(excluded.reviewer_hash, reviews_raw.reviewer_hash),
                    review_title = COALESCE(excluded.review_title, reviews_raw.review_title),
                    rating = COALESCE(excluded.rating, reviews_raw.rating),
                    review_date = COALESCE(excluded.review_date, reviews_raw.review_date),
                    raw_text = excluded.raw_text,
                    exact_text_hash = excluded.exact_text_hash,
                    normalized_text_hash = excluded.normalized_text_hash,
                    product_review_fingerprint = excluded.product_review_fingerprint,
                    duplicate_status = 'canonical',
                    canonical_review_id = excluded.canonical_review_id,
                    verified_purchase = COALESCE(excluded.verified_purchase, reviews_raw.verified_purchase),
                    helpful_votes = COALESCE(excluded.helpful_votes, reviews_raw.helpful_votes),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    review_id,
                    review.product_id,
                    review.product_name,
                    review.source,
                    marketplace,
                    marketplace_product_id,
                    marketplace_review_id,
                    source_review_key,
                    source_url,
                    reviewer_hash,
                    review_title,
                    review.rating,
                    review.review_date,
                    raw_text,
                    exact_text_hash,
                    normalized_text_hash,
                    product_review_fingerprint,
                    review_id,
                    _optional_bool_to_int(review.verified_purchase),
                    review.helpful_votes,
                ),
            )

            self._attach_review_source(
                connection=connection,
                canonical_review_id=review_id,
                product_id=review.product_id,
                source_review_key=source_review_key,
                source=review.source,
                marketplace=marketplace,
                marketplace_product_id=marketplace_product_id,
                marketplace_review_id=marketplace_review_id,
                source_url=source_url,
                reviewer_hash=reviewer_hash,
                discovered_via=discovered_via,
            )

            connection.execute(
                "DELETE FROM aspect_sentiments WHERE review_id = ?",
                (review_id,),
            )

            connection.execute(
                """
                INSERT OR REPLACE INTO reviews_processed (
                    review_id, product_id, clean_text, language_profile_json,
                    signal_types_json, quality_score, contradiction_flag,
                    sarcasm_flag, processing_notes_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    analysis.product_id,
                    analysis.clean_text,
                    json.dumps(analysis.language_profile, ensure_ascii=False),
                    json.dumps([item.value for item in analysis.signal_types]),
                    analysis.quality_score,
                    int(analysis.contradiction_flag),
                    int(analysis.sarcasm_flag),
                    json.dumps(analysis.processing_notes, ensure_ascii=False),
                ),
            )

            for aspect in analysis.aspect_sentiments:
                connection.execute(
                    """
                    INSERT INTO aspect_sentiments (
                        review_id, product_id, aspect, sub_aspect, sentiment,
                        intensity, confidence, evidence_span, provider
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        review_id,
                        analysis.product_id,
                        aspect.aspect,
                        aspect.sub_aspect,
                        aspect.sentiment.value,
                        aspect.intensity,
                        aspect.confidence,
                        aspect.evidence_span,
                        aspect.provider,
                    ),
                )

        return {
            "status": "saved",
            "review_id": review_id,
            "canonical_review_id": review_id,
            "duplicate": False,
            "duplicate_type": None,
        }

    def _attach_review_source(
        self,
        connection: sqlite3.Connection,
        canonical_review_id: str,
        product_id: str,
        source_review_key: str,
        source: str | None,
        marketplace: str | None,
        marketplace_product_id: str | None,
        marketplace_review_id: str | None,
        source_url: str | None,
        reviewer_hash: str | None,
        discovered_via: str | None,
    ) -> None:
        connection.execute(
            """
            INSERT INTO review_sources (
                canonical_review_id, product_id, source_review_key, source,
                marketplace, marketplace_product_id, marketplace_review_id,
                source_url, reviewer_hash, discovered_via
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_review_key) DO UPDATE SET
                canonical_review_id = excluded.canonical_review_id,
                product_id = excluded.product_id,
                source = COALESCE(excluded.source, review_sources.source),
                marketplace = COALESCE(excluded.marketplace, review_sources.marketplace),
                marketplace_product_id = COALESCE(excluded.marketplace_product_id, review_sources.marketplace_product_id),
                marketplace_review_id = COALESCE(excluded.marketplace_review_id, review_sources.marketplace_review_id),
                source_url = COALESCE(excluded.source_url, review_sources.source_url),
                reviewer_hash = COALESCE(excluded.reviewer_hash, review_sources.reviewer_hash),
                discovered_via = COALESCE(excluded.discovered_via, review_sources.discovered_via),
                last_seen_at = CURRENT_TIMESTAMP
            """,
            (
                canonical_review_id,
                product_id,
                source_review_key,
                source,
                marketplace,
                marketplace_product_id,
                marketplace_review_id,
                source_url,
                reviewer_hash,
                discovered_via,
            ),
        )

    def get_database_stats(self) -> dict:
        tables = [
            "products",
            "reviews_raw",
            "reviews_processed",
            "aspect_sentiments",
            "living_lexicon",
            "evaluation_runs",
            "competitor_mappings",
            "extraction_feedback",
            "review_sources",
            "review_duplicate_candidates",
            "acquisition_runs",
        ]

        with connect(self.database_path) as connection:
            stats: dict[str, int] = {}
            for table in tables:
                try:
                    stats[table] = int(
                        connection.execute(
                            f"SELECT COUNT(*) AS count FROM {table}"
                        ).fetchone()["count"]
                    )
                except sqlite3.OperationalError:
                    stats[table] = 0
            return stats

    def reset_review_data(self) -> dict:
        with connect(self.database_path) as connection:
            connection.execute("DELETE FROM aspect_sentiments")
            connection.execute("DELETE FROM reviews_processed")
            connection.execute("DELETE FROM reviews_raw")
            connection.execute("DELETE FROM competitor_mappings")
            connection.execute("DELETE FROM review_duplicate_candidates")
            connection.execute("DELETE FROM review_sources")
            connection.execute("DELETE FROM acquisition_runs")
            connection.execute("DELETE FROM extraction_feedback")
            connection.execute("DELETE FROM products")
        self.seed_lexicon()
        return self.get_database_stats()

    def upsert_product(
        self,
        product_id: str,
        product_name: str | None = None,
        brand: str | None = None,
        price_band: str | None = None,
        own_brand: bool | None = None,
        parent_company: str | None = None,
        marketplace: str | None = None,
        marketplace_product_id: str | None = None,
        marketplace_product_url: str | None = None,
        launch_period: str | None = None,
        comparison_group: str | None = None,
    ) -> None:
        if not product_id:
            raise ValueError("product_id is required")

        with connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO products (
                    product_id, product_name, brand, parent_company, price_band,
                    own_brand, marketplace, marketplace_product_id,
                    marketplace_product_url, launch_period, comparison_group
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    product_name = COALESCE(excluded.product_name, products.product_name),
                    brand = COALESCE(excluded.brand, products.brand),
                    parent_company = COALESCE(excluded.parent_company, products.parent_company),
                    price_band = COALESCE(excluded.price_band, products.price_band),
                    own_brand = COALESCE(excluded.own_brand, products.own_brand),
                    marketplace = COALESCE(excluded.marketplace, products.marketplace),
                    marketplace_product_id = COALESCE(excluded.marketplace_product_id, products.marketplace_product_id),
                    marketplace_product_url = COALESCE(excluded.marketplace_product_url, products.marketplace_product_url),
                    launch_period = COALESCE(excluded.launch_period, products.launch_period),
                    comparison_group = COALESCE(excluded.comparison_group, products.comparison_group),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    product_id,
                    product_name,
                    brand,
                    parent_company,
                    price_band,
                    int(own_brand) if own_brand is not None else None,
                    marketplace,
                    marketplace_product_id,
                    marketplace_product_url,
                    launch_period,
                    comparison_group,
                ),
            )

    def save_competitor_mapping(
        self,
        product_id: str,
        competitor_product_id: str,
        comparison_group: str = "direct_competitor",
        notes: str | None = None,
    ) -> None:
        if product_id == competitor_product_id:
            raise ValueError("A product cannot be mapped as its own competitor")

        with connect(self.database_path) as connection:
            connection.execute(
                """
                INSERT INTO competitor_mappings (
                    product_id, competitor_product_id, comparison_group, notes
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(product_id, competitor_product_id) DO UPDATE SET
                    comparison_group = excluded.comparison_group,
                    notes = COALESCE(excluded.notes, competitor_mappings.notes)
                """,
                (product_id, competitor_product_id, comparison_group, notes),
            )

    def list_competitors(self, product_id: str) -> list[dict]:
        with connect(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    c.competitor_product_id AS product_id,
                    p.product_name,
                    p.brand,
                    p.price_band,
                    p.own_brand,
                    p.marketplace,
                    p.marketplace_product_url,
                    c.comparison_group,
                    c.notes
                FROM competitor_mappings c
                LEFT JOIN products p ON p.product_id = c.competitor_product_id
                WHERE c.product_id = ?
                ORDER BY c.comparison_group ASC, p.product_name ASC, c.competitor_product_id ASC
                """,
                (product_id,),
            ).fetchall()

        return [dict(row) for row in rows]

    def get_competitor_benchmark(
        self,
        product_id: str,
        competitor_product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        own_summary = self.get_product_summary(product_id, start_date, end_date)
        competitor_summary = self.get_product_summary(
            competitor_product_id,
            start_date,
            end_date,
        )

        own_aspects = {
            item["aspect"]: item
            for item in self.get_aspect_summary(product_id, start_date, end_date)
        }
        competitor_aspects = {
            item["aspect"]: item
            for item in self.get_aspect_summary(
                competitor_product_id,
                start_date,
                end_date,
            )
        }

        all_aspects = sorted(set(own_aspects) | set(competitor_aspects))
        benchmark_aspects = []

        for aspect in all_aspects:
            own = own_aspects.get(aspect) or _empty_aspect(aspect)
            competitor = competitor_aspects.get(aspect) or _empty_aspect(aspect)

            own_mentions = int(own["mentions"])
            competitor_mentions = int(competitor["mentions"])
            gap = round(float(own["aspect_score"]) - float(competitor["aspect_score"]), 1)

            confidence_label = _benchmark_confidence_label(
                own_mentions=own_mentions,
                competitor_mentions=competitor_mentions,
                own_confidence=own.get("avg_confidence"),
                competitor_confidence=competitor.get("avg_confidence"),
            )

            benchmark_aspects.append(
                {
                    "aspect": aspect,
                    "own_score": float(own["aspect_score"]),
                    "competitor_score": float(competitor["aspect_score"]),
                    "gap": gap,
                    "own_mentions": own_mentions,
                    "competitor_mentions": competitor_mentions,
                    "own_confidence": own.get("avg_confidence"),
                    "competitor_confidence": competitor.get("avg_confidence"),
                    "confidence_label": confidence_label,
                    "interpretation": _gap_interpretation(
                        aspect=aspect,
                        gap=gap,
                        confidence_label=confidence_label,
                        own_mentions=own_mentions,
                        competitor_mentions=competitor_mentions,
                    ),
                }
            )

        direct_comparisons = [
            item
            for item in benchmark_aspects
            if item["own_mentions"] > 0 and item["competitor_mentions"] > 0
        ]

        strengths = sorted(
            [item for item in direct_comparisons if item["gap"] > 0],
            key=lambda item: (
                item["gap"],
                item["own_mentions"] + item["competitor_mentions"],
            ),
            reverse=True,
        )[:3]

        weaknesses = sorted(
            [item for item in direct_comparisons if item["gap"] < 0],
            key=lambda item: (
                item["gap"],
                -(item["own_mentions"] + item["competitor_mentions"]),
            ),
        )[:3]

        return {
            "product_id": product_id,
            "competitor_product_id": competitor_product_id,
            "period": {"start_date": start_date, "end_date": end_date},
            "own_review_count": own_summary["review_count"],
            "competitor_review_count": competitor_summary["review_count"],
            "benchmark_aspects": benchmark_aspects,
            "top_strengths": strengths,
            "top_weaknesses": weaknesses,
        }

    def save_extraction_feedback(
        self,
        review_id: str,
        product_id: str,
        aspect: str,
        predicted_sentiment: str,
        provider: str | None,
        is_correct: bool,
        corrected_aspect: str | None = None,
        corrected_sentiment: str | None = None,
        note: str | None = None,
    ) -> dict:
        """Create or update feedback for the same extracted evidence item.

        This now uses a DB-level unique identity through provider_key, so repeated
        clicks cannot inflate provider-quality metrics even under concurrent usage.
        """

        provider_key = provider or ""

        with connect(self.database_path) as connection:
            cursor = connection.execute(
                """
                INSERT INTO extraction_feedback (
                    review_id,
                    product_id,
                    aspect,
                    predicted_sentiment,
                    provider,
                    provider_key,
                    is_correct,
                    corrected_aspect,
                    corrected_sentiment,
                    note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    review_id,
                    product_id,
                    aspect,
                    predicted_sentiment,
                    provider_key
                ) DO UPDATE SET
                    provider = excluded.provider,
                    is_correct = excluded.is_correct,
                    corrected_aspect = excluded.corrected_aspect,
                    corrected_sentiment = excluded.corrected_sentiment,
                    note = excluded.note,
                    created_at = CURRENT_TIMESTAMP
                """,
                (
                    review_id,
                    product_id,
                    aspect,
                    predicted_sentiment,
                    provider,
                    provider_key,
                    int(is_correct),
                    corrected_aspect,
                    corrected_sentiment,
                    note,
                ),
            )

            row = connection.execute(
                """
                SELECT
                    id, review_id, product_id, aspect, predicted_sentiment,
                    provider, is_correct, corrected_aspect, corrected_sentiment,
                    note, created_at
                FROM extraction_feedback
                WHERE id = COALESCE(
                    ?,
                    (
                        SELECT id
                        FROM extraction_feedback
                        WHERE review_id = ?
                          AND product_id = ?
                          AND aspect = ?
                          AND predicted_sentiment = ?
                          AND provider_key = ?
                        LIMIT 1
                    )
                )
                """,
                (
                    cursor.lastrowid,
                    review_id,
                    product_id,
                    aspect,
                    predicted_sentiment,
                    provider_key,
                ),
            ).fetchone()

            if row is None:
                row = connection.execute(
                    """
                    SELECT
                        id, review_id, product_id, aspect, predicted_sentiment,
                        provider, is_correct, corrected_aspect, corrected_sentiment,
                        note, created_at
                    FROM extraction_feedback
                    WHERE review_id = ?
                      AND product_id = ?
                      AND aspect = ?
                      AND predicted_sentiment = ?
                      AND provider_key = ?
                    LIMIT 1
                    """,
                    (
                        review_id,
                        product_id,
                        aspect,
                        predicted_sentiment,
                        provider_key,
                    ),
                ).fetchone()

        return _feedback_row_to_dict(row)

    def list_extraction_feedback(
        self,
        product_id: str | None = None,
        provider: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Return latest unique feedback rows.

        One evidence item is identified by:
        review_id + product_id + aspect + predicted_sentiment + provider

        This keeps the Recent Feedback UI clean and prevents old repeated clicks
        from appearing as separate active judgments.
        """

        outer_clauses = []
        params: list[object] = []

        if product_id:
            outer_clauses.append("ef.product_id = ?")
            params.append(product_id)

        if provider:
            outer_clauses.append("ef.provider = ?")
            params.append(provider)

        where_sql = f"WHERE {' AND '.join(outer_clauses)}" if outer_clauses else ""
        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                WITH latest_feedback AS (
                    SELECT
                        review_id,
                        product_id,
                        aspect,
                        predicted_sentiment,
                        COALESCE(provider, '') AS provider_key,
                        MAX(id) AS latest_id
                    FROM extraction_feedback
                    GROUP BY
                        review_id,
                        product_id,
                        aspect,
                        predicted_sentiment,
                        COALESCE(provider, '')
                )
                SELECT
                    ef.id,
                    ef.review_id,
                    ef.product_id,
                    ef.aspect,
                    ef.predicted_sentiment,
                    ef.provider,
                    ef.is_correct,
                    ef.corrected_aspect,
                    ef.corrected_sentiment,
                    ef.note,
                    ef.created_at
                FROM extraction_feedback ef
                JOIN latest_feedback latest
                  ON ef.id = latest.latest_id
                {where_sql}
                ORDER BY ef.created_at DESC, ef.id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [_feedback_row_to_dict(row) for row in rows]

    def get_provider_quality(self) -> list[dict]:
        """Provider quality from the latest feedback per unique evidence item."""

        with connect(self.database_path) as connection:
            rows = connection.execute(
                """
                WITH latest_feedback AS (
                    SELECT ef.*
                    FROM extraction_feedback ef
                    JOIN (
                        SELECT
                            review_id,
                            product_id,
                            aspect,
                            predicted_sentiment,
                            COALESCE(provider, '') AS provider_key,
                            MAX(id) AS latest_id
                        FROM extraction_feedback
                        GROUP BY
                            review_id,
                            product_id,
                            aspect,
                            predicted_sentiment,
                            COALESCE(provider, '')
                    ) latest
                    ON ef.id = latest.latest_id
                )
                SELECT
                    COALESCE(provider, 'unknown') AS provider,
                    COUNT(*) AS total_feedback,
                    SUM(CASE WHEN is_correct = 1 THEN 1 ELSE 0 END) AS correct_count,
                    SUM(CASE WHEN is_correct = 0 THEN 1 ELSE 0 END) AS incorrect_count
                FROM latest_feedback
                GROUP BY COALESCE(provider, 'unknown')
                ORDER BY total_feedback DESC, provider ASC
                """
            ).fetchall()

        results = []
        for row in rows:
            total = int(row["total_feedback"] or 0)
            correct = int(row["correct_count"] or 0)
            incorrect = int(row["incorrect_count"] or 0)
            accuracy = round(correct / total, 3) if total else 0.0

            results.append(
                {
                    "provider": row["provider"],
                    "total_feedback": total,
                    "correct_count": correct,
                    "incorrect_count": incorrect,
                    "accuracy": accuracy,
                }
            )

        return results

    def list_lexicon_entries(
        self,
        search: str | None = None,
        aspect: str | None = None,
        sentiment_prior: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []

        if search:
            clauses.append(
                "(term LIKE ? OR normalized_term LIKE ? OR examples_json LIKE ?)"
            )
            like_value = f"%{search.lower()}%"
            params.extend([like_value, like_value, like_value])

        if aspect:
            clauses.append("aspect = ?")
            params.append(aspect)

        if sentiment_prior:
            clauses.append("sentiment_prior = ?")
            params.append(sentiment_prior)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    id,
                    term,
                    normalized_term,
                    term_type,
                    language_type,
                    aspect,
                    sentiment_prior,
                    intensity,
                    confidence,
                    source,
                    approved_by_human,
                    examples_json,
                    created_at
                FROM living_lexicon
                {where_sql}
                ORDER BY
                    approved_by_human DESC,
                    confidence DESC,
                    aspect ASC,
                    term ASC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [_lexicon_row_to_dict(row) for row in rows]

    def list_products(self) -> list[dict]:
        with connect(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    p.product_id,
                    COALESCE(p.product_name, p.product_id) AS product_name,
                    p.brand,
                    p.parent_company,
                    p.price_band,
                    p.own_brand,
                    p.marketplace,
                    p.marketplace_product_id,
                    p.marketplace_product_url,
                    p.launch_period,
                    p.comparison_group,
                    COUNT(r.review_id) AS review_count,
                    MIN(r.review_date) AS first_review_date,
                    MAX(r.review_date) AS latest_review_date
                FROM products p
                LEFT JOIN reviews_raw r
                  ON r.product_id = p.product_id
                 AND COALESCE(r.duplicate_status, 'canonical') = 'canonical'
                GROUP BY p.product_id
                ORDER BY latest_review_date DESC, product_name ASC
                """
            ).fetchall()

        return [
            {
                **dict(row),
                "own_brand": bool(row["own_brand"]),
            }
            for row in rows
        ]

    def list_review_sources(
        self,
        product_id: str | None = None,
        review_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []

        if product_id:
            clauses.append("product_id = ?")
            params.append(product_id)

        if review_id:
            clauses.append("canonical_review_id = ?")
            params.append(review_id)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    id, canonical_review_id, product_id, source_review_key,
                    source, marketplace, marketplace_product_id,
                    marketplace_review_id, source_url, reviewer_hash,
                    first_seen_at, last_seen_at
                FROM review_sources
                {where_sql}
                ORDER BY last_seen_at DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [dict(row) for row in rows]

    def get_review_duplicate_summary(self, product_id: str | None = None) -> dict:
        clauses = []
        params: list[object] = []
        if product_id:
            clauses.append("product_id = ?")
            params.append(product_id)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT duplicate_type, COUNT(*) AS count
                FROM review_duplicate_candidates
                {where_sql}
                GROUP BY duplicate_type
                ORDER BY count DESC
                """,
                params,
            ).fetchall()

            source_count = connection.execute(
                f"SELECT COUNT(*) AS count FROM review_sources {where_sql}",
                params,
            ).fetchone()["count"]

        return {
            "product_id": product_id,
            "source_count": int(source_count or 0),
            "duplicate_candidates": [dict(row) for row in rows],
        }

    def get_product_summary(
        self,
        product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        where_sql, params = _period_filter("r", product_id, start_date, end_date)

        with connect(self.database_path) as connection:
            base = connection.execute(
                f"""
                SELECT
                    COUNT(r.review_id) AS review_count,
                    AVG(r.rating) AS average_rating,
                    AVG(p.quality_score) AS average_quality_score,
                    SUM(CASE WHEN p.contradiction_flag = 1 THEN 1 ELSE 0 END) AS contradiction_count,
                    SUM(CASE WHEN p.sarcasm_flag = 1 THEN 1 ELSE 0 END) AS sarcasm_count
                FROM reviews_raw r
                JOIN reviews_processed p ON p.review_id = r.review_id
                WHERE {where_sql}
                """,
                params,
            ).fetchone()

            sentiments = connection.execute(
                f"""
                SELECT a.sentiment, COUNT(*) AS count, AVG(a.intensity) AS avg_intensity
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE {where_sql}
                GROUP BY a.sentiment
                """,
                params,
            ).fetchall()

            top_aspects = connection.execute(
                f"""
                SELECT a.aspect, COUNT(*) AS mentions
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE {where_sql}
                GROUP BY a.aspect
                ORDER BY mentions DESC, a.aspect ASC
                LIMIT 5
                """,
                params,
            ).fetchall()

        sentiment_counts = {row["sentiment"]: row["count"] for row in sentiments}
        net_score = _net_sentiment_score(sentiments)

        return {
            "product_id": product_id,
            "period": {"start_date": start_date, "end_date": end_date},
            "review_count": int(base["review_count"] or 0),
            "average_rating": _round_optional(base["average_rating"]),
            "average_quality_score": _round_optional(
                base["average_quality_score"],
                3,
            ),
            "net_sentiment_score": net_score,
            "sentiment_counts": sentiment_counts,
            "contradiction_count": int(base["contradiction_count"] or 0),
            "sarcasm_count": int(base["sarcasm_count"] or 0),
            "top_aspects": [dict(row) for row in top_aspects],
        }

    def get_aspect_summary(
        self,
        product_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict]:
        where_sql, params = _period_filter("r", product_id, start_date, end_date)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    a.aspect,
                    COUNT(*) AS mentions,
                    SUM(CASE WHEN a.sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
                    SUM(CASE WHEN a.sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count,
                    SUM(CASE WHEN a.sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
                    AVG(a.confidence) AS avg_confidence,
                    AVG(CASE
                        WHEN a.sentiment = 'positive' THEN a.intensity
                        WHEN a.sentiment = 'negative' THEN -a.intensity
                        ELSE 0
                    END) AS signed_intensity
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE {where_sql}
                GROUP BY a.aspect
                ORDER BY mentions DESC, a.aspect ASC
                """,
                params,
            ).fetchall()

        return [
            {
                "aspect": row["aspect"],
                "mentions": int(row["mentions"]),
                "positive_count": int(row["positive_count"] or 0),
                "negative_count": int(row["negative_count"] or 0),
                "neutral_count": int(row["neutral_count"] or 0),
                "avg_confidence": _round_optional(row["avg_confidence"], 3),
                "aspect_score": round(float(row["signed_intensity"] or 0) * 100, 1),
            }
            for row in rows
        ]

    def list_evidence(
        self,
        product_id: str,
        aspect: str | None = None,
        sentiment: Sentiment | None = None,
        limit: int = 10,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict]:
        where_sql, params = _period_filter("r", product_id, start_date, end_date)

        extra = []
        if aspect:
            extra.append("a.aspect = ?")
            params.append(aspect)

        if sentiment:
            extra.append("a.sentiment = ?")
            params.append(sentiment.value)

        full_where = where_sql + (" AND " + " AND ".join(extra) if extra else "")
        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    r.review_id, r.product_id, r.product_name, r.source, r.rating,
                    r.review_date, r.raw_text, p.clean_text, p.quality_score,
                    a.aspect, a.sentiment, a.intensity, a.confidence,
                    a.evidence_span, a.provider
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                JOIN reviews_processed p ON p.review_id = r.review_id
                WHERE {full_where}
                ORDER BY p.quality_score DESC, a.confidence DESC, r.review_date DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        result = [dict(row) for row in rows]
        for item in result:
            item["language_type"] = _detect_language_type(item.get("raw_text", ""))
        return result

    def get_sub_aspects(
        self,
        product_id: str,
        aspect: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, float]:
        """Return signed-intensity sub-aspect scores for a given aspect.

        Currently only camera sub-aspects are classified (front/night/video/overall).
        Returns an empty dict when no evidence rows exist.
        """
        where_sql, params = _period_filter("r", product_id, start_date, end_date)
        params.append(aspect)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT a.evidence_span, a.sentiment, a.intensity
                FROM aspect_sentiments a
                JOIN reviews_raw r ON r.review_id = a.review_id
                WHERE {where_sql} AND a.aspect = ?
                """,
                params,
            ).fetchall()

        buckets: dict[str, list[float]] = {
            "camera_front": [],
            "camera_night": [],
            "camera_video": [],
            "camera_overall": [],
        }

        for row in rows:
            span = row["evidence_span"] or ""
            intensity = float(row["intensity"] or 0)
            if row["sentiment"] == "positive":
                signed = intensity
            elif row["sentiment"] == "negative":
                signed = -intensity
            else:
                signed = 0.0

            if _CAMERA_FRONT.search(span):
                buckets["camera_front"].append(signed)
            elif _CAMERA_NIGHT.search(span):
                buckets["camera_night"].append(signed)
            elif _CAMERA_VIDEO.search(span):
                buckets["camera_video"].append(signed)
            else:
                buckets["camera_overall"].append(signed)

        return {
            sub: round(sum(scores) / len(scores) * 100, 1)
            for sub, scores in buckets.items()
            if scores
        }


def _normalize_review_text(value: str) -> str:
    text = value.lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-z0-9\u0900-\u097f ]+", "", text)
    return text.strip()


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


_HINGLISH_MARKERS = re.compile(
    r"\b(mast|bakwas|ekdum|nahi|nahin|hai|bahut|bhai)\b",
    re.IGNORECASE,
)

_CAMERA_FRONT = re.compile(r"front[\s\-]?cam|selfie", re.IGNORECASE)
_CAMERA_NIGHT = re.compile(r"night|low[\s\-]?light|dark(?:ness)?", re.IGNORECASE)
_CAMERA_VIDEO = re.compile(r"video|reel|recording", re.IGNORECASE)


def _detect_language_type(text: str) -> str:
    """Return 'hinglish', 'mixed', or 'english' based on Hinglish marker density."""
    if not text:
        return "english"
    count = len(_HINGLISH_MARKERS.findall(text))
    if count >= 3:
        return "hinglish"
    if count >= 1:
        return "mixed"
    return "english"


def _first_present(*values):
    for value in values:
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return None


def _build_source_review_key(
    source: str | None,
    marketplace: str | None,
    product_id: str,
    marketplace_product_id: str | None,
    marketplace_review_id: str | None,
    reviewer_hash: str | None,
    review_date: str | None,
    normalized_text_hash: str,
) -> str:
    if marketplace_review_id:
        seed = "|".join(
            [
                marketplace or source or "unknown_marketplace",
                marketplace_product_id or product_id,
                marketplace_review_id,
            ]
        )
    else:
        seed = "|".join(
            [
                source or marketplace or "unknown_source",
                marketplace_product_id or product_id,
                reviewer_hash or "unknown_reviewer",
                review_date or "unknown_date",
                normalized_text_hash,
            ]
        )
    return _hash_text(seed)



def _lexicon_row_to_dict(row) -> dict:
    try:
        examples = json.loads(row["examples_json"] or "[]")
    except json.JSONDecodeError:
        examples = []

    return {
        "id": int(row["id"]),
        "term": row["term"],
        "normalized_term": row["normalized_term"],
        "term_type": row["term_type"],
        "language_type": row["language_type"],
        "aspect": row["aspect"],
        "sentiment_prior": row["sentiment_prior"],
        "intensity": row["intensity"],
        "confidence": row["confidence"],
        "source": row["source"],
        "approved_by_human": bool(row["approved_by_human"]),
        "examples": examples,
        "created_at": row["created_at"],
    }


def _feedback_row_to_dict(row) -> dict:
    return {
        "id": int(row["id"]),
        "review_id": row["review_id"],
        "product_id": row["product_id"],
        "aspect": row["aspect"],
        "predicted_sentiment": row["predicted_sentiment"],
        "provider": row["provider"],
        "is_correct": bool(row["is_correct"]),
        "corrected_aspect": row["corrected_aspect"],
        "corrected_sentiment": row["corrected_sentiment"],
        "note": row["note"],
        "created_at": row["created_at"],
    }


def _optional_bool_to_int(value: bool | None) -> int | None:
    if value is None:
        return None
    return int(value)


def _period_filter(
    table_alias: str,
    product_id: str,
    start_date: str | None,
    end_date: str | None,
) -> tuple[str, list]:
    clauses = [f"{table_alias}.product_id = ?"]
    params: list = [product_id]

    if start_date:
        clauses.append(f"{table_alias}.review_date >= ?")
        params.append(start_date)

    if end_date:
        clauses.append(f"{table_alias}.review_date <= ?")
        params.append(end_date)

    return " AND ".join(clauses), params


def _net_sentiment_score(rows: Iterable[sqlite3.Row]) -> float:
    positive = 0.0
    negative = 0.0
    total = 0.0

    for row in rows:
        count = float(row["count"] or 0)
        intensity = float(row["avg_intensity"] or 0.5)
        total += count

        if row["sentiment"] == "positive":
            positive += count * intensity
        elif row["sentiment"] == "negative":
            negative += count * intensity

    if total == 0:
        return 0.0

    return round(((positive - negative) / total) * 100, 1)


def _round_optional(value: object, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _empty_aspect(aspect: str) -> dict:
    return {
        "aspect": aspect,
        "mentions": 0,
        "positive_count": 0,
        "negative_count": 0,
        "neutral_count": 0,
        "avg_confidence": None,
        "aspect_score": 0.0,
    }


def _benchmark_confidence_label(
    own_mentions: int,
    competitor_mentions: int,
    own_confidence: float | None,
    competitor_confidence: float | None,
) -> str:
    if own_mentions == 0 or competitor_mentions == 0:
        return "insufficient_comparison"

    total_mentions = own_mentions + competitor_mentions
    avg_conf = _safe_average([own_confidence, competitor_confidence])

    if total_mentions >= 50 and avg_conf >= 0.82:
        return "high"

    if total_mentions >= 10 and avg_conf >= 0.7:
        return "medium"

    return "low"


def _safe_average(values: list[float | None]) -> float:
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return 0.0
    return sum(valid) / len(valid)


def _gap_interpretation(
    aspect: str,
    gap: float,
    confidence_label: str,
    own_mentions: int = 0,
    competitor_mentions: int = 0,
) -> str:
    if own_mentions == 0 and competitor_mentions == 0:
        return f"No comparable evidence is available for {aspect} on either product."

    if own_mentions == 0:
        return (
            f"The competitor has {aspect} evidence, but the selected product has no comparable "
            f"{aspect} evidence yet. Treat this as an evidence gap, not a confirmed weakness."
        )

    if competitor_mentions == 0:
        return (
            f"The selected product has {aspect} evidence, but the competitor has no comparable "
            f"{aspect} evidence yet. Treat this as an evidence gap, not a confirmed strength."
        )

    if abs(gap) < 5:
        base = f"Near parity on {aspect}."
    elif gap > 0:
        base = f"The selected product is ahead on {aspect} by {gap} points."
    else:
        base = f"The competitor is ahead on {aspect} by {abs(gap)} points."

    if confidence_label == "low":
        return base + " Treat as directional because evidence volume/confidence is still low."

    return base