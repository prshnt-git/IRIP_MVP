
from __future__ import annotations

from collections import Counter

from app.db.database import connect
from app.db.repository import ReviewRepository
import json
import re

class ActiveEvaluationService:
    def __init__(self, repository: ReviewRepository) -> None:
        self.repository = repository
        self.database_path = repository.database_path

    def build_queue(
        self,
        product_id: str | None = None,
        limit: int = 100,
    ) -> dict:
        candidates = self._find_candidates(product_id=product_id, limit=limit)
        inserted_count = 0
        reason_counter: Counter[str] = Counter()

        with connect(self.database_path) as connection:
            for item in candidates:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO active_evaluation_queue (
                        review_id,
                        product_id,
                        aspect,
                        predicted_sentiment,
                        provider,
                        reason,
                        priority_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["review_id"],
                        item["product_id"],
                        item.get("aspect"),
                        item.get("predicted_sentiment"),
                        item.get("provider"),
                        item["reason"],
                        item["priority_score"],
                    ),
                )

                if cursor.rowcount:
                    inserted_count += 1
                    reason_counter[item["reason"]] += 1

            open_count = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM active_evaluation_queue
                WHERE status = 'open'
                """
            ).fetchone()["count"]

        return {
            "inserted_count": inserted_count,
            "open_count": int(open_count or 0),
            "reasons": dict(reason_counter),
        }

    def list_queue(
        self,
        status: str = "open",
        product_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        clauses = ["status = ?"]
        params: list[object] = [status]

        if product_id:
            clauses.append("product_id = ?")
            params.append(product_id)

        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    id,
                    review_id,
                    product_id,
                    aspect,
                    predicted_sentiment,
                    provider,
                    reason,
                    priority_score,
                    status,
                    created_at,
                    updated_at
                FROM active_evaluation_queue
                WHERE {' AND '.join(clauses)}
                ORDER BY priority_score DESC, created_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [dict(row) for row in rows]

    def update_status(self, item_id: int, status: str) -> dict | None:
        with connect(self.database_path) as connection:
            connection.execute(
                """
                UPDATE active_evaluation_queue
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (status, item_id),
            )

            row = connection.execute(
                """
                SELECT
                    id,
                    review_id,
                    product_id,
                    aspect,
                    predicted_sentiment,
                    provider,
                    reason,
                    priority_score,
                    status,
                    created_at,
                    updated_at
                FROM active_evaluation_queue
                WHERE id = ?
                """,
                (item_id,),
            ).fetchone()

        return dict(row) if row else None

    def _find_candidates(
        self,
        product_id: str | None,
        limit: int,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []

        if product_id:
            clauses.append("a.product_id = ?")
            params.append(product_id)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    a.review_id,
                    a.product_id,
                    a.aspect,
                    a.sentiment AS predicted_sentiment,
                    a.confidence,
                    a.intensity,
                    a.provider,
                    rp.quality_score,
                    rr.rating,
                    rr.raw_text
                FROM aspect_sentiments a
                JOIN reviews_processed rp ON rp.review_id = a.review_id
                JOIN reviews_raw rr ON rr.review_id = a.review_id
                {where_sql}
                ORDER BY rp.created_at DESC
                LIMIT ?
                """,
                [*params, limit],
            ).fetchall()

        candidates: list[dict] = []

        for row in rows:
            confidence = float(row["confidence"] or 0.0)
            quality_score = float(row["quality_score"] or 0.0)
            intensity = float(row["intensity"] or 0.0)
            sentiment = row["predicted_sentiment"]
            rating = row["rating"]

            if confidence < 0.72:
                candidates.append(
                    self._candidate(row, "low_confidence_extraction", 80 + (1 - confidence) * 20)
                )

            if quality_score < 0.75:
                candidates.append(
                    self._candidate(row, "low_quality_review_signal", 70 + (1 - quality_score) * 20)
                )

            if sentiment == "negative" and intensity >= 0.8:
                candidates.append(
                    self._candidate(row, "high_intensity_complaint", 75 + intensity * 10)
                )

            if rating is not None:
                rating_float = float(rating)
                if rating_float >= 4 and sentiment == "negative":
                    candidates.append(
                        self._candidate(row, "rating_text_contradiction_candidate", 85)
                    )
                if rating_float <= 2 and sentiment == "positive":
                    candidates.append(
                        self._candidate(row, "rating_text_contradiction_candidate", 85)
                    )

            if row["aspect"] in {"heating", "connectivity", "audio", "design", "after_sales"}:
                candidates.append(
                    self._candidate(row, "strategic_aspect_needs_validation", 72)
                )

        return candidates

    def _candidate(self, row, reason: str, priority_score: float) -> dict:
        return {
            "review_id": row["review_id"],
            "product_id": row["product_id"],
            "aspect": row["aspect"],
            "predicted_sentiment": row["predicted_sentiment"],
            "provider": row["provider"],
            "reason": reason,
            "priority_score": round(float(priority_score), 3),
        }
    
    def promote_to_golden_case(
        self,
        item_id: int,
        expected_aspect: str | None = None,
        expected_sentiment: str | None = None,
        expected: list[dict] | None = None,
        note: str | None = None,
    ) -> dict | None:
        """Promote a reviewed queue item into the DB-backed golden benchmark.

        Supports both:
        1. Backward-compatible single-label promotion.
        2. Multi-label promotion for real reviews with multiple aspects.

        This does not mutate Python source files. It stores human-approved
        golden cases in the database so the benchmark can grow from real data.
        """

        with connect(self.database_path) as connection:
            queue_row = connection.execute(
                """
                SELECT
                    q.id,
                    q.review_id,
                    q.product_id,
                    q.aspect,
                    q.predicted_sentiment,
                    q.provider,
                    q.reason,
                    r.product_name,
                    r.source,
                    r.rating,
                    r.review_date,
                    r.raw_text
                FROM active_evaluation_queue q
                JOIN reviews_raw r ON r.review_id = q.review_id
                WHERE q.id = ?
                """,
                (item_id,),
            ).fetchone()

            if queue_row is None:
                return None

            final_expected = self._resolve_expected_labels(
                queue_row=queue_row,
                expected_aspect=expected_aspect,
                expected_sentiment=expected_sentiment,
                expected=expected,
            )

            expected_json = json.dumps(final_expected, ensure_ascii=False)

            case_id = self._golden_case_id(
                review_id=queue_row["review_id"],
                expected=final_expected,
            )

            connection.execute(
                """
                INSERT INTO golden_review_cases (
                    case_id,
                    source_queue_item_id,
                    review_id,
                    product_id,
                    product_name,
                    source,
                    rating,
                    review_date,
                    raw_text,
                    expected_json,
                    note,
                    approved_by_human
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(case_id) DO UPDATE SET
                    expected_json = excluded.expected_json,
                    note = excluded.note,
                    approved_by_human = 1,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    case_id,
                    queue_row["id"],
                    queue_row["review_id"],
                    queue_row["product_id"],
                    queue_row["product_name"],
                    queue_row["source"],
                    queue_row["rating"],
                    queue_row["review_date"],
                    queue_row["raw_text"],
                    expected_json,
                    note,
                ),
            )

            connection.execute(
                """
                UPDATE active_evaluation_queue
                SET status = 'promoted', updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (item_id,),
            )

            row = connection.execute(
                """
                SELECT
                    id,
                    case_id,
                    source_queue_item_id,
                    review_id,
                    product_id,
                    product_name,
                    source,
                    rating,
                    review_date,
                    raw_text,
                    expected_json,
                    note,
                    approved_by_human,
                    created_at,
                    updated_at
                FROM golden_review_cases
                WHERE case_id = ?
                """,
                (case_id,),
            ).fetchone()

        return self._golden_case_row_to_dict(row) if row else None

    def list_golden_cases(
        self,
        product_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        clauses = []
        params: list[object] = []

        if product_id:
            clauses.append("product_id = ?")
            params.append(product_id)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)

        with connect(self.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    id,
                    case_id,
                    source_queue_item_id,
                    review_id,
                    product_id,
                    product_name,
                    source,
                    rating,
                    review_date,
                    raw_text,
                    expected_json,
                    note,
                    approved_by_human,
                    created_at,
                    updated_at
                FROM golden_review_cases
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                params,
            ).fetchall()

        return [self._golden_case_row_to_dict(row) for row in rows]

    def update_golden_case(
        self,
        case_id: str,
        expected: list[dict],
        note: str | None = None,
    ) -> dict | None:
        labels = self._dedupe_expected_labels(expected)

        if not labels:
            raise ValueError("expected must contain at least one valid aspect/sentiment label.")

        expected_json = json.dumps(labels, ensure_ascii=False)

        with connect(self.database_path) as connection:
            connection.execute(
                """
                UPDATE golden_review_cases
                SET
                    expected_json = ?,
                    note = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE case_id = ?
                """,
                (expected_json, note, case_id),
            )

            row = connection.execute(
                """
                SELECT
                    id,
                    case_id,
                    source_queue_item_id,
                    review_id,
                    product_id,
                    product_name,
                    source,
                    rating,
                    review_date,
                    raw_text,
                    expected_json,
                    note,
                    approved_by_human,
                    created_at,
                    updated_at
                FROM golden_review_cases
                WHERE case_id = ?
                """,
                (case_id,),
            ).fetchone()

        return self._golden_case_row_to_dict(row) if row else None

    def _golden_case_id(self, review_id: str, expected: list[dict]) -> str:
        label_part = "_".join(
            f"{item['aspect']}_{item['sentiment']}" for item in expected
        )
        raw = f"live_{review_id}_{label_part}".lower()
        return re.sub(r"[^a-z0-9_]+", "_", raw)
    
    def _resolve_expected_labels(
        self,
        queue_row,
        expected_aspect: str | None,
        expected_sentiment: str | None,
        expected: list[dict] | None,
    ) -> list[dict]:
        if expected:
            labels = [
                {
                    "aspect": str(item["aspect"]).strip(),
                    "sentiment": str(item["sentiment"]).strip(),
                }
                for item in expected
                if str(item.get("aspect", "")).strip()
                and str(item.get("sentiment", "")).strip()
            ]

            if not labels:
                raise ValueError("expected must contain at least one valid aspect/sentiment label.")

            return self._dedupe_expected_labels(labels)

        final_aspect = expected_aspect or queue_row["aspect"]
        final_sentiment = expected_sentiment or queue_row["predicted_sentiment"]

        if not final_aspect or not final_sentiment:
            raise ValueError(
                "expected_aspect and expected_sentiment are required when queue item has no prediction."
            )

        return self._dedupe_expected_labels(
            [
                {
                    "aspect": final_aspect,
                    "sentiment": final_sentiment,
                }
            ]
        )

    def _dedupe_expected_labels(self, labels: list[dict]) -> list[dict]:
        seen: set[tuple[str, str]] = set()
        cleaned: list[dict] = []

        for item in labels:
            aspect = str(item["aspect"]).strip().lower()
            sentiment = str(item["sentiment"]).strip().lower()
            key = (aspect, sentiment)

            if key in seen:
                continue

            seen.add(key)
            cleaned.append(
                {
                    "aspect": aspect,
                    "sentiment": sentiment,
                }
            )

        return cleaned

    def _golden_case_row_to_dict(self, row) -> dict:
        try:
            expected = json.loads(row["expected_json"] or "[]")
        except json.JSONDecodeError:
            expected = []

        return {
            "id": int(row["id"]),
            "case_id": row["case_id"],
            "source_queue_item_id": row["source_queue_item_id"],
            "review_id": row["review_id"],
            "product_id": row["product_id"],
            "product_name": row["product_name"],
            "source": row["source"],
            "rating": row["rating"],
            "review_date": row["review_date"],
            "raw_text": row["raw_text"],
            "expected": expected,
            "note": row["note"],
            "approved_by_human": bool(row["approved_by_human"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }