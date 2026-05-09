import sqlite3

from app.db.database import init_db
from app.db.repository import ReviewRepository


def test_living_lexicon_has_real_id_after_init(tmp_path):
    db_path = tmp_path / "test.db"
    repo = ReviewRepository(db_path)

    entries = repo.list_lexicon_entries(search="mast", limit=10)

    assert entries
    assert isinstance(entries[0]["id"], int)
    assert entries[0]["id"] > 0


def test_existing_old_lexicon_schema_is_migrated(tmp_path):
    db_path = tmp_path / "old_schema.db"

    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE living_lexicon (
            term TEXT PRIMARY KEY,
            normalized_term TEXT NOT NULL,
            term_type TEXT NOT NULL,
            language_type TEXT NOT NULL,
            aspect TEXT,
            sentiment_prior TEXT,
            intensity REAL NOT NULL,
            confidence REAL NOT NULL,
            source TEXT NOT NULL,
            approved_by_human INTEGER NOT NULL,
            examples_json TEXT NOT NULL DEFAULT '[]',
            last_seen_date TEXT,
            frequency INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        INSERT INTO living_lexicon (
            term, normalized_term, term_type, language_type,
            aspect, sentiment_prior, intensity, confidence,
            source, approved_by_human, examples_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "mast",
            "excellent",
            "sentiment",
            "hi_en_mixed",
            None,
            "positive",
            0.86,
            0.9,
            "seed",
            1,
            "[]",
        ),
    )
    connection.commit()
    connection.close()

    init_db(db_path)

    connection = sqlite3.connect(db_path)
    columns = [row[1] for row in connection.execute("PRAGMA table_info(living_lexicon)")]
    row = connection.execute(
        "SELECT id, term FROM living_lexicon WHERE term = 'mast'"
    ).fetchone()
    connection.close()

    assert "id" in columns
    assert row[0] > 0
    assert row[1] == "mast"


def test_feedback_upsert_prevents_duplicate_rows(tmp_path):
    repo = ReviewRepository(tmp_path / "test.db")

    first = repo.save_extraction_feedback(
        review_id="r1",
        product_id="phone_a",
        aspect="battery",
        predicted_sentiment="negative",
        provider="gemini:test",
        is_correct=True,
    )

    second = repo.save_extraction_feedback(
        review_id="r1",
        product_id="phone_a",
        aspect="battery",
        predicted_sentiment="negative",
        provider="gemini:test",
        is_correct=False,
    )

    feedback = repo.list_extraction_feedback(product_id="phone_a")
    quality = repo.get_provider_quality()

    assert first["id"] == second["id"]
    assert len(feedback) == 1
    assert feedback[0]["is_correct"] is False
    assert quality[0]["total_feedback"] == 1
    assert quality[0]["correct_count"] == 0
    assert quality[0]["incorrect_count"] == 1