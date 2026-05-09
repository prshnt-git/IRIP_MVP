from __future__ import annotations

from pathlib import Path
import sqlite3

CURRENT_SCHEMA_VERSION = 6

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    brand TEXT,
    price_band TEXT,
    own_brand INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS reviews_raw (
    review_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_name TEXT,
    source TEXT,
    rating REAL,
    review_date TEXT,
    raw_text TEXT NOT NULL,
    verified_purchase INTEGER,
    helpful_votes INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS reviews_processed (
    review_id TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    clean_text TEXT NOT NULL,
    language_profile_json TEXT NOT NULL,
    signal_types_json TEXT NOT NULL,
    quality_score REAL NOT NULL,
    contradiction_flag INTEGER NOT NULL,
    sarcasm_flag INTEGER NOT NULL,
    processing_notes_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (review_id) REFERENCES reviews_raw(review_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS aspect_sentiments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    aspect TEXT NOT NULL,
    sub_aspect TEXT,
    sentiment TEXT NOT NULL,
    intensity REAL NOT NULL,
    confidence REAL NOT NULL,
    evidence_span TEXT NOT NULL,
    provider TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (review_id) REFERENCES reviews_raw(review_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE IF NOT EXISTS living_lexicon (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term TEXT NOT NULL UNIQUE,
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
);

CREATE TABLE IF NOT EXISTS competitor_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL,
    competitor_product_id TEXT NOT NULL,
    comparison_group TEXT DEFAULT 'direct_competitor',
    notes TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, competitor_product_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
    FOREIGN KEY (competitor_product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evaluation_runs (
    run_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    total_cases INTEGER NOT NULL,
    aspect_precision_proxy REAL NOT NULL,
    sentiment_accuracy_proxy REAL NOT NULL,
    failed_case_ids_json TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS extraction_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    aspect TEXT NOT NULL,
    predicted_sentiment TEXT NOT NULL,
    provider TEXT,
    provider_key TEXT NOT NULL DEFAULT '',
    is_correct INTEGER NOT NULL,
    corrected_aspect TEXT,
    corrected_sentiment TEXT,
    note TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS active_evaluation_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    review_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    aspect TEXT,
    predicted_sentiment TEXT,
    provider TEXT,
    reason TEXT NOT NULL,
    priority_score REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(review_id, product_id, aspect, predicted_sentiment, provider, reason)
);

CREATE TABLE IF NOT EXISTS golden_review_cases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL UNIQUE,
    source_queue_item_id INTEGER,
    review_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    product_name TEXT,
    source TEXT,
    rating REAL,
    review_date TEXT,
    raw_text TEXT NOT NULL,
    expected_json TEXT NOT NULL,
    note TEXT,
    approved_by_human INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trusted_news_sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    source_tier INTEGER NOT NULL,
    source_type TEXT NOT NULL,
    allowed_domains_json TEXT NOT NULL,
    default_tags_json TEXT NOT NULL DEFAULT '[]',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS news_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_tier INTEGER NOT NULL,
    title TEXT NOT NULL,
    canonical_url TEXT NOT NULL UNIQUE,
    published_at TEXT,
    summary TEXT,
    discovered_via TEXT NOT NULL,
    topic_tags_json TEXT NOT NULL DEFAULT '[]',
    company_tags_json TEXT NOT NULL DEFAULT '[]',
    technology_tags_json TEXT NOT NULL DEFAULT '[]',
    region_tags_json TEXT NOT NULL DEFAULT '[]',
    relevance_score REAL NOT NULL DEFAULT 0,
    priority_label TEXT NOT NULL DEFAULT 'low',
    why_it_matters TEXT,
    evidence_url TEXT NOT NULL,
    ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES trusted_news_sources(source_id),
);

CREATE INDEX IF NOT EXISTS idx_news_items_source_date
ON news_items(source_id, published_at);

CREATE INDEX IF NOT EXISTS idx_news_items_relevance
ON news_items(relevance_score DESC, ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_golden_review_cases_product
ON golden_review_cases(product_id, created_at);

CREATE INDEX IF NOT EXISTS idx_active_evaluation_queue_status
ON active_evaluation_queue(status, priority_score DESC);

CREATE INDEX IF NOT EXISTS idx_active_evaluation_queue_product
ON active_evaluation_queue(product_id, status);

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_extraction_feedback_review
ON extraction_feedback(review_id, product_id, aspect);

CREATE INDEX IF NOT EXISTS idx_extraction_feedback_review
ON extraction_feedback(review_id, product_id, aspect);

CREATE INDEX IF NOT EXISTS idx_reviews_raw_product_date ON reviews_raw(product_id, review_date);
CREATE INDEX IF NOT EXISTS idx_aspect_sentiments_product_aspect ON aspect_sentiments(product_id, aspect, sentiment);
CREATE INDEX IF NOT EXISTS idx_reviews_processed_quality ON reviews_processed(product_id, quality_score);
CREATE INDEX IF NOT EXISTS idx_competitor_mappings_product ON competitor_mappings(product_id, competitor_product_id);
CREATE INDEX IF NOT EXISTS idx_living_lexicon_aspect_sentiment ON living_lexicon(aspect, sentiment_prior);
"""


def connect(database_path: str | Path) -> sqlite3.Connection:
    path = Path(database_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db(database_path: str | Path) -> None:
    with connect(database_path) as connection:
        connection.executescript(SCHEMA_SQL)
        _run_migrations(connection)


def _run_migrations(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    applied_versions = {
        int(row["version"])
        for row in connection.execute("SELECT version FROM schema_version").fetchall()
    }

    migrations = [
        (1, _migration_001_living_lexicon_real_id),
        (2, _migration_002_feedback_provider_key_unique_identity),
        (3, _migration_003_active_evaluation_queue),
        (4, _migration_004_golden_review_cases),
        (5, _migration_005_news_intelligence),
        (6, _migration_006_news_relevance_refinement),
    ]

    for version, migration in migrations:
        if version not in applied_versions:
            migration(connection)
            connection.execute(
                "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
                (version,),
            )


def _migration_001_living_lexicon_real_id(connection: sqlite3.Connection) -> None:
    """Add a real numeric id to living_lexicon.

    Older MVP DBs used `term TEXT PRIMARY KEY` and had no id column. That was fine
    for read-only lookup, but not good enough for future edit/approval workflows.
    """

    if not _table_exists(connection, "living_lexicon"):
        return

    if _column_exists(connection, "living_lexicon", "id"):
        return

    connection.execute("ALTER TABLE living_lexicon RENAME TO living_lexicon_old")

    connection.execute(
        """
        CREATE TABLE living_lexicon (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT NOT NULL UNIQUE,
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
    INSERT OR IGNORE INTO living_lexicon (
        term, normalized_term, term_type, language_type, aspect,
        sentiment_prior, intensity, confidence, source, approved_by_human,
        examples_json, last_seen_date, frequency, created_at, updated_at
    )
    SELECT
        term, normalized_term, term_type, language_type, aspect,
        sentiment_prior, intensity, confidence, source, approved_by_human,
        COALESCE(examples_json, '[]'), last_seen_date, frequency, created_at, updated_at
    FROM living_lexicon_old
    """
    )

    connection.execute("DROP TABLE living_lexicon_old")


def _migration_002_feedback_provider_key_unique_identity(connection: sqlite3.Connection) -> None:
    """Make feedback duplicate-safe at the database layer.

    One active analyst judgment should exist for:
    review_id + product_id + aspect + predicted_sentiment + provider_key
    """

    if not _table_exists(connection, "extraction_feedback"):
        return

    if not _column_exists(connection, "extraction_feedback", "provider_key"):
        connection.execute(
            "ALTER TABLE extraction_feedback ADD COLUMN provider_key TEXT NOT NULL DEFAULT ''"
        )

    connection.execute(
        """
        UPDATE extraction_feedback
        SET provider_key = COALESCE(provider, '')
        WHERE provider_key IS NULL OR provider_key = ''
        """
    )

    # Collapse old repeated-click duplicates before creating unique index.
    connection.execute(
        """
        DELETE FROM extraction_feedback
        WHERE id NOT IN (
            SELECT MAX(id)
            FROM extraction_feedback
            GROUP BY review_id, product_id, aspect, predicted_sentiment, provider_key
        )
        """
    )

    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_extraction_feedback_identity
        ON extraction_feedback(
            review_id,
            product_id,
            aspect,
            predicted_sentiment,
            provider_key
        )
        """
    )


def _migration_003_active_evaluation_queue(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS active_evaluation_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            review_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            aspect TEXT,
            predicted_sentiment TEXT,
            provider TEXT,
            reason TEXT NOT NULL,
            priority_score REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(review_id, product_id, aspect, predicted_sentiment, provider, reason)
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_active_evaluation_queue_status
        ON active_evaluation_queue(status, priority_score DESC)
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_active_evaluation_queue_product
        ON active_evaluation_queue(product_id, status)
        """
    )

def _migration_004_golden_review_cases(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS golden_review_cases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            case_id TEXT NOT NULL UNIQUE,
            source_queue_item_id INTEGER,
            review_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            product_name TEXT,
            source TEXT,
            rating REAL,
            review_date TEXT,
            raw_text TEXT NOT NULL,
            expected_json TEXT NOT NULL,
            note TEXT,
            approved_by_human INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_golden_review_cases_product
        ON golden_review_cases(product_id, created_at)
        """
    )

def _migration_005_news_intelligence(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS trusted_news_sources (
            source_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            source_tier INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            allowed_domains_json TEXT NOT NULL,
            default_tags_json TEXT NOT NULL DEFAULT '[]',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_tier INTEGER NOT NULL,
            title TEXT NOT NULL,
            canonical_url TEXT NOT NULL UNIQUE,
            published_at TEXT,
            summary TEXT,
            discovered_via TEXT NOT NULL,
            topic_tags_json TEXT NOT NULL DEFAULT '[]',
            company_tags_json TEXT NOT NULL DEFAULT '[]',
            technology_tags_json TEXT NOT NULL DEFAULT '[]',
            region_tags_json TEXT NOT NULL DEFAULT '[]',
            relevance_score REAL NOT NULL DEFAULT 0,
            priority_label TEXT NOT NULL DEFAULT 'low',
            why_it_matters TEXT,
            evidence_url TEXT NOT NULL,
            ingested_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (source_id) REFERENCES trusted_news_sources(source_id)
        )
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_items_source_date
        ON news_items(source_id, published_at)
        """
    )

    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_items_relevance
        ON news_items(relevance_score DESC, ingested_at DESC)
        """
    )

def _migration_006_news_relevance_refinement(connection: sqlite3.Connection) -> None:
    if not _table_exists(connection, "news_items"):
        return

    if not _column_exists(connection, "news_items", "priority_label"):
        connection.execute(
            "ALTER TABLE news_items ADD COLUMN priority_label TEXT NOT NULL DEFAULT 'low'"
        )

    if not _column_exists(connection, "news_items", "why_it_matters"):
        connection.execute(
            "ALTER TABLE news_items ADD COLUMN why_it_matters TEXT"
        )

def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _column_exists(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)