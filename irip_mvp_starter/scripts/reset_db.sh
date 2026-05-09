#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_DIR="$ROOT/backend/data"
DB_FILE="$DB_DIR/irip_mvp.db"
rm -f "$DB_FILE" "$DB_FILE-shm" "$DB_FILE-wal"
mkdir -p "$DB_DIR"
echo "Reset complete: removed SQLite database at $DB_FILE"
echo "Restart uvicorn, then re-import your CSV."
