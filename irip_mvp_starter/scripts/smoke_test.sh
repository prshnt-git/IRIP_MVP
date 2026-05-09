#!/usr/bin/env bash
set -euo pipefail
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

echo "Checking $BASE_URL/health"
curl -fsS "$BASE_URL/health" | python -m json.tool

echo "Checking products"
curl -fsS "$BASE_URL/products" | python -m json.tool

echo "Checking DB stats"
curl -fsS "$BASE_URL/debug/db-stats" | python -m json.tool
