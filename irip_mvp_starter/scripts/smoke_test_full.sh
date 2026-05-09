#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

echo "Checking backend health..."
curl -s "$BASE_URL/health" | python -m json.tool

echo ""
echo "Checking system version..."
curl -s "$BASE_URL/system/version" | python -m json.tool

echo ""
echo "Checking system readiness..."
curl -s "$BASE_URL/system/readiness" | python -m json.tool

echo ""
echo "Checking products..."
curl -s "$BASE_URL/products" | python -m json.tool

echo ""
echo "Checking LLM status..."
curl -s "$BASE_URL/llm/status" | python -m json.tool

echo ""
echo "Checking DB stats..."
curl -s "$BASE_URL/debug/db-stats" | python -m json.tool

echo ""
echo "Checking provider quality..."
curl -s "$BASE_URL/feedback/provider-quality" | python -m json.tool

echo ""
echo "Checking golden evaluation..."
curl -s "$BASE_URL/evaluation/golden?mode=rules" | python -m json.tool

echo ""
echo "Checking golden evaluation mode comparison..."
curl -s "$BASE_URL/evaluation/golden/compare" | python -m json.tool

echo ""
echo "Checking lexicon search..."
curl -s "$BASE_URL/lexicon?search=mast" | python -m json.tool

echo ""
echo "Checking sample product summary..."
curl -s "$BASE_URL/products/phone_a/summary" | python -m json.tool

echo ""
echo "Checking sample evidence..."
curl -s "$BASE_URL/products/phone_a/evidence?limit=5" | python -m json.tool

echo ""
echo "Checking sample benchmark..."
curl -s "$BASE_URL/products/phone_a/benchmark/phone_b" | python -m json.tool

echo ""
echo "Full smoke test completed."

echo ""
echo "Building active evaluation queue..."
curl -s -X POST "$BASE_URL/evaluation/active-queue/build?limit=100" | python -m json.tool

echo ""
echo "Checking active evaluation queue..."
curl -s "$BASE_URL/evaluation/active-queue?status=open&limit=10" | python -m json.tool

echo ""
echo "Checking import preview validation..."
curl -s -X POST "$BASE_URL/reviews/import-preview" \
  -H "Content-Type: application/json" \
  --data-raw '{
    "csv_text": "review_id,product_id,raw_text,rating,review_date\nr1,Phone A,Camera mast hai,5,2026-04-01\nr2,Phone A,,4,2026-04-02"
  }' | python -m json.tool

echo ""
echo "Checking trusted news sources..."
curl -s "$BASE_URL/news/sources" | python -m json.tool

echo ""
echo "Checking trusted news RSS XML ingestion..."
curl -s -X POST "$BASE_URL/news/ingest-rss-xml" \
  -H "Content-Type: application/json" \
  --data-raw '{
    "source_id": "qualcomm_newsroom",
    "discovered_via": "smoke_fixture",
    "rss_xml": "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><rss version=\"2.0\"><channel><item><title>Qualcomm announces new Snapdragon platform with on-device AI for smartphones</title><link>https://www.qualcomm.com/news/releases/new-snapdragon-ai-smartphone</link><description>New chipset improves smartphone AI, camera, and NPU performance.</description><pubDate>Wed, 06 May 2026 10:00:00 GMT</pubDate></item></channel></rss>"
  }' | python -m json.tool

echo ""
echo "Rescoring trusted news items..."
curl -s -X POST "$BASE_URL/news/rescore?limit=100" | python -m json.tool

echo ""
echo "Checking trusted news items..."
curl -s "$BASE_URL/news/items?min_relevance_score=10&limit=5" | python -m json.tool

echo ""
echo "Checking trusted news intelligence brief..."
curl -s "$BASE_URL/news/brief?min_relevance_score=10&limit=5" | python -m json.tool

echo ""
echo "Checking executive report..."
curl -s "$BASE_URL/reports/executive?product_id=phone_a&competitor_product_id=phone_b" | python -m json.tool

echo ""
echo "Checking ECharts-ready visualization dashboard data..."
curl -s "$BASE_URL/visuals/dashboard?product_id=phone_a&competitor_product_id=phone_b" | python -m json.tool