# CLAUDE.md — IRIP Project Bible
## Read this entire file before writing any code.

---

## WHO THIS IS FOR
You are Claude Code. This file gives you complete context on this project.
Never ask the user to re-explain the project. Never ask "what does IRIP stand for?"
Everything you need is here. When in doubt, refer to this file.

---

## PROJECT IDENTITY
**Name:** IRIP — India Review Intelligence Platform
**Owner:** Prashant (Transsion India — brands: itel, Infinix, Tecno)
**Purpose:** Automatically scrape Amazon.in and Flipkart reviews of smartphones,
analyze them using Gemini AI, and present competitive intelligence through a dashboard.
**Type:** Personal/portfolio project — no corporate compliance constraints.
**Status:** MVP deployed. Now upgrading to production quality.

---

## LIVE DEPLOYMENTS
| Layer | URL | Platform |
|-------|-----|----------|
| Frontend | https://irip-mvp.vercel.app | Vercel |
| Backend API | https://irip-api.onrender.com | Render (free tier) |
| Health check | https://irip-api.onrender.com/health | — |

---

## TECH STACK — DO NOT DEVIATE WITHOUT ASKING

### Backend
- **Language:** Python 3.12
- **Framework:** FastAPI (latest)
- **Database:** SQLite (local file, path: backend/data/irip.db) — upgrade to Supabase PostgreSQL in Phase 3
- **ORM:** SQLAlchemy (existing codebase) — upgrade to 2.0 style when rewriting files
- **AI:** Gemini 2.5 Flash via google-generativeai SDK (NEVER switch models without asking)
- **Deployment:** Render free tier — keep startup time under 30 seconds
- **Key env vars:** GEMINI_API_KEY, GEMINI_MODEL=gemini-2.5-flash, IRIP_LLM_MODE=selective, IRIP_LLM_PROVIDER=gemini

### Frontend
- **Framework:** React 19 + TypeScript 6
- **Build:** Vite 8
- **Charts:** Apache ECharts via echarts-for-react
- **Icons:** lucide-react
- **HTTP:** fetch() via /src/api.ts — all calls prefix with /api which Vite proxies locally and Vercel rewrites to Render in production
- **Adding:** shadcn/ui (Radix UI components), TanStack Query v5, Zustand, Framer Motion, Tailwind CSS v4
- **Deployment:** Vercel — vercel.json in frontend/ rewrites /api/* to irip-api.onrender.com/*

---

## PROJECT STRUCTURE
```
irip_mvp_starter/
├── CLAUDE.md                    ← THIS FILE
├── backend/
│   ├── app/
│   │   ├── main.py             ← All FastAPI routes (40+ endpoints)
│   │   ├── core/config.py      ← Settings via pydantic-settings
│   │   ├── db/
│   │   │   ├── database.py     ← SQLite connection
│   │   │   └── repository.py   ← All DB queries
│   │   ├── pipeline/
│   │   │   ├── hybrid_analyzer.py   ← Main analysis pipeline
│   │   │   └── review_analyzer.py   ← Rule-based analysis
│   │   ├── schemas/            ← Pydantic models (one file per domain)
│   │   ├── sample_data/
│   │   │   ├── sample_reviews.csv
│   │   │   └── sample_product_catalog.csv
│   │   └── [other modules]
│   ├── requirements.txt
│   └── .env                    ← NEVER commit this file
└── frontend/
    ├── src/
    │   ├── api.ts              ← All API calls — all use /api prefix
    │   ├── App.tsx             ← Main app + routing
    │   └── [components]
    ├── vercel.json             ← Rewrites /api/* to irip-api.onrender.com/*
    ├── vite.config.ts          ← Dev proxy: /api → localhost:8000 (strips /api)
    └── package.json
```

---

## API ROUTES (already built — do not recreate)
```
GET  /health                                    System health
GET  /products                                  List all products
GET  /products/{id}/summary                     Product metrics
GET  /products/{id}/themes                      Complaint/delight themes
GET  /products/{id}/forecast                    Sentiment forecast
GET  /products/{id}/intelligence-brief          Gemini-written brief
GET  /products/{id}/aspects                     Aspect scores
GET  /products/{id}/evidence                    Review evidence
GET  /products/{id}/competitors                 Competitor list
GET  /products/{id}/benchmark/{competitor_id}   Head-to-head benchmark
POST /reviews/import-csv                        Upload CSV file
POST /reviews/import-csv-url                    Import CSV from URL
POST /reviews/analyze                           Analyze single review
GET  /llm/status                                Gemini status
POST /llm/mode                                  Toggle LLM mode
GET  /news/items                                News feed
POST /news/ingest-rss                           Ingest RSS feed
GET  /news/brief                                News summary
GET  /reports/executive                         Executive report
GET  /visuals/dashboard                         Dashboard data
GET  /feedback/extraction                       Analyst feedback
POST /feedback/extraction                       Submit feedback
GET  /feedback/provider-quality                 Model accuracy
GET  /acquisition/providers                     Scraper status
GET  /reviews/sources                           Review sources
GET  /reviews/duplicates                        Duplicate detection
```

---

## THE 7 DASHBOARD TABS (user-facing features)

| Tab | Status | Description |
|-----|--------|-------------|
| Overview | Partial | Key metrics, summary cards, top praise/complaint, main graphs |
| Insights | Partial | AI-driven product advice, forecasting, actionability |
| Sentiment | Partial | All sentiment graphs, aspect drill-down, review evidence cards |
| Benchmark | Partial | Head-to-head vs competitor, radar chart, gap analysis |
| Market | NOT BUILT | India smartphone news, upcoming launches, competitor tracking |
| Trust | NOT BUILT | Data quality metrics, confidence scores, last scrape info |
| Report | NOT BUILT | Gemini Pro narrative + downloadable PDF |

---

## THE 3 DROPDOWNS (main user controls)
1. **Own product selector** — shows only Transsion products (itel/Infinix/Tecno, is_own_brand=true)
2. **Competitor selector** — shows any product (optional)
3. **Time period** — 3 preset buttons: Last 30 days / Last 90 days / Last 120 days + custom date range

---

## PRODUCT CONTEXT — CRITICAL FOR ANALYSIS
- **Own brands:** itel, Infinix, Tecno (all under Transsion India)
- **Market:** India — budget (₹5k–15k) and mid-range (₹15k–25k) segments
- **Review language:** English + Hinglish (Romanized Hindi mixed with English)
- **Platforms scraped:** Amazon.in, Flipkart
- **Hinglish sentiment guide:**
  - POSITIVE: mast, zabardast, ekdum, best, dhansu, paisa vasool, solid
  - NEGATIVE: bakwas, bekar, faltu, khatam, worst, ganda, waste
  - NEUTRAL: theek hai, average, okay, decent, ठीक ठाक
  - SARCASM FLAG: "wah wah", "kya kehna", comparison to premium brand for budget phone

---

## CODING RULES — FOLLOW THESE EXACTLY

1. **All Gemini API calls** go through the existing pipeline/hybrid_analyzer.py — never add new direct Gemini calls elsewhere
2. **All DB queries** go through db/repository.py — never write raw SQL in routes
3. **All frontend API calls** go through src/api.ts — never use fetch() directly in components
4. **Never put secrets in code** — use environment variables only, access via config.py
5. **Type everything** — no `any` in TypeScript, no untyped Python functions
6. **Write complete files** — when editing a file, return the complete rewritten file, not snippets
7. **One file per session** — focus on one file at a time for clean, reviewable changes
8. **Error handling everywhere** — try/catch in TS, try/except in Python, meaningful error messages
9. **No SQLite in production plan** — when upgrading DB, use Supabase PostgreSQL
10. **Keep Render startup fast** — avoid heavy imports at module level in main.py

---

## DEDUPLICATION STRATEGY
When building the scraper automation:
- Use SHA256 hash of: `product_id + source + review_date + text[:50]`
- Store hashes in a `scrape_log` table
- Before any Gemini analysis, check if hash exists — skip if found
- Only scrape last 7 days of reviews per run (Amazon/Flipkart sort by recent first)
- This means: day 1 processes 200+ reviews, day 30 processes only 5-20 new reviews

---

## GOOGLE SHEETS STRUCTURE (when building Sheets sync)
One Google Spreadsheet with 5 tabs, write-only from backend:
1. **Product Catalog** — all products with specs
2. **Raw Reviews** — every scraped review
3. **Cleaned Reviews** — after preprocessing (delivery reviews removed, text normalized)
4. **Processed Sentiment** — review + sentiment + aspect + key phrase + confidence
5. **Daily Summary** — one row per product per day (avg rating, % positive/negative, top complaint, top praise)

---

## PHASE PLAN (current progress)
- ✅ Phase 0: Old project (ChatGPT) — backend + frontend scaffolding done
- 🔄 Phase 1 (Day 1): Fix vercel.json, import real data, confirm all tabs work
- 📋 Phase 2 (Day 2): Dedup pipeline, GitHub Actions automation, Sheets sync
- 📋 Phase 3 (Day 3): shadcn/ui upgrade, Market tab, Trust tab, Report PDF
- 📋 Phase 4 (ongoing): Supabase migration, spec collection, regional languages

---

## WHAT "DONE" LOOKS LIKE
A Transsion product manager opens irip-mvp.vercel.app, selects Infinix Hot 50 Pro,
selects Samsung Galaxy M35 as competitor, clicks "Last 90 days", clicks Generate.
Within 3 seconds they see: overall sentiment score, top 3 praises, top 3 complaints,
aspect radar chart vs competitor, complaint cluster timeline, AI intelligence brief,
market news about India smartphone segment, data quality metrics, and can download
a professional PDF report — all from real scraped review data, updated daily automatically.

---

## SESSION START CHECKLIST
Before writing any code in a new session:
1. Read this entire CLAUDE.md
2. Confirm which single file you're working on today
3. Check if the file already exists and read it first
4. Write the complete file (not snippets)
5. Tell the user exactly what command to run to test it

---
*Last updated: May 2026 | Version: 1.0*
