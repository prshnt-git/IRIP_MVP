# IRIP MVP Architecture

## Product goal

IRIP is an internal OEM smartphone review intelligence product for India. It converts messy Indian e-commerce review text into evidence-backed product, competitor, and trend intelligence.

## MVP foundation

This starter implements the core intelligence foundation before UI polish:

1. **Resource Router** — provider-agnostic model/API routing by task.
2. **Living Lexicon** — versionable term intelligence for Hinglish, smartphone aspects, service/delivery noise, intensity, and sentiment priors.
3. **Evaluation Loop** — repeatable test cases for measuring extractor quality before trusting outputs.
4. **Review Pipeline** — cleaning, normalization, language/signal hints, and explainable local aspect sentiment extraction.

## Why this structure

The system must not be hardcoded to one LLM, one model, or one static dictionary. Indian smartphone review language changes quickly. The MVP therefore starts with modular routing and lexicon storage so better free/open-source APIs and models can be plugged in later without breaking working features.

## Current constraints

- CSV/API ingestion comes next.
- Frontend workspace comes after stable backend contracts.
- Local providers are the default to keep cost near zero.
- Gemini or other LLM providers are optional fallback routes, not the foundation.
