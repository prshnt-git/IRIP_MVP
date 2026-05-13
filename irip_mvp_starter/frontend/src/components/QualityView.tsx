import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, ChevronDown, ChevronUp, RefreshCw, ShieldCheck } from "lucide-react";
import type { VisualDashboard, ExecutiveReport, KpiCard } from "../api";
import { fetchDedupStats } from "../api";

// ─── Local helpers (mirrored from App.tsx for standalone use) ─────────────────

type Tone = "good" | "bad" | "warn" | "neutral" | "primary";

function formatValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return String(value);
    return value.toFixed(value > 10 ? 1 : 2);
  }
  return value;
}

function getReviewCount(dashboard: VisualDashboard): number {
  const card = dashboard.kpi_cards.find((item) => item.id === "review_count");
  const numeric = Number(card?.value || 0);
  return Number.isFinite(numeric) ? numeric : 0;
}

function buildEvidenceLevel(
  dashboard: VisualDashboard,
  report: ExecutiveReport | null
) {
  const reviewCount = getReviewCount(dashboard);
  const confidence =
    report?.confidence_note ||
    "Confidence depends on review volume, data quality, and evidence coverage.";

  if (reviewCount === 0)
    return {
      label: "No Evidence",
      tone: "bad" as Tone,
      text: "No usable review evidence is available yet.",
    };
  if (reviewCount < 30)
    return {
      label: "Early Signal",
      tone: "warn" as Tone,
      text: "Useful for early pattern discovery, not final product judgment.",
      helper: confidence,
    };
  if (reviewCount < 100)
    return {
      label: "Directional",
      tone: "warn" as Tone,
      text: "Enough evidence for directional reading, but still validate major claims.",
      helper: confidence,
    };
  if (reviewCount < 500)
    return {
      label: "Stronger Signal",
      tone: "good" as Tone,
      text: "Review volume is strong enough for more meaningful product interpretation.",
      helper: confidence,
    };
  return {
    label: "High Confidence",
    tone: "good" as Tone,
    text: "Large review volume supports high-confidence pattern reading.",
    helper: confidence,
  };
}

function buildQualityLabel(value: number): string {
  if (!Number.isFinite(value) || value <= 0) return "Unknown";
  if (value >= 0.85) return "Good";
  if (value >= 0.7) return "Usable";
  if (value >= 0.5) return "Needs Review";
  return "Weak";
}

function buildTrustCards(
  dashboard: VisualDashboard,
  report: ExecutiveReport | null
): KpiCard[] {
  const evidence = buildEvidenceLevel(dashboard, report);
  const reviewCount = getReviewCount(dashboard);
  const qualityCard = dashboard.kpi_cards.find((item) => item.id === "quality_score");
  const qualityValue = Number(qualityCard?.value || 0);
  const qualityLabel = buildQualityLabel(qualityValue);

  return [
    {
      id: "trust_confidence_level",
      label: "Confidence Level",
      value: evidence.label,
      helper_text: evidence.text,
      status: evidence.tone,
    },
    {
      id: "trust_review_coverage",
      label: "Review Coverage",
      value: reviewCount,
      helper_text:
        reviewCount === 0
          ? "No review sample available."
          : reviewCount < 30
            ? "Small sample. Read as early signal."
            : reviewCount < 100
              ? "Usable sample for directional analysis."
              : "Stronger sample for product interpretation.",
      status: reviewCount < 30 ? "warn" : "good",
    },
    {
      id: "trust_data_quality",
      label: "Data Quality",
      value: qualityLabel,
      helper_text: "Readable review text improves aspect and sentiment extraction.",
      status:
        qualityValue >= 0.75 ? "good" : qualityValue >= 0.5 ? "warn" : "bad",
    },
  ];
}

function confidencePercent(label: string): number {
  const map: Record<string, number> = {
    "High Confidence": 95,
    "Stronger Signal": 75,
    "Directional": 50,
    "Early Signal": 25,
    "No Evidence": 5,
  };
  return map[label] ?? 0;
}

// ─── Status Card ──────────────────────────────────────────────────────────────

function StatusCard({
  label,
  value,
  sub,
  indicator,
  progress,
}: {
  label: string;
  value: string | number;
  sub: string;
  indicator: "good" | "warn" | "bad" | "neutral";
  progress?: number;
}) {
  return (
    <article className="trv-status-card">
      <div className="trv-status-card-top">
        <span className="trv-status-label">{label}</span>
        <span className={`trv-status-dot trv-status-dot--${indicator}`} />
      </div>
      <strong className="trv-status-value">{value}</strong>
      {progress !== undefined && (
        <div className="trv-progress-bar">
          <div
            className={`trv-progress-fill trv-progress-fill--${indicator}`}
            style={{ width: `${Math.min(progress, 100)}%` }}
          />
        </div>
      )}
      <span className="trv-status-sub">{sub}</span>
    </article>
  );
}

// ─── Skeleton ─────────────────────────────────────────────────────────────────

function StatusSkeleton() {
  return (
    <div className="trv-status-grid">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="trv-skeleton trv-skeleton--status" />
      ))}
    </div>
  );
}

// ─── Error card ───────────────────────────────────────────────────────────────

function ErrorCard({ onRetry }: { onRetry: () => void }) {
  return (
    <div className="trv-error-card">
      <AlertTriangle size={16} />
      <div>
        <strong>Data temporarily unavailable</strong>
        <p>System status could not be loaded right now.</p>
      </div>
      <button className="trv-retry-btn" type="button" onClick={onRetry}>
        <RefreshCw size={13} />
        Retry
      </button>
    </div>
  );
}

// ─── How This Works ───────────────────────────────────────────────────────────

function HowItWorks() {
  const [open, setOpen] = useState(false);
  return (
    <div className="trv-how-section">
      <button
        className="trv-how-btn"
        type="button"
        onClick={() => setOpen((v) => !v)}
      >
        <ShieldCheck size={15} />
        <span>How This Works</span>
        {open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
      </button>
      {open && (
        <div className="trv-how-body">
          <p className="trv-how-para">
            <strong>Review collection.</strong> IRIP scrapes Amazon.in and
            Flipkart product pages for the selected smartphone, collecting
            all publicly visible customer reviews. Each review is fingerprinted
            with a SHA-256 hash to prevent duplicate processing — only new
            reviews since the last run are ingested.
          </p>
          <p className="trv-how-para">
            <strong>Gemini analysis.</strong> Each review is passed to Gemini
            2.5 Flash which identifies the aspect being discussed (camera,
            battery, display, etc.), the sentiment (positive, negative, neutral),
            the intensity of that sentiment, and an evidence span — the exact
            phrase that supports the classification. In selective mode, Gemini
            is only invoked for reviews that the rule-based pipeline cannot
            classify with sufficient confidence.
          </p>
          <p className="trv-how-para">
            <strong>Confidence scores.</strong> Each extracted signal carries a
            confidence score (0–1) reflecting how certain the model is about
            its aspect and sentiment classification. Scores above 0.75 are
            treated as high confidence. Aggregated across enough reviews (100+),
            these signals form statistically reliable patterns. With fewer than
            30 reviews, treat all patterns as directional early signals only.
          </p>
        </div>
      )}
    </div>
  );
}

// ─── StatusPill (local) ───────────────────────────────────────────────────────

function StatusPill({ label, tone }: { label: string; tone: Tone }) {
  return <span className={`status-pill ${tone}`}>{label}</span>;
}

// ─── Main export ──────────────────────────────────────────────────────────────

export default function QualityView({
  dashboard,
  report,
}: {
  dashboard: VisualDashboard;
  report: ExecutiveReport | null;
}) {
  const {
    data: dedupStats,
    isLoading: dedupLoading,
    isError: dedupError,
    refetch,
  } = useQuery({
    queryKey: ["dedupStats"],
    queryFn: fetchDedupStats,
    staleTime: 5 * 60 * 1000,
    retry: 1,
  });

  const evidence = buildEvidenceLevel(dashboard, report);
  const trustCards = buildTrustCards(dashboard, report);
  const reviewCount = getReviewCount(dashboard);
  const confPct = confidencePercent(evidence.label);

  const freshnessIndicator: "good" | "warn" | "bad" =
    dedupStats && dedupStats.scraped_today > 0
      ? "good"
      : dedupStats && dedupStats.scraped_today === 0
        ? "warn"
        : "neutral" as "warn";

  const freshnessValue =
    dedupStats && dedupStats.scraped_today > 0
      ? `${dedupStats.scraped_today} new today`
      : "No new reviews today";

  const reviewIndicator: "good" | "warn" | "bad" =
    reviewCount >= 100 ? "good" : reviewCount >= 30 ? "warn" : "bad";

  const confIndicator: "good" | "warn" | "bad" =
    confPct >= 75 ? "good" : confPct >= 40 ? "warn" : "bad";

  return (
    <div className="trv-root">
      {/* Header */}
      <div className="trv-header">
        <div>
          <p className="irip-eyebrow">Trust Layer</p>
          <h2>Can we trust this analysis?</h2>
        </div>
        <StatusPill label={evidence.label} tone={evidence.tone} />
      </div>

      {/* System Status */}
      <section className="trv-section">
        <div className="trv-section-head">
          <span className="trv-eyebrow">System Status</span>
          <h3>Current data pipeline health</h3>
        </div>
        {dedupLoading ? (
          <StatusSkeleton />
        ) : dedupError ? (
          <ErrorCard onRetry={() => void refetch()} />
        ) : (
          <div className="trv-status-grid">
            <StatusCard
              label="Total Reviews"
              value={reviewCount}
              indicator={reviewIndicator}
              sub={
                reviewCount >= 100
                  ? "Strong sample"
                  : reviewCount >= 30
                    ? "Directional sample"
                    : "Small sample"
              }
            />
            <StatusCard
              label="Data Freshness"
              value={freshnessValue}
              indicator={freshnessIndicator}
              sub={
                dedupStats && dedupStats.scraped_today > 0
                  ? "Pipeline active"
                  : "Check scraper logs"
              }
            />
            <StatusCard
              label="Analysis Confidence"
              value={`${confPct}%`}
              indicator={confIndicator}
              sub={evidence.label}
              progress={confPct}
            />
            <StatusCard
              label="Duplicates Prevented"
              value={dedupStats?.total_seen ?? "—"}
              indicator="good"
              sub="Total dedup checks logged"
            />
          </div>
        )}
      </section>

      {/* Evidence Quality */}
      <section className="trv-section">
        <div className="trv-section-head">
          <span className="trv-eyebrow">Evidence Quality</span>
          <h3>Review analysis confidence breakdown</h3>
        </div>
        <div className="quality-card-grid">
          {trustCards.map((card) => (
            <article className="quality-card" key={card.id}>
              <span>{card.label}</span>
              <strong>{formatValue(card.value)}</strong>
              <p>{card.helper_text || "No note available."}</p>
            </article>
          ))}
        </div>
      </section>

      {/* Explainer */}
      <div className="quality-explainer">
        <ShieldCheck size={18} />
        <div>
          <strong>How to use this analysis</strong>
          <p>
            {evidence.text} Keep claims evidence-linked, and avoid treating
            small-sample patterns as final product truth.
          </p>
        </div>
      </div>

      {/* How This Works */}
      <HowItWorks />
    </div>
  );
}
