import { useMemo } from "react";
import { motion } from "framer-motion";
import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, ChevronRight } from "lucide-react";
import type { VisualDashboard, ExecutiveReport, KpiCard } from "../api";
import { fetchCompetitorBenchmark } from "../api";

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

function labelize(value?: string | null): string {
  if (!value) return "Unknown";
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function statusTone(status?: string | null): "good" | "warn" | "bad" | "neutral" {
  const n = (status || "").toLowerCase();
  if (n.includes("ready") || n === "active" || n === "pass" || n === "good") return "good";
  if (n.includes("warn") || n.includes("directional") || n.includes("low") || n.includes("limitation"))
    return "warn";
  if (n.includes("fail") || n.includes("negative") || n.includes("error") || n === "bad")
    return "bad";
  return "neutral";
}

function ensureSentence(value: string): string {
  if (!value) return "Not enough evidence yet.";
  return /[.!?…]$/.test(value) ? value : `${value}.`;
}

function compressInsight(value: string): string {
  const cleaned = value
    .replace("The selected product", "Selected product")
    .replace("the selected product", "selected product")
    .replace("Treat as directional because evidence volume/confidence is still low.", "")
    .replace("Evidence strength:", "")
    .replace(/\s+/g, " ")
    .trim();
  if (cleaned.length <= 96) return ensureSentence(cleaned);
  const firstSentence = cleaned.split(".")[0]?.trim();
  if (firstSentence && firstSentence.length <= 96) return ensureSentence(firstSentence);
  return ensureSentence(`${cleaned.slice(0, 92).trim()}…`);
}

function pickFirst(items?: string[] | null, fallback = "Not enough evidence yet."): string {
  return items?.find((item) => item && item.trim()) || fallback;
}

function cleanThemeText(text: string): string {
  return text
    .replace(/:\s*Camera delight theme\.?/gi, " is showing early delight.")
    .replace(/:\s*Battery complaint theme\.?/gi, " is the main complaint area to validate.")
    .replace(/\bphone_a\b/gi, "the selected product")
    .replace(/\bphone_b\b/gi, "the competitor")
    .trim();
}

function getReviewCount(dashboard: VisualDashboard): number {
  const card = dashboard.kpi_cards.find((item) => item.id === "review_count");
  const numeric = Number(card?.value || 0);
  return Number.isFinite(numeric) ? numeric : 0;
}

function buildSentimentRead(dashboard: VisualDashboard): {
  title: string;
  helper: string;
  tone: Tone;
} {
  const sentimentData = dashboard.sentiment_distribution_chart?.data || [];
  const positive =
    sentimentData.find((item) => item.label.toLowerCase() === "positive")?.value || 0;
  const negative =
    sentimentData.find((item) => item.label.toLowerCase() === "negative")?.value || 0;
  const neutral =
    sentimentData.find((item) => item.label.toLowerCase() === "neutral")?.value || 0;

  if (positive === 0 && negative === 0 && neutral === 0)
    return { title: "No sentiment pattern yet.", helper: "Import more reviews to read user mood.", tone: "neutral" };
  if (positive > negative * 1.25)
    return { title: "Positive sentiment is leading.", helper: `${positive} positive vs ${negative} negative aspect signal(s).`, tone: "good" };
  if (negative > positive * 1.25)
    return { title: "Negative sentiment is leading.", helper: `${negative} negative vs ${positive} positive aspect signal(s).`, tone: "bad" };
  return { title: "Sentiment is mixed.", helper: `${positive} positive and ${negative} negative aspect signal(s).`, tone: "warn" };
}

function buildCompetitorContext(
  dashboard: VisualDashboard,
  report: ExecutiveReport | null
): { title: string; helper: string; tone: Tone } {
  if (!dashboard.competitor_product_id)
    return {
      title: "Product-only view.",
      helper: "Select a competitor only when benchmark comparison is needed.",
      tone: "neutral",
    };
  const takeaway = compressInsight(
    cleanThemeText(
      pickFirst(
        report?.competitor_takeaways,
        dashboard.competitor_gap_chart?.data?.[0]?.interpretation || "Benchmark evidence is available."
      )
    )
  );
  return { title: takeaway, helper: "Directional competitor context from aspect-level gaps.", tone: "primary" };
}

type InsightCard = { id: string; label: string; title: string; helper: string; tone: Tone };

function buildUserInsightCards(
  dashboard: VisualDashboard,
  report: ExecutiveReport | null
): InsightCard[] {
  const reviewCount = getReviewCount(dashboard);
  const strength = compressInsight(
    cleanThemeText(pickFirst(report?.key_strengths, "No clear positive customer signal yet."))
  );
  const risk = compressInsight(
    cleanThemeText(pickFirst(report?.key_risks, "No clear risk signal yet."))
  );
  const topAspect = dashboard.top_aspect_chart?.data?.[0]?.label
    ? labelize(dashboard.top_aspect_chart.data[0].label)
    : "No dominant aspect yet";
  const sentimentRead = buildSentimentRead(dashboard);
  const competitorContext = buildCompetitorContext(dashboard, report);

  return [
    { id: "customer_like", label: "Customer Like", title: strength, helper: "Strongest positive signal found in review evidence.", tone: "good" },
    { id: "customer_complaint", label: "Customer Complaint", title: risk, helper: "Most important negative signal to understand.", tone: "bad" },
    {
      id: "evidence_base",
      label: "Evidence Base",
      title: reviewCount < 30
        ? `${reviewCount} usable review(s). Treat as early signal.`
        : `${reviewCount} usable reviews. Stronger sample size.`,
      helper: `${topAspect} is currently the most discussed aspect.`,
      tone: reviewCount < 30 ? "warn" : "good",
    },
    { id: "sentiment_read", label: "Sentiment Read", title: sentimentRead.title, helper: sentimentRead.helper, tone: sentimentRead.tone },
    { id: "competitor_context", label: "Competitor Context", title: competitorContext.title, helper: competitorContext.helper, tone: competitorContext.tone },
  ];
}

function buildOverviewKpis(dashboard: VisualDashboard): KpiCard[] {
  const reviewCount = getReviewCount(dashboard);
  const rating = dashboard.kpi_cards.find((item) => item.id === "average_rating");
  const sentimentData = dashboard.sentiment_distribution_chart?.data || [];
  const positive = sentimentData.find((item) => item.label.toLowerCase() === "positive")?.value || 0;
  const negative = sentimentData.find((item) => item.label.toLowerCase() === "negative")?.value || 0;

  return [
    { id: "overview_review_sample", label: "Review Sample", value: reviewCount, helper_text: reviewCount < 30 ? "Small sample. Read as early signal." : "Usable sample for directional reads.", status: reviewCount < 30 ? "low_volume" : "usable" },
    { id: "overview_avg_rating", label: "Avg Rating", value: rating?.value ?? "—", helper_text: "Marketplace/user rating average.", status: null },
    { id: "overview_positive_signals", label: "Positive Signals", value: positive, helper_text: "Positive aspect-level mentions found.", status: positive > negative ? "good" : null },
    { id: "overview_negative_signals", label: "Negative Signals", value: negative, helper_text: "Negative aspect-level mentions found.", status: negative > positive ? "warn" : null },
  ];
}

// ─── Framer Motion variants ────────────────────────────────────────────────────

const STAGGER_CONTAINER = {
  animate: { transition: { staggerChildren: 0.1 } },
};

const STAGGER_ITEM = {
  initial: { opacity: 0, y: 12 },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.35, ease: "easeOut" as const },
  },
};

// ─── Fairness Banner ──────────────────────────────────────────────────────────

function FairnessBanner({
  productName,
  competitorId,
  ratio,
}: {
  productName: string;
  competitorId: string;
  ratio: number;
}) {
  return (
    <div className="ovv-fairness-banner">
      <AlertTriangle size={14} />
      <span>
        Note: <strong>{productName}</strong> has ~{ratio}× more reviews than{" "}
        <strong>{competitorId}</strong> — comparison may favour statistical
        significance.
      </span>
    </div>
  );
}

// ─── Main export ──────────────────────────────────────────────────────────────

export default function OverviewView({
  dashboard,
  report,
  onOpenEvidence,
  productId,
  competitorId,
  startDate,
  endDate,
  productName,
}: {
  dashboard: VisualDashboard;
  report: ExecutiveReport | null;
  onOpenEvidence: () => void;
  productId: string;
  competitorId: string;
  startDate: string;
  endDate: string;
  productName: string;
}) {
  const insightCards = buildUserInsightCards(dashboard, report);
  const overviewKpis = buildOverviewKpis(dashboard);

  const lastUpdated = useMemo(
    () =>
      new Date().toLocaleTimeString("en-IN", {
        hour: "2-digit",
        minute: "2-digit",
      }),
    []
  );

  const { data: benchmark } = useQuery({
    queryKey: ["benchmark", productId, competitorId, startDate, endDate],
    queryFn: () =>
      fetchCompetitorBenchmark(productId, competitorId, {
        start_date: startDate || undefined,
        end_date: endDate || undefined,
      }),
    enabled: Boolean(productId && competitorId),
    staleTime: 5 * 60 * 1000,
  });

  const showFairness =
    !!benchmark &&
    benchmark.own_review_count > 0 &&
    benchmark.competitor_review_count > 0 &&
    benchmark.own_review_count > benchmark.competitor_review_count * 2;

  const fairnessRatio = benchmark
    ? Math.round(
        benchmark.own_review_count / Math.max(benchmark.competitor_review_count, 1)
      )
    : 1;

  return (
    <div className="overview-view">
      {/* Header */}
      <div className="tile-section-header">
        <div>
          <p className="irip-eyebrow">Overview</p>
          <h2>What matters right now</h2>
          <span className="ovv-last-updated">Last updated {lastUpdated}</span>
        </div>
        <button className="micro-button" type="button" onClick={onOpenEvidence}>
          Evidence
          <ChevronRight size={14} />
        </button>
      </div>

      {/* Fairness banner */}
      {showFairness && (
        <FairnessBanner
          productName={productName}
          competitorId={competitorId}
          ratio={fairnessRatio}
        />
      )}

      {/* Insight cards — stagger animated */}
      <motion.div
        className="insight-card-grid"
        variants={STAGGER_CONTAINER}
        initial="initial"
        animate="animate"
      >
        {insightCards.map((card) => (
          <motion.article
            className={`insight-card ${card.tone}`}
            key={card.id}
            variants={STAGGER_ITEM}
          >
            <span>{card.label}</span>
            <strong>{card.title}</strong>
            <p>{card.helper}</p>
          </motion.article>
        ))}
      </motion.div>

      {/* KPI cards — stagger animated */}
      <motion.div
        className="kpi-card-grid"
        variants={STAGGER_CONTAINER}
        initial="initial"
        animate="animate"
      >
        {overviewKpis.map((card) => (
          <motion.article
            className="kpi-card"
            key={card.id}
            variants={STAGGER_ITEM}
          >
            <div className="kpi-card-label-row">
              <span>{card.label}</span>
              {card.status ? (
                <span
                  className={`status-pill ${statusTone(card.status)} compact`}
                >
                  {labelize(card.status)}
                </span>
              ) : null}
            </div>
            <strong>{formatValue(card.value)}</strong>
            <p>{card.helper_text || "—"}</p>
          </motion.article>
        ))}
      </motion.div>
    </div>
  );
}
