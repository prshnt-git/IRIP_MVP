import { useEffect, useRef, useState, useMemo } from "react";
import ReactECharts from "echarts-for-react";
import {
  AlertTriangle,
  BarChart3,
  ChevronRight,
  Database,
  Download,
  FileText,
  Filter,
  Gauge,
  Layers3,
  Loader2,
  Newspaper,
  RefreshCw,
  Search,
  ShieldCheck,
  Sparkles,
  UploadCloud,
  X,
} from "lucide-react";
import { create } from "zustand";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  type ProductItem,
  type VisualDashboard,
  type ExecutiveReport,
  type EvidenceLink,
  type KpiCard,
  type ChartBlock,
  type CompetitorGapChart,
  type CompetitorGapDatum,
  type AspectReasonCard,
  type BenchmarkSummary,
  type BenchmarkSpecTable,
  type SignalChip,
  type ChartDatum,
  type ImportPreviewResponse,
  fetchProducts,
  fetchVisualDashboard,
  fetchExecutiveReport,
  importReviewsFromCsvUrl,
} from "./api";
import "./App.css";
import ReportView from "./components/ReportView";
import SentimentView from "./components/SentimentView";

// ============================================================
// Zustand store — shared selection state
// ============================================================

type TimePeriod = { startDate: string; endDate: string };

type WorkspaceState = {
  selectedProductId: string;
  selectedCompetitorId: string;
  timePeriod: TimePeriod;
  setSelectedProductId: (id: string) => void;
  setSelectedCompetitorId: (id: string) => void;
  setTimePeriod: (patch: Partial<TimePeriod>) => void;
};

const useWorkspaceStore = create<WorkspaceState>((set) => ({
  selectedProductId: "",
  selectedCompetitorId: "",
  timePeriod: { startDate: "", endDate: "" },
  setSelectedProductId: (id) => set({ selectedProductId: id }),
  setSelectedCompetitorId: (id) => set({ selectedCompetitorId: id }),
  setTimePeriod: (patch) =>
    set((s) => ({ timePeriod: { ...s.timePeriod, ...patch } })),
}));

// ============================================================
// TanStack Query client (provider added at root of App return)
// ============================================================

const queryClient = new QueryClient();

// ============================================================
// Local types (UI-only — not from API)
// ============================================================

type ViewKey =
  | "overview"
  | "summary"
  | "sentiment"
  | "competitor"
  | "news"
  | "quality"
  | "report";

type Tone = "good" | "bad" | "warn" | "neutral" | "primary";

type InsightCard = {
  id: string;
  label: string;
  title: string;
  helper: string;
  tone: Tone;
};

// ============================================================
// ECharts tooltip param shapes (avoids `any`)
// ============================================================

type EChartsItemParam = {
  marker: string;
  name: string;
  value: number;
};

type EChartsAxisParam = {
  name: string;
  value: number;
  dataIndex: number;
};

// ============================================================
// Constants
// ============================================================

const IRIP_DATA_WORKSPACE_URL =
  "https://docs.google.com/spreadsheets/d/1whhBvVjxHpOEqgY7JCGcjM0TXYh4dVlEIFhSRs9gnQI/edit?coid=1059348359&pli=1&gid=518385256#gid=518385256";

const views: { key: ViewKey; label: string; icon: typeof BarChart3 }[] = [
  { key: "overview", label: "Overview", icon: Layers3 },
  { key: "summary", label: "Insights", icon: Sparkles },
  { key: "sentiment", label: "Sentiment", icon: BarChart3 },
  { key: "competitor", label: "Benchmark", icon: Gauge },
  { key: "news", label: "Market", icon: Newspaper },
  { key: "quality", label: "Trust", icon: ShieldCheck },
  { key: "report", label: "Report", icon: FileText },
];

// ============================================================
// Module-level workspace fetcher (pure function, no closure deps)
// ============================================================

async function fetchWorkspace(
  productId: string,
  competitorId: string,
  startDate: string,
  endDate: string
): Promise<[VisualDashboard, ExecutiveReport]> {
  const params = {
    product_id: productId,
    competitor_product_id: competitorId || undefined,
    start_date: startDate || undefined,
    end_date: endDate || undefined,
  };
  return Promise.all([
    fetchVisualDashboard(params),
    fetchExecutiveReport(params),
  ]);
}

// ============================================================
// UI helper functions
// ============================================================

function formatValue(value: string | number | null | undefined) {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return String(value);
    return value.toFixed(value > 10 ? 1 : 2);
  }
  return value;
}

function labelize(value?: string | null) {
  if (!value) return "Unknown";
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function statusTone(status?: string | null): Exclude<Tone, "primary"> {
  const n = (status || "").toLowerCase();
  if (n.includes("ready") || n === "active" || n === "pass") return "good";
  if (
    n.includes("warn") ||
    n.includes("directional") ||
    n.includes("low") ||
    n.includes("limitation")
  )
    return "warn";
  if (
    n.includes("fail") ||
    n.includes("negative") ||
    n.includes("error")
  )
    return "bad";
  return "neutral";
}

function pickFirst(items?: string[] | null, fallback = "Not enough evidence yet.") {
  return items?.find((item) => item && item.trim()) || fallback;
}

function isDebugLikeBullet(text: string) {
  const lowered = text.toLowerCase();
  return (
    lowered.includes("has 1 review") ||
    lowered.includes("has 2 review") ||
    lowered.includes("has 3 review") ||
    lowered.includes("net sentiment score") ||
    lowered.includes("top delight signal") ||
    lowered.includes("top complaint signal") ||
    lowered.includes("in the selected period with average rating")
  );
}

function cleanThemeText(text: string) {
  return text
    .replace(/:\s*Camera delight theme\.?/gi, " is showing early delight.")
    .replace(
      /:\s*Battery complaint theme\.?/gi,
      " is the main complaint area to validate."
    )
    .replace(/\bphone_a\b/gi, "the selected product")
    .replace(/\bphone_b\b/gi, "the competitor")
    .trim();
}

function getReviewCount(dashboard: VisualDashboard) {
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
      helper:
        "Import product reviews before interpreting customer sentiment or product risks.",
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

function buildQualityLabel(value: number) {
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
  const qualityCard = dashboard.kpi_cards.find(
    (item) => item.id === "quality_score"
  );
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
      helper_text:
        "Readable review text improves aspect and sentiment extraction.",
      status:
        qualityValue >= 0.75 ? "good" : qualityValue >= 0.5 ? "warn" : "bad",
    },
  ];
}

function compressInsight(value: string) {
  const cleaned = value
    .replace("The selected product", "Selected product")
    .replace("the selected product", "selected product")
    .replace(
      "Treat as directional because evidence volume/confidence is still low.",
      ""
    )
    .replace("Evidence strength:", "")
    .replace(/\s+/g, " ")
    .trim();

  if (cleaned.length <= 96) return ensureSentence(cleaned);

  const firstSentence = cleaned.split(".")[0]?.trim();
  if (firstSentence && firstSentence.length <= 96)
    return ensureSentence(firstSentence);

  return ensureSentence(`${cleaned.slice(0, 92).trim()}…`);
}

function ensureSentence(value: string) {
  if (!value) return "Not enough evidence yet.";
  return /[.!?…]$/.test(value) ? value : `${value}.`;
}

function buildSentimentRead(dashboard: VisualDashboard): {
  title: string;
  helper: string;
  tone: Tone;
} {
  const sentimentData = dashboard.sentiment_distribution_chart?.data || [];
  const positive =
    sentimentData.find((item) => item.label.toLowerCase() === "positive")
      ?.value || 0;
  const negative =
    sentimentData.find((item) => item.label.toLowerCase() === "negative")
      ?.value || 0;
  const neutral =
    sentimentData.find((item) => item.label.toLowerCase() === "neutral")
      ?.value || 0;

  if (positive === 0 && negative === 0 && neutral === 0)
    return {
      title: "No sentiment pattern yet.",
      helper: "Import more reviews to read user mood.",
      tone: "neutral",
    };

  if (positive > negative * 1.25)
    return {
      title: "Positive sentiment is leading.",
      helper: `${positive} positive vs ${negative} negative aspect signal(s).`,
      tone: "good",
    };

  if (negative > positive * 1.25)
    return {
      title: "Negative sentiment is leading.",
      helper: `${negative} negative vs ${positive} positive aspect signal(s).`,
      tone: "bad",
    };

  return {
    title: "Sentiment is mixed.",
    helper: `${positive} positive and ${negative} negative aspect signal(s).`,
    tone: "warn",
  };
}

function buildCompetitorContext(
  dashboard: VisualDashboard,
  report: ExecutiveReport | null
): { title: string; helper: string; tone: Tone } {
  if (!dashboard.competitor_product_id)
    return {
      title: "Product-only view.",
      helper:
        "Select a competitor only when benchmark comparison is needed.",
      tone: "neutral",
    };

  const takeaway = compressInsight(
    cleanThemeText(
      pickFirst(
        report?.competitor_takeaways,
        dashboard.competitor_gap_chart?.data?.[0]?.interpretation ||
          "Benchmark evidence is available."
      )
    )
  );

  return {
    title: takeaway,
    helper: "Directional competitor context from aspect-level gaps.",
    tone: "primary",
  };
}

function buildUserInsightCards(
  dashboard: VisualDashboard,
  report: ExecutiveReport | null
): InsightCard[] {
  const reviewCount = getReviewCount(dashboard);
  const strength = compressInsight(
    cleanThemeText(
      pickFirst(report?.key_strengths, "No clear positive customer signal yet.")
    )
  );
  const risk = compressInsight(
    cleanThemeText(
      pickFirst(report?.key_risks, "No clear risk signal yet.")
    )
  );
  const topAspect = dashboard.top_aspect_chart?.data?.[0]?.label
    ? labelize(dashboard.top_aspect_chart.data[0].label)
    : "No dominant aspect yet";
  const sentimentRead = buildSentimentRead(dashboard);
  const competitorContext = buildCompetitorContext(dashboard, report);

  return [
    {
      id: "customer_like",
      label: "Customer Like",
      title: strength,
      helper: "Strongest positive signal found in review evidence.",
      tone: "good",
    },
    {
      id: "customer_complaint",
      label: "Customer Complaint",
      title: risk,
      helper: "Most important negative signal to understand.",
      tone: "bad",
    },
    {
      id: "evidence_base",
      label: "Evidence Base",
      title:
        reviewCount < 30
          ? `${reviewCount} usable review(s). Treat as early signal.`
          : `${reviewCount} usable reviews. Stronger sample size.`,
      helper: `${topAspect} is currently the most discussed aspect.`,
      tone: reviewCount < 30 ? "warn" : "good",
    },
    {
      id: "sentiment_read",
      label: "Sentiment Read",
      title: sentimentRead.title,
      helper: sentimentRead.helper,
      tone: sentimentRead.tone,
    },
    {
      id: "competitor_context",
      label: "Competitor Context",
      title: competitorContext.title,
      helper: competitorContext.helper,
      tone: competitorContext.tone,
    },
  ];
}

function buildOverviewKpis(dashboard: VisualDashboard): KpiCard[] {
  const reviewCount = getReviewCount(dashboard);
  const rating = dashboard.kpi_cards.find(
    (item) => item.id === "average_rating"
  );
  const sentimentData = dashboard.sentiment_distribution_chart?.data || [];
  const positive =
    sentimentData.find((item) => item.label.toLowerCase() === "positive")
      ?.value || 0;
  const negative =
    sentimentData.find((item) => item.label.toLowerCase() === "negative")
      ?.value || 0;

  return [
    {
      id: "overview_review_sample",
      label: "Review Sample",
      value: reviewCount,
      helper_text:
        reviewCount < 30
          ? "Small sample. Read as early signal."
          : "Usable sample for directional reads.",
      status: reviewCount < 30 ? "low_volume" : "usable",
    },
    {
      id: "overview_avg_rating",
      label: "Avg Rating",
      value: rating?.value ?? "—",
      helper_text: "Marketplace/user rating average.",
      status: null,
    },
    {
      id: "overview_positive_signals",
      label: "Positive Signals",
      value: positive,
      helper_text: "Positive aspect-level mentions found.",
      status: positive > negative ? "good" : null,
    },
    {
      id: "overview_negative_signals",
      label: "Negative Signals",
      value: negative,
      helper_text: "Negative aspect-level mentions found.",
      status: negative > positive ? "warn" : null,
    },
  ];
}

function getCleanReportBullets(report: ExecutiveReport | null) {
  if (!report) return [];
  const combined = [
    ...(report.executive_summary || []),
    ...(report.competitor_takeaways || []),
    ...(report.market_news_signals || []),
  ];
  return combined
    .filter((item) => !isDebugLikeBullet(item))
    .map(cleanThemeText)
    .slice(0, 6);
}

// ============================================================
// Root App component
// ============================================================

export default function App() {
  const hasBootstrappedRef = useRef(false);

  // Zustand — shared selection state
  const selectedProductId = useWorkspaceStore((s) => s.selectedProductId);
  const setSelectedProductId = useWorkspaceStore(
    (s) => s.setSelectedProductId
  );
  const selectedCompetitorId = useWorkspaceStore(
    (s) => s.selectedCompetitorId
  );
  const setSelectedCompetitorId = useWorkspaceStore(
    (s) => s.setSelectedCompetitorId
  );
  const timePeriod = useWorkspaceStore((s) => s.timePeriod);
  const setTimePeriod = useWorkspaceStore((s) => s.setTimePeriod);

  // Local state
  const [products, setProducts] = useState<ProductItem[]>([]);
  const [activeView, setActiveView] = useState<ViewKey>("overview");
  const [dashboard, setDashboard] = useState<VisualDashboard | null>(null);
  const [report, setReport] = useState<ExecutiveReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [productsLoading, setProductsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [importUrl, setImportUrl] = useState("");
  const [importPreview] = useState<ImportPreviewResponse | null>(null);
  const [importStatus, setImportStatus] = useState<
    "idle" | "previewing" | "importing" | "success" | "error"
  >("idle");
  const [evidenceOpen, setEvidenceOpen] = useState(false);
  const [reportOpen, setReportOpen] = useState(false);
  const [importOpen, setImportOpen] = useState(false);

  useEffect(() => {
    if (hasBootstrappedRef.current) return;
    hasBootstrappedRef.current = true;
    void bootstrapWorkspace();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function bootstrapWorkspace() {
    setProductsLoading(true);
    setLoading(true);
    setError(null);

    try {
      const items = await fetchProducts();
      setProducts(items);

      // Auto-select first own-brand product (itel / Infinix / Tecno)
      const defaultProduct =
        items.find(
          (item) => item.is_own_brand === true || item.own_brand === true
        ) ||
        items.find((item) => {
          const brand = (item.brand || "").toLowerCase();
          return ["tecno", "infinix", "itel"].includes(brand);
        }) ||
        items[0];

      const nextProductId = defaultProduct?.product_id ?? "";
      const nextCompetitorId = "";

      setSelectedProductId(nextProductId);
      setSelectedCompetitorId(nextCompetitorId);

      if (nextProductId) {
        const [dashboardResult, reportResult] = await fetchWorkspace(
          nextProductId,
          nextCompetitorId,
          "",
          ""
        );
        setDashboard(dashboardResult);
        setReport(reportResult);
      }
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to bootstrap workspace."
      );
    } finally {
      setProductsLoading(false);
      setLoading(false);
    }
  }

  async function loadProducts() {
    setProductsLoading(true);
    try {
      const items = await fetchProducts();
      setProducts(items);

      const productStillExists = items.some(
        (item) => item.product_id === selectedProductId
      );
      const competitorStillExists = items.some(
        (item) => item.product_id === selectedCompetitorId
      );

      if (!productStillExists && items[0]) {
        setSelectedProductId(items[0].product_id);
      }
      if (!competitorStillExists || selectedCompetitorId === selectedProductId) {
        setSelectedCompetitorId("");
      }
    } catch (err) {
      console.warn("Product load failed", err);
    } finally {
      setProductsLoading(false);
    }
  }

  async function loadWorkspace(
    nextProductId = selectedProductId,
    nextCompetitorId = selectedCompetitorId
  ) {
    setLoading(true);
    setError(null);
    try {
      const [dashboardResult, reportResult] = await fetchWorkspace(
        nextProductId,
        nextCompetitorId,
        timePeriod.startDate,
        timePeriod.endDate
      );
      setDashboard(dashboardResult);
      setReport(reportResult);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Unable to load workspace."
      );
    } finally {
      setLoading(false);
    }
  }

  function handlePreviewImport() {
    window.open(IRIP_DATA_WORKSPACE_URL, "_blank", "noopener,noreferrer");
  }

  async function handleFinalImport() {
    if (!importUrl.trim()) return;
    setImportStatus("importing");
    setError(null);
    try {
      await importReviewsFromCsvUrl(importUrl.trim());
      setImportStatus("success");
      setImportOpen(false);
      await loadProducts();
      await loadWorkspace();
    } catch (err) {
      setImportStatus("error");
      setError(err instanceof Error ? err.message : "Import failed.");
    }
  }

  function handleProductChange(productId: string) {
    setSelectedProductId(productId);
    if (selectedCompetitorId === productId) {
      setSelectedCompetitorId("");
    }
  }

  function handleCompetitorChange(competitorId: string) {
    if (!competitorId || competitorId === selectedProductId) {
      setSelectedCompetitorId("");
      return;
    }
    setSelectedCompetitorId(competitorId);
  }

  function downloadReport() {
    const payload = {
      dashboard,
      report,
      generated_at: new Date().toISOString(),
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `irip-report-${selectedProductId}.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  return (
    <QueryClientProvider client={queryClient}>
      <div className="irip-app-shell">
        <TopImportBar
          importUrl={importUrl}
          importStatus={importStatus}
          readinessStatus={dashboard?.readiness_status}
          onImportUrlChange={setImportUrl}
          onPreviewImport={handlePreviewImport}
          onDownloadReport={downloadReport}
          loading={loading}
        />

        <div className="irip-body-grid">
          <main className="irip-left-workspace">
            <section className="irip-workspace-header compact">
              <ViewChipSwitcher activeView={activeView} onChange={setActiveView} />
            </section>

            <section className="irip-main-tile">
              {loading && !dashboard ? (
                <LoadingTile />
              ) : error && !dashboard ? (
                <ErrorTile
                  message={error}
                  onRetry={() => void loadWorkspace()}
                />
              ) : dashboard ? (
                <MainVisualTile
                  activeView={activeView}
                  dashboard={dashboard}
                  report={report}
                  onOpenEvidence={() => setEvidenceOpen(true)}
                  onOpenReport={() => setReportOpen(true)}
                  productId={selectedProductId}
                  competitorId={selectedCompetitorId}
                  startDate={timePeriod.startDate}
                  endDate={timePeriod.endDate}
                  productName={
                    products.find((p) => p.product_id === selectedProductId)
                      ?.product_name ?? selectedProductId
                  }
                />
              ) : (
                <EmptyWorkspace onRetry={() => void loadWorkspace()} />
              )}
            </section>
          </main>

          <RightControlPanel
            products={products}
            productsLoading={productsLoading}
            selectedProductId={selectedProductId}
            selectedCompetitorId={selectedCompetitorId}
            startDate={timePeriod.startDate}
            endDate={timePeriod.endDate}
            dashboard={dashboard}
            loading={loading}
            onProductChange={handleProductChange}
            onCompetitorChange={handleCompetitorChange}
            onStartDateChange={(date) => setTimePeriod({ startDate: date })}
            onEndDateChange={(date) => setTimePeriod({ endDate: date })}
            onGenerate={() => void loadWorkspace()}
            onOpenEvidence={() => setEvidenceOpen(true)}
            onOpenReport={() => setReportOpen(true)}
          />
        </div>

        {error ? (
          <div className="irip-floating-error">
            <AlertTriangle size={15} />
            <span>{error}</span>
            <button type="button" onClick={() => setError(null)}>
              <X size={14} />
            </button>
          </div>
        ) : null}

        <EvidenceDrawer
          open={evidenceOpen}
          evidenceLinks={
            dashboard?.evidence_links || report?.evidence_links || []
          }
          onClose={() => setEvidenceOpen(false)}
        />

        <ReportModal
          open={reportOpen}
          report={report}
          onClose={() => setReportOpen(false)}
        />

        <ImportPreviewModal
          open={importOpen}
          preview={importPreview}
          importStatus={importStatus}
          onClose={() => setImportOpen(false)}
          onImport={handleFinalImport}
        />
      </div>
    </QueryClientProvider>
  );
}

// ============================================================
// Sub-components (layout unchanged)
// ============================================================

function TopImportBar({
  importUrl,
  importStatus,
  readinessStatus,
  loading,
  onImportUrlChange,
  onPreviewImport,
  onDownloadReport,
}: {
  importUrl: string;
  importStatus: string;
  readinessStatus?: string | null;
  loading: boolean;
  onImportUrlChange: (value: string) => void;
  onPreviewImport: () => void;
  onDownloadReport: () => void;
}) {
  return (
    <header className="top-import-bar">
      <div className="top-brand-section">
        <div className="brand-mark">
          <Sparkles size={18} />
        </div>
        <div className="brand-copy">
          <span>IRIP</span>
          <small>India Review Intelligence Platform</small>
        </div>
      </div>

      <div className="top-import-section">
        <div className="import-input-shell">
          <Search size={16} />
          <input
            id="irip-import-url"
            name="importUrl"
            value={importUrl}
            onChange={(event) => onImportUrlChange(event.target.value)}
            placeholder="Import CSV or Google Sheet link"
            autoComplete="off"
          />
        </div>

        <button
          className="top-action-button primary"
          type="button"
          onClick={onPreviewImport}
        >
          <UploadCloud size={15} />
          <span>
            {importStatus === "previewing"
              ? "Opening Workspace"
              : "Open Workspace"}
          </span>
        </button>

        <button
          className="top-action-button ghost"
          type="button"
          onClick={onDownloadReport}
        >
          <Download size={15} />
          <span>Download Report</span>
        </button>
      </div>

      <div className="top-status-section">
        <span className={`status-dot ${statusTone(readinessStatus)}`} />
        <span>
          {loading
            ? "Refreshing"
            : labelize(readinessStatus || "ready with limitations")}
        </span>
      </div>
    </header>
  );
}

function ViewChipSwitcher({
  activeView,
  onChange,
}: {
  activeView: ViewKey;
  onChange: (view: ViewKey) => void;
}) {
  return (
    <div className="view-chip-row">
      {views.map((view) => {
        const Icon = view.icon;
        const active = activeView === view.key;
        return (
          <button
            key={view.key}
            className={`view-chip ${active ? "active" : ""}`}
            type="button"
            onClick={() => onChange(view.key)}
          >
            <Icon size={15} />
            <span>{view.label}</span>
          </button>
        );
      })}
    </div>
  );
}

function MainVisualTile({
  activeView,
  dashboard,
  report,
  onOpenEvidence,
  onOpenReport,
  productId,
  competitorId,
  startDate,
  endDate,
  productName,
}: {
  activeView: ViewKey;
  dashboard: VisualDashboard;
  report: ExecutiveReport | null;
  onOpenEvidence: () => void;
  onOpenReport: () => void;
  productId: string;
  competitorId: string;
  startDate: string;
  endDate: string;
  productName: string;
}) {
  return (
    <div
      className={
        activeView === "report"
          ? "main-visual-content report-scroll-content"
          : "main-visual-content"
      }
    >
      {activeView === "overview" ? (
        <OverviewView
          dashboard={dashboard}
          report={report}
          onOpenEvidence={onOpenEvidence}
        />
      ) : activeView === "summary" ? (
        <SummaryView
          dashboard={dashboard}
          report={report}
          onOpenEvidence={onOpenEvidence}
          onOpenReport={onOpenReport}
        />
      ) : activeView === "sentiment" ? (
        <SentimentView
          productId={productId}
          startDate={startDate}
          endDate={endDate}
          dashboard={dashboard}
        />
      ) : activeView === "competitor" ? (
        <CompetitorView
          dashboard={dashboard}
          onOpenEvidence={onOpenEvidence}
        />
      ) : activeView === "news" ? (
        <NewsView dashboard={dashboard} />
      ) : activeView === "quality" ? (
        <QualityView dashboard={dashboard} report={report} />
      ) : (
        <ReportView
          productId={productId}
          competitorId={competitorId}
          startDate={startDate}
          endDate={endDate}
          productName={productName}
          dashboard={dashboard}
        />
      )}
    </div>
  );
}

function OverviewView({
  dashboard,
  report,
  onOpenEvidence,
}: {
  dashboard: VisualDashboard;
  report: ExecutiveReport | null;
  onOpenEvidence: () => void;
}) {
  const insightCards = buildUserInsightCards(dashboard, report);
  const overviewKpis = buildOverviewKpis(dashboard);

  return (
    <div className="overview-view">
      <div className="tile-section-header">
        <div>
          <p className="irip-eyebrow">Overview</p>
          <h2>What matters right now</h2>
        </div>
        <button className="micro-button" type="button" onClick={onOpenEvidence}>
          Evidence
          <ChevronRight size={14} />
        </button>
      </div>

      <div className="insight-card-grid">
        {insightCards.map((card) => (
          <article className={`insight-card ${card.tone}`} key={card.id}>
            <span>{card.label}</span>
            <strong>{card.title}</strong>
            <p>{card.helper}</p>
          </article>
        ))}
      </div>

      <KpiCardGrid cards={overviewKpis} />
    </div>
  );
}

function SummaryView({
  dashboard,
  report,
  onOpenEvidence,
  onOpenReport,
}: {
  dashboard: VisualDashboard;
  report: ExecutiveReport | null;
  onOpenEvidence: () => void;
  onOpenReport: () => void;
}) {
  const insightCards = buildUserInsightCards(dashboard, report);
  const topStrength =
    insightCards.find((item) => item.id === "customer_like")?.title ||
    "No clear strength yet.";
  const topRisk =
    insightCards.find((item) => item.id === "customer_complaint")?.title ||
    "No clear risk yet.";
  const confidence = buildEvidenceLevel(dashboard, report);
  const nextActions = dashboard.recommended_actions?.length
    ? dashboard.recommended_actions.map(cleanThemeText)
    : report?.recommended_actions.map(cleanThemeText) || [];

  return (
    <div className="summary-view">
      <div className="tile-section-header">
        <div>
          <p className="irip-eyebrow">Combined answer</p>
          <h2>Why this matters</h2>
        </div>
        <div className="button-pair">
          <button
            className="micro-button"
            type="button"
            onClick={onOpenEvidence}
          >
            Evidence
          </button>
          <button
            className="micro-button primary"
            type="button"
            onClick={onOpenReport}
          >
            Full Report
          </button>
        </div>
      </div>

      <div className="summary-view-grid">
        <section className="summary-panel confidence">
          <span>Confidence</span>
          <p>{confidence.text}</p>
          <small>{confidence.helper}</small>
        </section>

        <section className="summary-panel strength">
          <span>Strength</span>
          <p>{topStrength}</p>
        </section>

        <section className="summary-panel risk">
          <span>Risk</span>
          <p>{topRisk}</p>
        </section>
      </div>

      <div className="summary-action-list">
        <div className="summary-action-list-header">
          <span>Recommended actions</span>
          <small>{nextActions.length} action(s)</small>
        </div>

        {nextActions.slice(0, 6).map((action, index) => (
          <article
            className="summary-action-row"
            key={`${action}-${index}`}
          >
            <strong>{String(index + 1).padStart(2, "0")}</strong>
            <p>{action}</p>
          </article>
        ))}
      </div>
    </div>
  );
}

function findKpiCard(cards: KpiCard[], id: string) {
  return cards.find((card) => card.id === id);
}

function cleanShortSentimentLine(value: string) {
  return value
    .replace("signal(s)", "signals")
    .replace(" are currently balanced.", "")
    .replace(
      "Small sample. Treat aspect sentiment as early signal.",
      "Small sample. Read carefully."
    )
    .trim();
}

function volumePercent(value: number | string, rows: ChartDatum[]) {
  const max = Math.max(...rows.map((item) => Number(item.value || 0)), 1);
  return Math.max(8, Math.round((Number(value || 0) / max) * 100));
}

function sentimentCleanTone(label: string) {
  const value = label.toLowerCase();
  if (value.includes("positive")) return "positive";
  if (value.includes("negative")) return "negative";
  if (value.includes("mixed") || value.includes("polarized")) return "mixed";
  return "neutral";
}

function cleanReactionLabel(label: string) {
  const value = label.toLowerCase();
  if (value.includes("positive")) return "Positive";
  if (value.includes("negative")) return "Negative";
  if (value.includes("mixed") || value.includes("polarized")) return "Mixed";
  if (value.includes("neutral")) return "Neutral";
  return labelize(label);
}

function CompetitorView({
  dashboard,
  onOpenEvidence,
}: {
  dashboard: VisualDashboard;
  onOpenEvidence: () => void;
}) {
  const hasCompetitor = Boolean(dashboard.competitor_product_id);
  const gapRows = dashboard.competitor_gap_chart?.data || [];
  const specTable = dashboard.benchmark_spec_table;
  const specRows = specTable?.rows || [];
  const summary =
    dashboard.benchmark_summary || buildLocalBenchmarkSummary(gapRows);

  if (!hasCompetitor) {
    return (
      <div className="benchmark-final-view">
        <div className="tile-section-header benchmark-final-header">
          <div>
            <p className="irip-eyebrow">Benchmark</p>
            <h2>Select a competitor to compare products</h2>
          </div>
        </div>
        <section className="benchmark-final-empty">
          <h3>No competitor selected</h3>
          <p>
            Select a competitor from the control rail to unlock benchmark
            comparison.
          </p>
        </section>
      </div>
    );
  }

  return (
    <div className="benchmark-final-view">
      <div className="tile-section-header benchmark-final-header">
        <div>
          <p className="irip-eyebrow">Benchmark</p>
          <h2>Where the selected product leads or lags</h2>
        </div>
        <button
          className="micro-button"
          type="button"
          onClick={onOpenEvidence}
        >
          Evidence
          <ChevronRight size={14} />
        </button>
      </div>

      <section className="benchmark-final-chart-panel">
        <div className="benchmark-final-section-head">
          <div>
            <span>Review Gap Chart</span>
            <h3>
              Positive means selected product leads. Negative means competitor
              leads.
            </h3>
          </div>
        </div>
        <EChartCard chart={dashboard.competitor_gap_chart} variant="gap" compact />
      </section>

      <section className="benchmark-final-table-panel">
        <div className="benchmark-final-section-head">
          <div>
            <span>Comparison Table</span>
            <h3>Pure specification comparison between selected products</h3>
          </div>
          <small>{specRows.length} spec(s)</small>
        </div>

        <div className="benchmark-final-table spec-table">
          <div className="benchmark-final-table-head">
            <span>Spec</span>
            <span>{specTable?.selected_product_name || "Selected"}</span>
            <span>{specTable?.competitor_product_name || "Competitor"}</span>
          </div>

          {specRows.length ? (
            specRows.map((item) => (
              <article
                className="benchmark-final-table-row"
                key={`${item.category}-${item.field}`}
              >
                <strong>{item.field}</strong>
                <span>{formatSpecValue(item.selected_product_value)}</span>
                <span>{formatSpecValue(item.competitor_value)}</span>
              </article>
            ))
          ) : (
            <section className="benchmark-final-empty compact">
              <h3>No spec table yet</h3>
              <p>
                Generate the comparison again, or add product catalog/spec data
                for verified comparison.
              </p>
            </section>
          )}
        </div>

        {specTable?.confidence_note ? (
          <p className="benchmark-spec-note">{specTable.confidence_note}</p>
        ) : null}
      </section>

      <section className="benchmark-final-summary-panel">
        <div className="benchmark-final-section-head">
          <div>
            <span>Summary</span>
            <h3>{cleanBenchmarkSummaryText(summary.headline)}</h3>
          </div>
        </div>

        <div className="benchmark-final-summary-grid">
          <article>
            <span>Selected Product</span>
            <p>
              {cleanBenchmarkSummaryText(summary.selected_product_summary)}
            </p>
          </article>
          <article>
            <span>Competitor</span>
            <p>{cleanBenchmarkSummaryText(summary.competitor_summary)}</p>
          </article>
        </div>

        {summary.bullets?.length ? (
          <ul className="benchmark-final-bullets">
            {summary.bullets.slice(0, 3).map((item) => (
              <li key={item}>{cleanBenchmarkSummaryText(item)}</li>
            ))}
          </ul>
        ) : null}
      </section>
    </div>
  );
}

function buildBenchmarkSignal(item: CompetitorGapDatum) {
  const confidence = (item.confidence_label || "").toLowerCase();
  if (confidence.includes("insufficient") || confidence.includes("gap"))
    return { label: "Evidence Gap", tone: "warn" };
  if (Math.abs(item.gap) < 5) return { label: "Near Parity", tone: "neutral" };
  if (item.gap > 0) return { label: "Selected Leads", tone: "good" };
  return { label: "Competitor Leads", tone: "bad" };
}

function buildLocalBenchmarkSummary(
  rows: CompetitorGapDatum[]
): BenchmarkSummary {
  const selected = rows
    .filter(
      (item) =>
        item.gap > 0 &&
        !item.confidence_label.toLowerCase().includes("insufficient")
    )
    .map((item) => labelize(item.aspect));

  const competitor = rows
    .filter(
      (item) =>
        item.gap < 0 &&
        !item.confidence_label.toLowerCase().includes("insufficient")
    )
    .map((item) => labelize(item.aspect));

  const gaps = rows
    .filter((item) =>
      item.confidence_label.toLowerCase().includes("insufficient")
    )
    .map((item) => labelize(item.aspect));

  return {
    headline: "Review benchmark is available for selected aspects.",
    selected_product_summary: selected.length
      ? `Users appear to prefer the selected product for ${joinShort(selected)}.`
      : "No clear selected-product preference is visible yet.",
    competitor_summary: competitor.length
      ? `Users appear to prefer the competitor for ${joinShort(competitor)}.`
      : "No clear competitor preference is visible yet.",
    risk_summary: gaps.length
      ? `${joinShort(gaps)} have evidence gaps, so read those areas carefully.`
      : "Read this benchmark as directional until more reviews are available.",
    bullets: [],
    source: "rules",
  };
}

function joinShort(values: string[]) {
  if (!values.length) return "none";
  if (values.length === 1) return values[0];
  if (values.length === 2) return `${values[0]} and ${values[1]}`;
  return `${values.slice(0, 2).join(", ")}, and ${values.length - 2} more`;
}

function formatSpecValue(value?: string | null) {
  return (value || "").trim() || "Unknown";
}

function cleanBenchmarkSummaryText(value: string) {
  return value
    .replace(/\b\d+(?:\.\d+)?\s*points?\b/gi, "")
    .replace(/\bby\s+\d+(?:\.\d+)?\b/gi, "")
    .replace(/\b[+-]?\d+(?:\.\d+)?\s*gap\b/gi, "")
    .replace(/\s+/g, " ")
    .replace(" ,", ",")
    .replace(" .", ".")
    .trim();
}

function NewsView({ dashboard }: { dashboard: VisualDashboard }) {
  return (
    <div className="news-view">
      <div className="news-chart-zone">
        <EChartCard chart={dashboard.news_signal_chart} variant="bar" />
        <EChartCard
          chart={dashboard.source_tier_chart}
          variant="donut"
          compact
        />
      </div>
      <SignalChipGroup chips={dashboard.news_signal_chips} />
    </div>
  );
}

function QualityView({
  dashboard,
  report,
}: {
  dashboard: VisualDashboard;
  report: ExecutiveReport | null;
}) {
  const evidence = buildEvidenceLevel(dashboard, report);
  const trustCards = buildTrustCards(dashboard, report);

  return (
    <div className="quality-view">
      <div className="tile-section-header">
        <div>
          <p className="irip-eyebrow">Trust Layer</p>
          <h2>Can we trust this analysis?</h2>
        </div>
        <StatusPill status={evidence.label} />
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
    </div>
  );
}

// @ts-ignore — retained for reference; superseded by src/components/ReportView.tsx
function _LegacyReportView({
  report,
  dashboard,
  onOpenReport,
  onOpenEvidence,
}: {
  report: ExecutiveReport | null;
  dashboard: VisualDashboard;
  onOpenReport: () => void;
  onOpenEvidence: () => void;
}) {
  const reviewCount = getReviewCount(dashboard);
  const confidence = buildEvidenceLevel(dashboard, report);
  const overviewKpis = buildOverviewKpis(dashboard);
  const sentiment = buildSentimentRead(dashboard);
  const aspectCards = dashboard.aspect_reason_cards || [];
  const topAspects = dashboard.top_aspect_chart?.data || [];
  const benchmarkRows = dashboard.competitor_gap_chart?.data || [];
  const specRows = dashboard.benchmark_spec_table?.rows || [];
  const cleanBullets = getCleanReportBullets(report);
  const actions = dashboard.recommended_actions?.length
    ? dashboard.recommended_actions.map(cleanThemeText)
    : report?.recommended_actions.map(cleanThemeText) || [];
  const strengths = report?.key_strengths?.map(cleanThemeText) || [];
  const risks = report?.key_risks?.map(cleanThemeText) || [];
  const competitorTakeaways =
    report?.competitor_takeaways?.map(cleanThemeText) || [];

  return (
    <div className="report-view">
      <div className="tile-section-header">
        <div>
          <p className="irip-eyebrow">Executive report</p>
          <h2>Automated product intelligence brief</h2>
        </div>
        <div className="button-pair">
          <button
            className="micro-button"
            type="button"
            onClick={onOpenEvidence}
          >
            Evidence
          </button>
          <button
            className="micro-button primary"
            type="button"
            onClick={onOpenReport}
          >
            Full Report
          </button>
        </div>
      </div>

      <div className="report-preview-grid">
        <section className="report-preview-card wide">
          <span>Executive Snapshot</span>
          <p>
            {sentiment.title} This report is generated from {reviewCount} usable
            review(s), aspect signals, benchmark gaps, and evidence links.
          </p>
        </section>

        <section className="report-preview-card">
          <span>Confidence</span>
          <p>{confidence.label}</p>
          <small>{confidence.text}</small>
        </section>

        <section className="report-preview-card">
          <span>Review Base</span>
          <p>{reviewCount} usable reviews</p>
          <small>
            {reviewCount < 30 ? "Early signal" : "Directional evidence"}
          </small>
        </section>
      </div>

      <KpiCardGrid cards={overviewKpis} />

      <section className="report-decision-card">
        <span>Dataset interpretation</span>
        <p>
          {confidence.helper} Catalog-only products such as TECNO POVA Curve 2
          5G and itel Zeno 200 should be shown as coverage gaps unless reviews
          are imported for them.
        </p>
      </section>

      <div className="summary-view-grid">
        <section className="summary-panel strength">
          <span>Positive Signals</span>
          <p>{strengths[0] || "No strong positive signal available yet."}</p>
        </section>

        <section className="summary-panel risk">
          <span>Risk Signals</span>
          <p>{risks[0] || "No strong risk signal available yet."}</p>
        </section>

        <section className="summary-panel confidence">
          <span>Competitor Context</span>
          <p>
            {competitorTakeaways[0] ||
              "Select a competitor to strengthen benchmark interpretation."}
          </p>
        </section>
      </div>

      <section className="report-modal-section">
        <h3>Aspect Intelligence</h3>
        <div className="report-modal-list">
          {aspectCards.length ? (
            aspectCards.slice(0, 8).map((item: AspectReasonCard) => (
              <div
                className="report-modal-item"
                key={`${item.aspect}-${item.reaction}`}
              >
                <span />
                <p>
                  <strong>{labelize(item.aspect)}:</strong> {item.one_liner} (
                  {item.mention_count} mention(s), {item.confidence_label})
                </p>
              </div>
            ))
          ) : topAspects.length ? (
            topAspects.slice(0, 8).map((item) => (
              <div className="report-modal-item" key={item.label}>
                <span />
                <p>
                  <strong>{labelize(item.label)}:</strong> {item.value}{" "}
                  mention(s). {item.helper_text || ""}
                </p>
              </div>
            ))
          ) : (
            <div className="report-modal-item">
              <span />
              <p>No aspect-level evidence is available yet.</p>
            </div>
          )}
        </div>
      </section>

      <section className="report-modal-section">
        <h3>Competitor Benchmark</h3>
        <div className="report-modal-list">
          {benchmarkRows.length ? (
            benchmarkRows.slice(0, 8).map((item) => (
              <div
                className="report-modal-item"
                key={`${item.aspect}-${item.gap}`}
              >
                <span />
                <p>
                  <strong>{labelize(item.aspect)}:</strong>{" "}
                  {cleanBenchmarkSummaryText(item.interpretation)} (
                  {item.confidence_label})
                </p>
              </div>
            ))
          ) : (
            <div className="report-modal-item">
              <span />
              <p>
                Select a competitor to generate product-vs-competitor benchmark
                findings.
              </p>
            </div>
          )}
        </div>
      </section>

      <section className="report-modal-section">
        <h3>Spec Comparison Coverage</h3>
        <div className="report-modal-list">
          {specRows.length ? (
            specRows.slice(0, 8).map((item) => (
              <div
                className="report-modal-item"
                key={`${item.category}-${item.field}`}
              >
                <span />
                <p>
                  <strong>{item.field}:</strong>{" "}
                  {formatSpecValue(item.selected_product_value)} vs{" "}
                  {formatSpecValue(item.competitor_value)}.{" "}
                  {item.why_it_matters || ""}
                </p>
              </div>
            ))
          ) : (
            <div className="report-modal-item">
              <span />
              <p>
                No verified spec comparison table is available for this
                selection.
              </p>
            </div>
          )}
        </div>
      </section>

      <section className="report-modal-section">
        <h3>Recommended Actions</h3>
        <div className="report-modal-list">
          {actions.length ? (
            actions.slice(0, 8).map((action, index) => (
              <div
                className="report-modal-item"
                key={`${action}-${index}`}
              >
                <span />
                <p>{action}</p>
              </div>
            ))
          ) : (
            <div className="report-modal-item">
              <span />
              <p>
                Import more recent reviews and validate the strongest aspect
                signals.
              </p>
            </div>
          )}
        </div>
      </section>

      <section className="report-decision-card">
        <span>Gemini / external report workflow</span>
        <p>
          Use the Open Workspace button for the final review feed, cleaning
          reports, and Gemini report input. Gemini should be used for polished
          narrative only; the in-app report remains generated from backend data.
        </p>
      </section>

      {cleanBullets.length ? (
        <div className="report-bullets">
          {cleanBullets.map((item) => (
            <div className="report-bullet" key={item}>
              <span />
              <p>{item}</p>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function KpiCardGrid({ cards }: { cards: KpiCard[] }) {
  return (
    <div className="kpi-card-grid">
      {cards.map((card) => (
        <article className="kpi-card" key={card.id}>
          <div className="kpi-card-label-row">
            <span>{card.label}</span>
            {card.status ? (
              <StatusPill status={card.status} compact />
            ) : null}
          </div>
          <strong>{formatValue(card.value)}</strong>
          <p>{card.helper_text || "—"}</p>
        </article>
      ))}
    </div>
  );
}

function EChartCard({
  chart,
  variant,
  compact = false,
}: {
  chart: ChartBlock | CompetitorGapChart;
  variant: "donut" | "bar" | "gap";
  compact?: boolean;
}) {
  const [chartReady, setChartReady] = useState(false);

  useEffect(() => {
    let firstFrame = 0;
    let secondFrame = 0;
    setChartReady(false);
    firstFrame = window.requestAnimationFrame(() => {
      secondFrame = window.requestAnimationFrame(() => setChartReady(true));
    });
    return () => {
      window.cancelAnimationFrame(firstFrame);
      window.cancelAnimationFrame(secondFrame);
    };
  }, [chart.chart_id, chart.data.length, variant]);

  const option = useMemo(() => {
    if (variant === "donut") return buildDonutOption(chart as ChartBlock);
    if (variant === "gap")
      return buildGapOption(chart as CompetitorGapChart);
    return buildHorizontalBarOption(chart as ChartBlock);
  }, [chart, variant]);

  return (
    <section className={`echart-card ${compact ? "compact" : ""}`}>
      <div className="echart-card-header">
        <div>
          <p className="irip-eyebrow">
            {chart.chart_type.replace("echarts_", "")}
          </p>
          <h3>{chart.title}</h3>
        </div>
      </div>

      {chart.description ? (
        <p className="echart-description">{chart.description}</p>
      ) : null}

      <div className="echart-stage">
        {chart.data.length && chartReady ? (
          <ReactECharts
            option={option}
            notMerge
            lazyUpdate
            opts={{ renderer: "canvas" }}
            style={{ height: "100%", width: "100%", minHeight: 220 }}
            onChartReady={(instance) => {
              window.requestAnimationFrame(() => instance.resize());
            }}
          />
        ) : chart.data.length ? (
          <div className="chart-loading-shell">
            <Loader2 className="spin" size={18} />
            <span>Preparing chart</span>
          </div>
        ) : (
          <EmptyCard
            title="No chart data"
            text="Import or select a product to populate this visual."
          />
        )}
      </div>
    </section>
  );
}

function buildDonutOption(chart: ChartBlock) {
  return {
    tooltip: {
      trigger: "item",
      formatter: (params: EChartsItemParam) => {
        const datum = chart.data.find(
          (item) => labelize(item.label) === params.name
        );
        return `${params.marker}<strong>${params.name}</strong><br/>${params.value} signal(s)<br/>${
          datum?.helper_text || ""
        }`;
      },
    },
    legend: {
      bottom: 0,
      left: "center",
      itemWidth: 9,
      itemHeight: 9,
      textStyle: { color: "#6f6a7c", fontSize: 11 },
    },
    color: ["#6d5dfc", "#e65f6d", "#2aa876", "#f0b84e", "#7f8da3"],
    series: [
      {
        name: chart.title,
        type: "pie",
        radius: ["52%", "74%"],
        center: ["50%", "45%"],
        avoidLabelOverlap: true,
        label: { show: false },
        emphasis: { scale: true, scaleSize: 6 },
        data: chart.data.map((item) => ({
          name: labelize(item.label),
          value: item.value,
        })),
      },
    ],
  };
}

function buildHorizontalBarOption(chart: ChartBlock) {
  const data = [...chart.data].reverse();
  return {
    grid: { left: 12, right: 18, top: 8, bottom: 10, containLabel: true },
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      formatter: (params: EChartsAxisParam[]) => {
        const first = params[0];
        const datum = data[first.dataIndex];
        return `<strong>${first.name}</strong><br/>Value: ${first.value}<br/>${
          datum?.helper_text || ""
        }`;
      },
    },
    xAxis: {
      type: "value",
      axisLine: { show: false },
      splitLine: { lineStyle: { color: "rgba(94, 83, 120, 0.12)" } },
      axisLabel: { color: "#7d778c", fontSize: 11 },
    },
    yAxis: {
      type: "category",
      data: data.map((item) => labelize(item.label)),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#4c465c", fontSize: 12 },
    },
    series: [
      {
        type: "bar",
        barWidth: 12,
        data: data.map((item) => item.value),
        itemStyle: { borderRadius: [0, 8, 8, 0], color: "#6d5dfc" },
      },
    ],
  };
}

function buildGapOption(chart: CompetitorGapChart) {
  const data = [...chart.data].reverse();
  return {
    grid: { left: 18, right: 28, top: 12, bottom: 16, containLabel: true },
    tooltip: {
      trigger: "axis",
      confine: true,
      appendToBody: true,
      axisPointer: { type: "shadow" },
      formatter: (params: EChartsAxisParam[]) => {
        const first = params[0];
        const datum = data[first.dataIndex];
        const signal = buildBenchmarkSignal(datum);
        return `<strong>${labelize(datum.aspect)}</strong><br/>${signal.label}`;
      },
    },
    xAxis: {
      type: "value",
      min: -200,
      max: 200,
      axisLine: { show: false },
      splitLine: { lineStyle: { color: "rgba(94, 83, 120, 0.12)" } },
      axisLabel: { color: "#7d778c", fontSize: 11 },
    },
    yAxis: {
      type: "category",
      data: data.map((item) => labelize(item.aspect)),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: "#4c465c", fontSize: 12 },
    },
    series: [
      {
        type: "bar",
        barWidth: 14,
        data: data.map((item) => ({
          value: item.gap,
          itemStyle: {
            color: item.gap >= 0 ? "#2aaa76" : "#e65f6d",
            borderRadius: item.gap >= 0 ? [0, 8, 8, 0] : [8, 0, 0, 8],
          },
        })),
      },
    ],
  };
}

function SignalChipGroup({ chips }: { chips: SignalChip[] }) {
  return (
    <div className="signal-chip-zone">
      <div className="tile-section-header slim">
        <div>
          <p className="irip-eyebrow">Signals</p>
          <h3>Detected intelligence tags</h3>
        </div>
      </div>
      <div className="signal-chip-grid">
        {chips.map((chip) => (
          <span
            className={`signal-chip ${chip.signal_type}`}
            key={`${chip.signal_type}-${chip.label}`}
          >
            {labelize(chip.label)}
          </span>
        ))}
      </div>
    </div>
  );
}

function RightControlPanel({
  products,
  productsLoading,
  selectedProductId,
  selectedCompetitorId,
  startDate,
  endDate,
  dashboard,
  loading,
  onProductChange,
  onCompetitorChange,
  onStartDateChange,
  onEndDateChange,
  onGenerate,
  onOpenEvidence,
  onOpenReport,
}: {
  products: ProductItem[];
  productsLoading: boolean;
  selectedProductId: string;
  selectedCompetitorId: string;
  startDate: string;
  endDate: string;
  dashboard: VisualDashboard | null;
  loading: boolean;
  onProductChange: (value: string) => void;
  onCompetitorChange: (value: string) => void;
  onStartDateChange: (value: string) => void;
  onEndDateChange: (value: string) => void;
  onGenerate: () => void;
  onOpenEvidence: () => void;
  onOpenReport: () => void;
}) {
  return (
    <aside className="right-control-panel">
      <div className="panel-section panel-title-section">
        <div>
          <p className="irip-eyebrow">Control rail</p>
          <h2>Scope &amp; trust</h2>
        </div>
        <Filter size={18} />
      </div>

      <div className="panel-section">
        <p className="panel-section-title">Scope</p>

        <label className="control-field">
          <span>Product</span>
          <select
            id="irip-product-select"
            name="productId"
            value={selectedProductId}
            onChange={(event) => onProductChange(event.target.value)}
          >
            {productsLoading ? <option>Loading products…</option> : null}
            {products
              .filter((product) => {
                const brand = (product.brand || "").toLowerCase();
                return (
                  product.is_own_brand === true ||
                  product.own_brand === true ||
                  ["tecno", "infinix", "itel"].includes(brand)
                );
              })
              .map((product) => (
                <option key={product.product_id} value={product.product_id}>
                  {product.product_name || product.product_id}
                </option>
              ))}
          </select>
        </label>

        <label className="control-field">
          <span>Compare with optional</span>
          <select
            id="irip-competitor-select"
            name="competitorProductId"
            value={selectedCompetitorId}
            onChange={(event) => onCompetitorChange(event.target.value)}
          >
            <option value="">Select competitor</option>
            {products
              .filter((product) => product.product_id !== selectedProductId)
              .map((product) => (
                <option key={product.product_id} value={product.product_id}>
                  {product.product_name || product.product_id}
                </option>
              ))}
          </select>
        </label>

        <div className="date-grid">
          <label className="control-field">
            <span>Start</span>
            <input
              id="irip-start-date"
              name="startDate"
              value={startDate}
              onChange={(event) => onStartDateChange(event.target.value)}
              type="date"
            />
          </label>

          <label className="control-field">
            <span>End</span>
            <input
              id="irip-end-date"
              name="endDate"
              value={endDate}
              onChange={(event) => onEndDateChange(event.target.value)}
              type="date"
            />
          </label>
        </div>

        <button
          className="generate-button"
          type="button"
          onClick={onGenerate}
          disabled={loading}
        >
          {loading ? (
            <Loader2 className="spin" size={16} />
          ) : (
            <RefreshCw size={16} />
          )}
          <span>
            {loading
              ? "Generating"
              : selectedCompetitorId
                ? "Generate Comparison View"
                : "Generate Product View"}
          </span>
        </button>
      </div>

      <div className="panel-section trust-panel">
        <p className="panel-section-title">Trust</p>
        <StatusPill status={dashboard?.readiness_status || "waiting"} />

        {dashboard?.quality_cards?.slice(0, 2).map((card) => (
          <div className="trust-row" key={card.id}>
            <span>{card.label}</span>
            <strong>{labelize(String(card.value || "—"))}</strong>
            <small>{card.helper_text}</small>
          </div>
        ))}
      </div>

      <div className="panel-section panel-action-stack">
        <button
          className="side-action-button"
          type="button"
          onClick={onOpenEvidence}
        >
          <Database size={16} />
          <span>Open Evidence</span>
        </button>

        <button
          className="side-action-button primary"
          type="button"
          onClick={onOpenReport}
        >
          <FileText size={16} />
          <span>Executive Report</span>
        </button>
      </div>
    </aside>
  );
}

function EvidenceDrawer({
  open,
  evidenceLinks,
  onClose,
}: {
  open: boolean;
  evidenceLinks: EvidenceLink[];
  onClose: () => void;
}) {
  return (
    <div className={`drawer-backdrop ${open ? "open" : ""}`}>
      <aside className={`evidence-drawer ${open ? "open" : ""}`}>
        <div className="drawer-header">
          <div>
            <p className="irip-eyebrow">Traceability</p>
            <h2>Evidence Links</h2>
          </div>
          <button className="icon-button" type="button" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="drawer-body">
          {evidenceLinks.length ? (
            evidenceLinks.map((item, index) => (
              <article
                className="evidence-card"
                key={`${item.label}-${index}`}
              >
                <div className="evidence-card-top">
                  <span>{labelize(item.source_type)}</span>
                  <small>{item.source_name || "IRIP"}</small>
                </div>
                <h3>{item.label}</h3>
                {item.evidence_url ? (
                  <a href={item.evidence_url} target="_blank" rel="noreferrer">
                    Open source
                  </a>
                ) : (
                  <code>{item.reference_id || "No reference available"}</code>
                )}
              </article>
            ))
          ) : (
            <EmptyCard
              title="No evidence links"
              text="Evidence will appear after analysis runs."
            />
          )}
        </div>
      </aside>
    </div>
  );
}

function ReportModal({
  open,
  report,
  onClose,
}: {
  open: boolean;
  report: ExecutiveReport | null;
  onClose: () => void;
}) {
  return (
    <div className={`modal-backdrop ${open ? "open" : ""}`}>
      <section className={`report-modal ${open ? "open" : ""}`}>
        <div className="drawer-header">
          <div>
            <p className="irip-eyebrow">Executive output</p>
            <h2>{report?.report_title || "Executive Report"}</h2>
          </div>
          <button className="icon-button" type="button" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="report-modal-body">
          {report ? (
            <>
              <section className="report-modal-section confidence">
                <h3>Confidence note</h3>
                <p>{report.confidence_note}</p>
              </section>

              <ReportSection
                title="Executive Summary"
                items={report.executive_summary.map(cleanThemeText)}
              />
              <ReportSection
                title="Key Strengths"
                items={report.key_strengths.map(cleanThemeText)}
              />
              <ReportSection
                title="Key Risks"
                items={report.key_risks.map(cleanThemeText)}
              />
              <ReportSection
                title="Competitor Takeaways"
                items={report.competitor_takeaways.map(cleanThemeText)}
              />
              <ReportSection
                title="Market News Signals"
                items={report.market_news_signals.map(cleanThemeText)}
              />
              <ReportSection
                title="Recommended Actions"
                items={report.recommended_actions.map(cleanThemeText)}
              />
            </>
          ) : (
            <EmptyCard
              title="Report not loaded"
              text="Generate the workspace first."
            />
          )}
        </div>
      </section>
    </div>
  );
}

function ReportSection({ title, items }: { title: string; items: string[] }) {
  return (
    <section className="report-modal-section">
      <h3>{title}</h3>
      <div className="report-modal-list">
        {items.map((item) => (
          <div className="report-modal-item" key={item}>
            <span />
            <p>{item}</p>
          </div>
        ))}
      </div>
    </section>
  );
}

function ImportPreviewModal({
  open,
  preview,
  importStatus,
  onClose,
  onImport,
}: {
  open: boolean;
  preview: ImportPreviewResponse | null;
  importStatus: string;
  onClose: () => void;
  onImport: () => void;
}) {
  return (
    <div className={`modal-backdrop ${open ? "open" : ""}`}>
      <section className={`import-modal ${open ? "open" : ""}`}>
        <div className="drawer-header">
          <div>
            <p className="irip-eyebrow">Safe import</p>
            <h2>Import Preview</h2>
          </div>
          <button className="icon-button" type="button" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="import-preview-body">
          {preview ? (
            <>
              <div className="import-stats-grid">
                <KpiMini label="Valid" value={preview.valid_count} tone="good" />
                <KpiMini
                  label="Failed"
                  value={preview.failed_count}
                  tone="bad"
                />
                <KpiMini
                  label="Warnings"
                  value={preview.warning_count}
                  tone="warn"
                />
              </div>

              <section className="preview-list-section">
                <h3>Detected columns</h3>
                <div className="column-chip-row">
                  {preview.detected_columns.map((column) => (
                    <span key={column}>{column}</span>
                  ))}
                </div>
              </section>

              {preview.errors.length ? (
                <section className="preview-list-section error">
                  <h3>Errors</h3>
                  {preview.errors.slice(0, 5).map((item) => (
                    <p key={`${item.row_number}-${item.reason}`}>
                      Row {item.row_number}: {item.reason}
                    </p>
                  ))}
                </section>
              ) : null}

              {preview.warnings.length ? (
                <section className="preview-list-section warning">
                  <h3>Warnings</h3>
                  {preview.warnings.slice(0, 5).map((item) => (
                    <p key={`${item.row_number}-${item.reason}`}>
                      Row {item.row_number}: {item.reason}
                    </p>
                  ))}
                </section>
              ) : null}

              <div className="modal-footer">
                <button
                  className="side-action-button"
                  type="button"
                  onClick={onClose}
                >
                  Cancel
                </button>
                <button
                  className="side-action-button primary"
                  type="button"
                  onClick={onImport}
                  disabled={
                    importStatus === "importing" || preview.valid_count === 0
                  }
                >
                  {importStatus === "importing"
                    ? "Importing…"
                    : "Import Valid Rows"}
                </button>
              </div>
            </>
          ) : (
            <EmptyCard
              title="No preview"
              text="Paste a CSV URL and preview first."
            />
          )}
        </div>
      </section>
    </div>
  );
}

function KpiMini({
  label,
  value,
  tone,
}: {
  label: string;
  value: number;
  tone: "good" | "bad" | "warn";
}) {
  return (
    <article className={`kpi-mini ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function StatusPill({
  status,
  compact = false,
}: {
  status?: string | null;
  compact?: boolean;
}) {
  const tone = statusTone(status);
  return (
    <span className={`status-pill ${tone} ${compact ? "compact" : ""}`}>
      {labelize(status || "waiting")}
    </span>
  );
}

function LoadingTile() {
  return (
    <div className="loading-tile">
      <Loader2 className="spin" size={28} />
      <h2>Building intelligence view</h2>
      <p>
        Loading dashboard visuals, report sections, confidence notes, and
        evidence links.
      </p>
    </div>
  );
}

function ErrorTile({
  message,
  onRetry,
}: {
  message: string;
  onRetry: () => void;
}) {
  return (
    <div className="loading-tile error">
      <AlertTriangle size={28} />
      <h2>Workspace failed to load</h2>
      <p>{message}</p>
      <button
        className="generate-button small"
        type="button"
        onClick={onRetry}
      >
        Retry
      </button>
    </div>
  );
}

function EmptyWorkspace({ onRetry }: { onRetry: () => void }) {
  return (
    <div className="loading-tile">
      <Database size={28} />
      <h2>No workspace loaded</h2>
      <p>Select product scope and generate the intelligence view.</p>
      <button
        className="generate-button small"
        type="button"
        onClick={onRetry}
      >
        Generate View
      </button>
    </div>
  );
}

function EmptyCard({ title, text }: { title: string; text: string }) {
  return (
    <div className="empty-card">
      <h3>{title}</h3>
      <p>{text}</p>
    </div>
  );
}

// Unused in current UI but kept for future BenchmarkSpecTable display
type _BenchmarkSpecTableRef = BenchmarkSpecTable;
void (undefined as unknown as _BenchmarkSpecTableRef);
