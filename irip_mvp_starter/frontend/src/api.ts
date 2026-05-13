// ============================================================
// IRIP API Client
// Base: /api (Vite dev proxy → localhost:8000, Vercel → irip-api.onrender.com)
// ============================================================

const REQUEST_TIMEOUT_MS = 10_000;

/** Set once after login to attach a Bearer token to every request. */
let _authToken: string | null = null;
export function setAuthToken(token: string | null): void {
  _authToken = token;
}

// ============================================================
// Types — shared primitives
// ============================================================

export type PeriodFilter = {
  start_date?: string;
  end_date?: string;
};

// ============================================================
// Types — products
// ============================================================

export type ProductItem = {
  product_id: string;
  product_name?: string | null;
  review_count?: number;
  first_review_date?: string | null;
  latest_review_date?: string | null;
  brand?: string | null;
  /** Legacy field from earlier API versions. */
  own_brand?: boolean | null;
  /** Canonical field — true for itel / Infinix / Tecno products. */
  is_own_brand?: boolean | null;
};

export type ProductSummary = {
  product_id: string;
  period: { start_date: string | null; end_date: string | null };
  review_count: number;
  average_rating: number | null;
  average_quality_score: number | null;
  net_sentiment_score: number;
  sentiment_counts: Record<string, number>;
  contradiction_count: number;
  sarcasm_count: number;
  top_aspects: Array<{ aspect: string; mentions: number }>;
};

export type ThemeEvidenceItem = {
  review_id: string;
  source: string | null;
  rating: number | null;
  review_date: string | null;
  raw_text: string;
  evidence_span: string | null;
  confidence: number | null;
  provider: string | null;
};

export type ThemeItem = {
  theme_id: string;
  theme_name: string;
  aspect: string;
  theme_type: string;
  sentiment: string;
  mention_count: number;
  avg_intensity: number;
  avg_confidence: number;
  severity_score: number;
  actionability: string;
  interpretation: string;
  evidence: ThemeEvidenceItem[];
};

export type ProductThemesResponse = {
  product_id: string;
  period: { start_date: string | null; end_date: string | null };
  complaint_themes: ThemeItem[];
  delight_themes: ThemeItem[];
  watchlist_themes: ThemeItem[];
};

export type ForecastAspectItem = {
  aspect: string;
  current_score: number;
  previous_score: number | null;
  movement: number | null;
  direction: string;
  current_mentions: number;
  previous_mentions: number;
  confidence_label: string;
  explanation: string;
};

export type ProductForecastResponse = {
  product_id: string;
  forecast_basis: string;
  forecast_window: string;
  overall_direction: string;
  confidence_label: string;
  aspects: ForecastAspectItem[];
  caveats: string[];
};

export type IntelligenceBriefResponse = {
  product_id: string;
  period: { start_date: string | null; end_date: string | null };
  executive_summary: string;
  top_strengths: string[];
  top_risks: string[];
  recommended_actions: string[];
  evidence_note: string;
  confidence_note: string;
};

export type CompetitorItem = {
  product_id: string;
  product_name: string | null;
  brand: string | null;
  price_band: string | null;
  comparison_group: string | null;
  notes: string | null;
};

export type BenchmarkAspectItem = {
  aspect: string;
  own_score: number;
  competitor_score: number;
  gap: number;
  own_mentions: number;
  competitor_mentions: number;
  own_confidence: number | null;
  competitor_confidence: number | null;
  confidence_label: string;
  interpretation: string;
};

export type CompetitorBenchmark = {
  product_id: string;
  competitor_product_id: string;
  period: { start_date: string | null; end_date: string | null };
  own_review_count: number;
  competitor_review_count: number;
  benchmark_aspects: BenchmarkAspectItem[];
  top_strengths: BenchmarkAspectItem[];
  top_weaknesses: BenchmarkAspectItem[];
};

export type EvidenceItem = {
  review_id: string;
  product_id: string;
  product_name: string | null;
  source: string | null;
  rating: number | null;
  review_date: string | null;
  raw_text: string;
  clean_text: string;
  quality_score: number | null;
  aspect: string;
  sentiment: string;
  intensity: number;
  confidence: number;
  evidence_span: string | null;
  provider: string | null;
  language_type?: string | null;
};

export type AspectSummaryItem = {
  aspect: string;
  mentions: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
  avg_confidence: number | null;
  aspect_score: number;
  sub_aspects?: Record<string, number> | null;
};

// ============================================================
// Types — visual dashboard
// ============================================================

export type WorkflowTile = {
  id: string;
  title: string;
  status: string;
  primary_text: string;
  secondary_text: string;
};

export type KpiCard = {
  id: string;
  label: string;
  value: string | number | null;
  helper_text?: string | null;
  status?: string | null;
};

export type ChartDatum = {
  label: string;
  value: number;
  secondary_value?: number | null;
  category?: string | null;
  status?: string | null;
  helper_text?: string | null;
};

export type ChartBlock = {
  chart_id: string;
  chart_type: string;
  title: string;
  description?: string | null;
  data: ChartDatum[];
  encoding?: Record<string, string | null>;
  recommended_echarts?: {
    series_type: string;
    orientation?: string | null;
    interactive?: string[];
    suggested_chart?: string;
  };
};

export type AspectSentimentDatum = {
  aspect: string;
  mentions: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
  aspect_score: number;
  avg_confidence?: number | null;
  sentiment_label: string;
  priority_bucket: string;
  interpretation: string;
};

export type AspectSentimentChart = {
  chart_id: string;
  chart_type: string;
  title: string;
  description?: string | null;
  data: AspectSentimentDatum[];
  encoding?: Record<string, string | null>;
  recommended_echarts?: {
    series_type: string;
    orientation?: string | null;
    interactive?: string[];
    suggested_chart?: string;
  };
};

export type SentimentPriorityDatum = {
  aspect: string;
  mentions: number;
  aspect_score: number;
  sentiment_label: string;
  priority_bucket: string;
  priority_level: string;
  interpretation: string;
};

export type SentimentPriorityMatrix = {
  matrix_id: string;
  title: string;
  description?: string | null;
  data: SentimentPriorityDatum[];
};

export type AspectReasonCard = {
  aspect: string;
  reaction: string;
  mention_count: number;
  positive_count: number;
  negative_count: number;
  neutral_count: number;
  one_liner: string;
  evidence_terms: string[];
  evidence_examples: string[];
  confidence_label: string;
  llm_generated?: boolean;
  reason_source?: string | null;
};

export type CompetitorGapDatum = {
  aspect: string;
  gap: number;
  own_score: number;
  competitor_score: number;
  confidence_label: string;
  interpretation: string;
};

export type CompetitorGapChart = {
  chart_id: string;
  chart_type: string;
  title: string;
  description?: string | null;
  data: CompetitorGapDatum[];
  encoding?: Record<string, string | null>;
  recommended_echarts?: {
    series_type: string;
    orientation?: string | null;
    interactive?: string[];
    suggested_chart?: string;
  };
};

export type SignalChip = {
  label: string;
  signal_type: string;
  weight?: number | null;
};

export type EvidenceLink = {
  label: string;
  source_type: string;
  source_name?: string | null;
  evidence_url?: string | null;
  reference_id?: string | null;
};

export type BenchmarkSummary = {
  headline: string;
  selected_product_summary: string;
  competitor_summary: string;
  risk_summary: string;
  bullets: string[];
  source?: string | null;
};

export type BenchmarkSpecRow = {
  category: string;
  field: string;
  selected_product_value: string;
  competitor_value: string;
  winner: "selected_product" | "competitor" | "tie" | "unknown" | "not_applicable" | string;
  confidence: "verified" | "likely" | "unknown" | string;
  source_status: "model_knowledge" | "needs_source" | "unknown" | string;
  why_it_matters?: string | null;
};

export type BenchmarkSpecTable = {
  selected_product_name: string;
  competitor_product_name: string;
  source?: string | null;
  confidence_note?: string | null;
  rows: BenchmarkSpecRow[];
  unknown_fields?: string[];
};

export type VisualDashboard = {
  product_id: string;
  competitor_product_id?: string | null;
  readiness_status: string;
  workflow_tiles: WorkflowTile[];
  kpi_cards: KpiCard[];
  sentiment_distribution_chart: ChartBlock;
  top_aspect_chart: ChartBlock;
  aspect_sentiment_chart?: AspectSentimentChart | null;
  sentiment_priority_matrix?: SentimentPriorityMatrix | null;
  sentiment_insight_cards?: KpiCard[];
  aspect_reason_cards?: AspectReasonCard[];
  competitor_gap_chart: CompetitorGapChart;
  news_signal_chart: ChartBlock;
  source_tier_chart: ChartBlock;
  quality_cards: KpiCard[];
  news_signal_chips: SignalChip[];
  recommended_actions: string[];
  evidence_links: EvidenceLink[];
  benchmark_summary?: BenchmarkSummary | null;
  benchmark_spec_table?: BenchmarkSpecTable | null;
};

// ============================================================
// Types — market intelligence
// ============================================================

export type MarketPulseItem = {
  headline: string;
  summary: string;
  relevance: "high" | "medium";
  category: "launch" | "trend" | "competitor" | "consumer";
};

export type UpcomingLaunchItem = {
  brand: string;
  model: string;
  estimated_date: string;
  expected_price_inr: string;
  key_feature: string;
};

export type CompetitorWatchItem = {
  brand: string;
  recent_move: string;
  threat_level: "high" | "medium" | "low";
  our_response: string;
};

export type MarketIntelligence = {
  market_pulse: MarketPulseItem[];
  upcoming_launches: UpcomingLaunchItem[];
  competitor_watch: CompetitorWatchItem[];
  segment_trend: string;
  consumer_shift: string;
  cached_at: string;
  cache_expires_at: string;
  error: boolean;
  error_message: string | null;
};

export type DedupStats = {
  total_seen: number;
  scraped_today: number;
};

// ============================================================
// Types — executive report
// ============================================================

export type ExecutiveReport = {
  report_title: string;
  product_id: string;
  competitor_product_id?: string | null;
  period: Record<string, string | null>;
  confidence_note: string;
  executive_summary: string[];
  key_strengths: string[];
  key_risks: string[];
  competitor_takeaways: string[];
  market_news_signals: string[];
  recommended_actions: string[];
  sections: { title: string; bullets: string[] }[];
  evidence_links: EvidenceLink[];
  executive_narrative?: string | null;
};

// ============================================================
// Types — imports + LLM + feedback
// ============================================================

export type ImportResult = {
  imported_count: number;
  failed_count: number;
  errors: Array<{ row_number: number; reason: string }>;
  product_ids: string[];
};

export type ImportPreviewResponse = {
  valid_count: number;
  failed_count: number;
  warning_count: number;
  required_columns_present: boolean;
  detected_columns: string[];
  errors: { row_number: number; reason: string; value?: string | null }[];
  warnings: { row_number: number; reason: string; value?: string | null }[];
  sample_valid_rows: Record<string, string>[];
};

export type LlmProviderStatus = {
  provider: string;
  enabled: boolean;
  model: string | null;
  mode: string;
  reason: string | null;
};

export type LlmModeUpdateResponse = {
  mode: string;
  message: string;
};

export type ExtractionFeedbackCreate = {
  review_id: string;
  product_id: string;
  aspect: string;
  predicted_sentiment: string;
  provider: string | null;
  is_correct: boolean;
  corrected_aspect?: string | null;
  corrected_sentiment?: string | null;
  note?: string | null;
};

export type ExtractionFeedbackItem = ExtractionFeedbackCreate & {
  id: number;
  created_at: string;
};

export type ProviderQualityItem = {
  provider: string;
  total_feedback: number;
  correct_count: number;
  incorrect_count: number;
  accuracy: number;
};

// ============================================================
// Internal helpers
// ============================================================

function buildQuery(params: Record<string, string | number | undefined | null>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      search.set(key, String(value));
    }
  }
  const qs = search.toString();
  return qs ? `?${qs}` : "";
}

function buildHeaders(extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = extra ? { ...extra } : {};
  if (_authToken) {
    headers["Authorization"] = `Bearer ${_authToken}`;
  }
  return headers;
}

async function fetchWithTimeout(url: string, options: RequestInit = {}): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function extractErrorMessage(response: Response): Promise<string> {
  const text = await response.text();
  try {
    const json = JSON.parse(text) as { detail?: unknown; message?: unknown; error?: unknown };
    const msg = json.detail ?? json.message ?? json.error;
    if (typeof msg === "string") return msg;
    if (msg !== undefined) return JSON.stringify(msg);
  } catch {
    // use raw text
  }
  return text || `HTTP ${response.status}`;
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetchWithTimeout(`/api${path}`, {
    headers: buildHeaders(),
  });
  if (!response.ok) {
    throw new Error(await extractErrorMessage(response));
  }
  return response.json() as Promise<T>;
}

async function postJson<T>(path: string, payload: unknown): Promise<T> {
  const response = await fetchWithTimeout(`/api${path}`, {
    method: "POST",
    headers: buildHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(await extractErrorMessage(response));
  }
  return response.json() as Promise<T>;
}

// ============================================================
// API — products
// ============================================================

export async function fetchProducts(): Promise<ProductItem[]> {
  return getJson<ProductItem[]>("/products");
}

export async function fetchProductSummary(
  productId: string,
  period?: PeriodFilter
): Promise<ProductSummary> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });
  return getJson<ProductSummary>(
    `/products/${encodeURIComponent(productId)}/summary${query}`
  );
}

export async function fetchProductThemes(
  productId: string,
  period?: PeriodFilter
): Promise<ProductThemesResponse> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });
  return getJson<ProductThemesResponse>(
    `/products/${encodeURIComponent(productId)}/themes${query}`
  );
}

export async function fetchProductForecast(
  productId: string,
  period?: PeriodFilter
): Promise<ProductForecastResponse> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });
  return getJson<ProductForecastResponse>(
    `/products/${encodeURIComponent(productId)}/forecast${query}`
  );
}

export async function fetchIntelligenceBrief(
  productId: string,
  period?: PeriodFilter
): Promise<IntelligenceBriefResponse> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });
  return getJson<IntelligenceBriefResponse>(
    `/products/${encodeURIComponent(productId)}/intelligence-brief${query}`
  );
}

export async function fetchProductCompetitors(
  productId: string
): Promise<CompetitorItem[]> {
  return getJson<CompetitorItem[]>(
    `/products/${encodeURIComponent(productId)}/competitors`
  );
}

export async function fetchCompetitorBenchmark(
  productId: string,
  competitorProductId: string,
  period?: PeriodFilter
): Promise<CompetitorBenchmark> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });
  return getJson<CompetitorBenchmark>(
    `/products/${encodeURIComponent(productId)}/benchmark/${encodeURIComponent(
      competitorProductId
    )}${query}`
  );
}

export async function fetchProductEvidence(
  productId: string,
  filters?: {
    aspect?: string;
    sentiment?: string;
    limit?: number;
    start_date?: string;
    end_date?: string;
  }
): Promise<EvidenceItem[]> {
  const query = buildQuery({
    aspect: filters?.aspect,
    sentiment: filters?.sentiment,
    limit: filters?.limit,
    start_date: filters?.start_date,
    end_date: filters?.end_date,
  });
  return getJson<EvidenceItem[]>(
    `/products/${encodeURIComponent(productId)}/evidence${query}`
  );
}

export async function fetchProductAspects(
  productId: string,
  period?: PeriodFilter
): Promise<AspectSummaryItem[]> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });
  return getJson<AspectSummaryItem[]>(
    `/products/${encodeURIComponent(productId)}/aspects${query}`
  );
}

export async function fetchMarketIntelligence(): Promise<MarketIntelligence> {
  return getJson<MarketIntelligence>("/market/intelligence");
}

export async function fetchDedupStats(): Promise<DedupStats> {
  return getJson<DedupStats>("/trust/dedup-stats");
}

export type HealthResponse = { status: string };
export async function fetchHealth(): Promise<HealthResponse> {
  return getJson<HealthResponse>("/health");
}

// ============================================================
// API — visual dashboard + executive report
// ============================================================

export type WorkspaceParams = {
  product_id: string;
  competitor_product_id?: string;
  start_date?: string;
  end_date?: string;
};

export async function fetchVisualDashboard(
  params: WorkspaceParams
): Promise<VisualDashboard> {
  const query = buildQuery({
    product_id: params.product_id,
    competitor_product_id: params.competitor_product_id,
    start_date: params.start_date,
    end_date: params.end_date,
  });
  return getJson<VisualDashboard>(`/visuals/dashboard${query}`);
}

export async function fetchExecutiveReport(
  params: WorkspaceParams
): Promise<ExecutiveReport> {
  const query = buildQuery({
    product_id: params.product_id,
    competitor_product_id: params.competitor_product_id,
    start_date: params.start_date,
    end_date: params.end_date,
  });
  return getJson<ExecutiveReport>(`/reports/executive${query}`);
}

export async function fetchReportWithNarrative(
  params: WorkspaceParams
): Promise<ExecutiveReport> {
  const query = buildQuery({
    product_id: params.product_id,
    competitor_product_id: params.competitor_product_id,
    start_date: params.start_date,
    end_date: params.end_date,
    include_narrative: "true",
  });
  return getJson<ExecutiveReport>(`/reports/executive${query}`);
}

// ============================================================
// API — review import
// ============================================================

export async function importReviewsFromCsvUrl(url: string): Promise<ImportResult> {
  return postJson<ImportResult>("/reviews/import-csv-url", { url });
}

// ============================================================
// API — LLM
// ============================================================

export async function fetchLlmStatus(): Promise<LlmProviderStatus> {
  return getJson<LlmProviderStatus>("/llm/status");
}

export async function updateLlmMode(mode: string): Promise<LlmModeUpdateResponse> {
  return postJson<LlmModeUpdateResponse>("/llm/mode", { mode });
}

// ============================================================
// API — feedback
// ============================================================

export async function submitExtractionFeedback(
  payload: ExtractionFeedbackCreate
): Promise<ExtractionFeedbackItem> {
  return postJson<ExtractionFeedbackItem>("/feedback/extraction", payload);
}

export async function fetchProviderQuality(): Promise<ProviderQualityItem[]> {
  return getJson<ProviderQualityItem[]>("/feedback/provider-quality");
}

export async function fetchExtractionFeedback(filters?: {
  product_id?: string;
  provider?: string;
  limit?: number;
}): Promise<ExtractionFeedbackItem[]> {
  const query = buildQuery({
    product_id: filters?.product_id,
    provider: filters?.provider,
    limit: filters?.limit,
  });
  return getJson<ExtractionFeedbackItem[]>(`/feedback/extraction${query}`);
}
