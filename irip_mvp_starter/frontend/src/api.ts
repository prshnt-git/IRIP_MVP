export type PeriodFilter = {
  start_date?: string;
  end_date?: string;
};

export type ProductItem = {
  product_id: string;
  product_name: string | null;
  review_count: number;
  first_review_date: string | null;
  latest_review_date: string | null;
};

export type ProductSummary = {
  product_id: string;
  period: {
    start_date: string | null;
    end_date: string | null;
  };
  review_count: number;
  average_rating: number | null;
  average_quality_score: number | null;
  net_sentiment_score: number;
  sentiment_counts: Record<string, number>;
  contradiction_count: number;
  sarcasm_count: number;
  top_aspects: Array<{
    aspect: string;
    mentions: number;
  }>;
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
  period: {
    start_date: string | null;
    end_date: string | null;
  };
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
  period: {
    start_date: string | null;
    end_date: string | null;
  };
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
  period: {
    start_date: string | null;
    end_date: string | null;
  };
  own_review_count: number;
  competitor_review_count: number;
  benchmark_aspects: BenchmarkAspectItem[];
  top_strengths: BenchmarkAspectItem[];
  top_weaknesses: BenchmarkAspectItem[];
};

export type ImportResult = {
  imported_count: number;
  failed_count: number;
  errors: Array<{
    row_number: number;
    reason: string;
  }>;
  product_ids: string[];
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

function buildQuery(params: Record<string, string | number | undefined>): string {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== "") {
      searchParams.set(key, String(value));
    }
  });

  const query = searchParams.toString();
  return query ? `?${query}` : "";
}

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`/api${path}`);

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API error ${response.status}: ${text}`);
  }

  return response.json() as Promise<T>;
}

async function postJson<T>(path: string, payload: unknown): Promise<T> {
  const response = await fetch(`/api${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`API error ${response.status}: ${text}`);
  }

  return response.json() as Promise<T>;
}

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

  return getJson<ProductSummary>(`/products/${encodeURIComponent(productId)}/summary${query}`);
}

export async function fetchProductThemes(
  productId: string,
  period?: PeriodFilter
): Promise<ProductThemesResponse> {
  const query = buildQuery({
    start_date: period?.start_date,
    end_date: period?.end_date,
  });

  return getJson<ProductThemesResponse>(`/products/${encodeURIComponent(productId)}/themes${query}`);
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

export async function fetchProductCompetitors(productId: string): Promise<CompetitorItem[]> {
  return getJson<CompetitorItem[]>(`/products/${encodeURIComponent(productId)}/competitors`);
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

  return getJson<EvidenceItem[]>(`/products/${encodeURIComponent(productId)}/evidence${query}`);
}

export async function importReviewsFromCsvUrl(url: string): Promise<ImportResult> {
  return postJson<ImportResult>("/reviews/import-csv-url", { url });
}

export async function fetchLlmStatus(): Promise<LlmProviderStatus> {
  return getJson<LlmProviderStatus>("/llm/status");
}

export async function updateLlmMode(mode: string): Promise<LlmModeUpdateResponse> {
  return postJson<LlmModeUpdateResponse>("/llm/mode", { mode });
}

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