import { useState, useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import ReactECharts from "echarts-for-react";
import {
  Battery,
  Camera,
  Monitor,
  Zap,
  Tag,
  Headphones,
  Wifi,
  Package,
  Code,
  Star,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import type { VisualDashboard, AspectSummaryItem, EvidenceItem } from "../api";
import { fetchProductAspects, fetchProductEvidence } from "../api";

// ─── Animated counter ─────────────────────────────────────────────────────────

function AnimatedNumber({ value, duration = 1200 }: { value: number; duration?: number }) {
  const [display, setDisplay] = useState(0);
  const prev = useRef(0);
  const rafRef = useRef<number>(0);

  useEffect(() => {
    const start = prev.current;
    const delta = value - start;
    const startTime = performance.now();

    const frame = (now: number) => {
      const elapsed = now - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setDisplay(Math.round(start + delta * eased));
      if (progress < 1) {
        rafRef.current = requestAnimationFrame(frame);
      } else {
        prev.current = value;
      }
    };

    rafRef.current = requestAnimationFrame(frame);
    return () => cancelAnimationFrame(rafRef.current);
  }, [value, duration]);

  return <>{display}</>;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

const ASPECT_ICONS: Record<string, React.ReactNode> = {
  battery: <Battery size={16} />,
  camera: <Camera size={16} />,
  display: <Monitor size={16} />,
  screen: <Monitor size={16} />,
  performance: <Zap size={16} />,
  speed: <Zap size={16} />,
  price: <Tag size={16} />,
  value: <Tag size={16} />,
  audio: <Headphones size={16} />,
  sound: <Headphones size={16} />,
  connectivity: <Wifi size={16} />,
  network: <Wifi size={16} />,
  packaging: <Package size={16} />,
  software: <Code size={16} />,
  ui: <Code size={16} />,
};

function aspectIcon(aspect: string) {
  return ASPECT_ICONS[aspect.toLowerCase()] ?? <Star size={16} />;
}

function labelize(s: string) {
  return s.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function sentimentColorHex(sentiment: string) {
  if (sentiment === "positive") return "#22c55e";
  if (sentiment === "negative") return "#ef4444";
  return "#94a3b8";
}

function sentimentBadgeClass(sentiment: string) {
  if (sentiment === "positive") return "sentv-badge sentv-badge--pos";
  if (sentiment === "negative") return "sentv-badge sentv-badge--neg";
  return "sentv-badge sentv-badge--neu";
}

function languageBadgeClass(lang: string | null | undefined) {
  if (lang === "hinglish") return "sentv-badge sentv-badge--hin";
  if (lang === "mixed") return "sentv-badge sentv-badge--mix";
  return "sentv-badge sentv-badge--eng";
}

function ScoreBar({ score }: { score: number }) {
  const pct = Math.min(Math.abs(score), 100);
  const color = score >= 0 ? "#22c55e" : "#ef4444";
  return (
    <div className="sentv-score-track">
      <div className="sentv-score-fill" style={{ width: `${pct}%`, backgroundColor: color }} />
    </div>
  );
}

// ─── Stat Overview ────────────────────────────────────────────────────────────

function StatCards({ dashboard }: { dashboard: VisualDashboard }) {
  const dist = dashboard.sentiment_distribution_chart?.data ?? [];
  const total = dist.reduce((s, d) => s + d.value, 0);

  const pct = (label: string) => {
    if (!total) return 0;
    const row = dist.find((d) => d.label.toLowerCase() === label);
    return Math.round(((row?.value ?? 0) / total) * 100);
  };

  const positivePct = pct("positive");
  const negativePct = pct("negative");
  const neutralPct = pct("neutral");

  const ratingCard = dashboard.kpi_cards.find((c) => c.id === "average_rating");
  const rawRating = ratingCard?.value ?? null;
  const rating =
    typeof rawRating === "number"
      ? rawRating
      : parseFloat(String(rawRating ?? "0")) || 0;

  return (
    <div className="sentv-stat-grid">
      <article className="sentv-stat-card sentv-stat-card--pos">
        <span className="sentv-stat-label">Positive</span>
        <strong className="sentv-stat-value">
          <AnimatedNumber value={positivePct} />%
        </strong>
        <div className="sentv-stat-bar">
          <div className="sentv-stat-bar-fill" style={{ width: `${positivePct}%` }} />
        </div>
      </article>

      <article className="sentv-stat-card sentv-stat-card--neg">
        <span className="sentv-stat-label">Negative</span>
        <strong className="sentv-stat-value">
          <AnimatedNumber value={negativePct} />%
        </strong>
        <div className="sentv-stat-bar">
          <div className="sentv-stat-bar-fill" style={{ width: `${negativePct}%` }} />
        </div>
      </article>

      <article className="sentv-stat-card sentv-stat-card--neu">
        <span className="sentv-stat-label">Neutral</span>
        <strong className="sentv-stat-value">
          <AnimatedNumber value={neutralPct} />%
        </strong>
        <div className="sentv-stat-bar">
          <div className="sentv-stat-bar-fill" style={{ width: `${neutralPct}%` }} />
        </div>
      </article>

      <article className="sentv-stat-card sentv-stat-card--rat">
        <span className="sentv-stat-label">Avg Rating</span>
        <strong className="sentv-stat-value">{rating.toFixed(1)} ★</strong>
        <p className="sentv-stat-sub">out of 5</p>
      </article>
    </div>
  );
}

// ─── Aspect Breakdown ─────────────────────────────────────────────────────────

function AspectCard({ item }: { item: AspectSummaryItem }) {
  const [open, setOpen] = useState(false);
  const hasSubs = item.sub_aspects != null && Object.keys(item.sub_aspects).length > 0;
  const scoreLabel =
    item.aspect_score >= 10
      ? "sentv-badge--pos"
      : item.aspect_score <= -10
      ? "sentv-badge--neg"
      : "sentv-badge--neu";

  return (
    <article className="sentv-aspect-card">
      <div
        className={`sentv-aspect-header${hasSubs ? " sentv-aspect-header--clickable" : ""}`}
        onClick={() => hasSubs && setOpen((o) => !o)}
      >
        <div className="sentv-aspect-icon">{aspectIcon(item.aspect)}</div>
        <div className="sentv-aspect-meta">
          <div className="sentv-aspect-name-row">
            <strong>{labelize(item.aspect)}</strong>
            <span className={`sentv-badge ${scoreLabel}`}>
              {item.aspect_score >= 0 ? "+" : ""}
              {item.aspect_score.toFixed(0)}
            </span>
          </div>
          <ScoreBar score={item.aspect_score} />
          <div className="sentv-aspect-counts">
            <span className="sentv-count-pos">{item.positive_count}+</span>
            <span className="sentv-count-neg">{item.negative_count}−</span>
            <span className="sentv-count-neu">{item.neutral_count}~</span>
            <span className="sentv-count-total">{item.mentions} mentions</span>
          </div>
        </div>
        {hasSubs && (
          <button className="sentv-expand-btn" type="button" aria-label="expand sub-aspects">
            {open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          </button>
        )}
      </div>

      {open && hasSubs && (
        <div className="sentv-sub-aspects">
          {Object.entries(item.sub_aspects!).map(([sub, score]) => (
            <div key={sub} className="sentv-sub-row">
              <span className="sentv-sub-name">{labelize(sub)}</span>
              <ScoreBar score={score} />
              <span className="sentv-sub-score">
                {score >= 0 ? "+" : ""}
                {score.toFixed(0)}
              </span>
            </div>
          ))}
        </div>
      )}
    </article>
  );
}

function AspectBreakdown({
  productId,
  startDate,
  endDate,
}: {
  productId: string;
  startDate: string;
  endDate: string;
}) {
  const { data: aspects = [], isLoading } = useQuery<AspectSummaryItem[]>({
    queryKey: ["aspects", productId, startDate, endDate],
    queryFn: () =>
      fetchProductAspects(productId, { start_date: startDate, end_date: endDate }),
    staleTime: 5 * 60_000,
  });

  return (
    <section className="sentv-panel">
      <div className="sentv-panel-head">
        <div>
          <span className="sentv-eyebrow">Aspect View</span>
          <h3>What users feel about each area</h3>
        </div>
        <small>{aspects.length} aspect(s) · camera expands into sub-aspects</small>
      </div>

      {isLoading ? (
        <p className="sentv-empty">Loading aspects…</p>
      ) : aspects.length === 0 ? (
        <p className="sentv-empty">No aspect data. Import and analyze reviews first.</p>
      ) : (
        <div className="sentv-aspect-list">
          {aspects.map((item) => (
            <AspectCard key={item.aspect} item={item} />
          ))}
        </div>
      )}
    </section>
  );
}

// ─── Evidence Cards ───────────────────────────────────────────────────────────

type EvidenceFilter = "all" | "positive" | "negative" | "hinglish";

function EvidenceSection({
  evidence,
  isLoading,
}: {
  evidence: EvidenceItem[];
  isLoading: boolean;
}) {
  const [sentFilter, setSentFilter] = useState<EvidenceFilter>("all");
  const [aspectFilter, setAspectFilter] = useState("");

  const aspects = Array.from(new Set(evidence.map((e) => e.aspect))).sort();

  const filtered = evidence.filter((e) => {
    if (aspectFilter && e.aspect !== aspectFilter) return false;
    if (sentFilter === "positive" && e.sentiment !== "positive") return false;
    if (sentFilter === "negative" && e.sentiment !== "negative") return false;
    if (sentFilter === "hinglish" && e.language_type !== "hinglish") return false;
    return true;
  });

  const FILTER_BUTTONS: { key: EvidenceFilter; label: string }[] = [
    { key: "all",      label: "All" },
    { key: "positive", label: "Positive" },
    { key: "negative", label: "Negative" },
    { key: "hinglish", label: "Hinglish" },
  ];

  return (
    <section className="sentv-panel">
      <div className="sentv-panel-head">
        <div>
          <span className="sentv-eyebrow">Review Evidence</span>
          <h3>Verbatim user feedback</h3>
        </div>
        <small>
          {filtered.length} / {evidence.length} reviews
        </small>
      </div>

      <div className="sentv-filter-row">
        <div className="sentv-filter-chips">
          {FILTER_BUTTONS.map((btn) => (
            <button
              key={btn.key}
              type="button"
              className={`sentv-chip${sentFilter === btn.key ? " active" : ""}`}
              onClick={() => setSentFilter(btn.key)}
            >
              {btn.label}
            </button>
          ))}
        </div>
        {aspects.length > 1 && (
          <select
            className="sentv-aspect-select"
            value={aspectFilter}
            onChange={(e) => setAspectFilter(e.target.value)}
          >
            <option value="">All aspects</option>
            {aspects.map((a) => (
              <option key={a} value={a}>
                {labelize(a)}
              </option>
            ))}
          </select>
        )}
      </div>

      {isLoading ? (
        <p className="sentv-empty">Loading reviews…</p>
      ) : filtered.length === 0 ? (
        <p className="sentv-empty">No reviews match these filters.</p>
      ) : (
        <div className="sentv-evidence-list">
          {filtered.slice(0, 20).map((item, idx) => (
            <article key={`${item.review_id}-${idx}`} className="sentv-evidence-card">
              <div className="sentv-evidence-top">
                <div className="sentv-evidence-badges">
                  <span className={sentimentBadgeClass(item.sentiment)}>
                    {item.sentiment}
                  </span>
                  <span className={languageBadgeClass(item.language_type)}>
                    {item.language_type ?? "english"}
                  </span>
                  <span className="sentv-badge sentv-badge--asp">
                    {labelize(item.aspect)}
                  </span>
                </div>
                <div className="sentv-evidence-meta">
                  {item.rating != null && (
                    <span className="sentv-stars">
                      {"★".repeat(Math.round(item.rating))}
                      {"☆".repeat(5 - Math.round(item.rating))}
                    </span>
                  )}
                  {item.source && <span className="sentv-source">{item.source}</span>}
                  {item.review_date && (
                    <span className="sentv-date">{item.review_date.slice(0, 10)}</span>
                  )}
                </div>
              </div>
              {item.evidence_span && (
                <blockquote className="sentv-evidence-span">
                  "{item.evidence_span}"
                </blockquote>
              )}
              <p className="sentv-evidence-text">
                {item.raw_text.length > 240
                  ? item.raw_text.slice(0, 240) + "…"
                  : item.raw_text}
              </p>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

// ─── Phrase TreeMap ───────────────────────────────────────────────────────────

function buildTreeMapData(evidence: EvidenceItem[]) {
  const map = new Map<string, { count: number; pos: number; neg: number }>();

  for (const item of evidence) {
    if (!item.evidence_span) continue;
    const phrases = item.evidence_span
      .split(/[,;|]/)
      .map((p) => p.trim().toLowerCase())
      .filter((p) => p.length > 3 && p.length < 60);

    for (const phrase of phrases) {
      const existing = map.get(phrase);
      if (existing) {
        existing.count += 1;
        if (item.sentiment === "positive") existing.pos += 1;
        if (item.sentiment === "negative") existing.neg += 1;
      } else {
        map.set(phrase, {
          count: 1,
          pos: item.sentiment === "positive" ? 1 : 0,
          neg: item.sentiment === "negative" ? 1 : 0,
        });
      }
    }
  }

  return Array.from(map.entries())
    .sort((a, b) => b[1].count - a[1].count)
    .slice(0, 40)
    .map(([name, data]) => {
      const dominant =
        data.pos > data.neg
          ? "positive"
          : data.neg > data.pos
          ? "negative"
          : "neutral";
      return {
        name: labelize(name),
        value: data.count,
        itemStyle: { color: sentimentColorHex(dominant), opacity: 0.85 },
      };
    });
}

function PhraseTreeMap({ evidence }: { evidence: EvidenceItem[] }) {
  const treeData = buildTreeMapData(evidence);
  if (treeData.length < 3) return null;

  const option = {
    tooltip: {
      trigger: "item",
      formatter: ({ name, value }: { name: string; value: number }) =>
        `${name}: ${value} mention(s)`,
    },
    series: [
      {
        type: "treemap",
        data: treeData,
        width: "100%",
        height: "100%",
        roam: false,
        nodeClick: false,
        breadcrumb: { show: false },
        label: { show: true, fontSize: 12, color: "#fff", overflow: "truncate" },
        itemStyle: { borderWidth: 2, borderColor: "#0f172a" },
        levels: [{ itemStyle: { borderWidth: 0 } }],
      },
    ],
  };

  return (
    <section className="sentv-panel">
      <div className="sentv-panel-head">
        <div>
          <span className="sentv-eyebrow">Key Phrases</span>
          <h3>Most discussed topics — size = mentions, color = sentiment</h3>
        </div>
      </div>
      <ReactECharts option={option} style={{ height: 260 }} />
    </section>
  );
}

// ─── Main export ──────────────────────────────────────────────────────────────

export default function SentimentView({
  productId,
  startDate,
  endDate,
  dashboard,
}: {
  productId: string;
  startDate: string;
  endDate: string;
  dashboard: VisualDashboard;
}) {
  const period = { start_date: startDate, end_date: endDate };

  const { data: evidence = [], isLoading: evidenceLoading } = useQuery<EvidenceItem[]>({
    queryKey: ["evidence", productId, startDate, endDate],
    queryFn: () => fetchProductEvidence(productId, { ...period, limit: 50 }),
    staleTime: 5 * 60_000,
  });

  return (
    <div className="sentv-root">
      <div className="sentv-header">
        <p className="irip-eyebrow">Sentiment</p>
        <h2>What users feel about this product</h2>
      </div>

      <StatCards dashboard={dashboard} />
      <AspectBreakdown productId={productId} startDate={startDate} endDate={endDate} />
      <EvidenceSection evidence={evidence} isLoading={evidenceLoading} />
      <PhraseTreeMap evidence={evidence} />
    </div>
  );
}
