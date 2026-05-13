import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import { RefreshCw, AlertTriangle, Clock } from "lucide-react";
import type {
  MarketIntelligence,
  MarketPulseItem,
  UpcomingLaunchItem,
  CompetitorWatchItem,
} from "../api";
import { fetchMarketIntelligence } from "../api";

// ─── Constants ────────────────────────────────────────────────────────────────

const OWN_BRANDS = new Set(["itel", "infinix", "tecno", "transsion"]);

const CATEGORY_META: Record<
  MarketPulseItem["category"],
  { icon: string; label: string }
> = {
  launch:     { icon: "🚀", label: "Launch" },
  trend:      { icon: "📈", label: "Trend" },
  competitor: { icon: "⚔️", label: "Competitor" },
  consumer:   { icon: "👥", label: "Consumer" },
};

const THREAT_COLORS: Record<CompetitorWatchItem["threat_level"], string> = {
  high:   "mkt-badge mkt-badge--high",
  medium: "mkt-badge mkt-badge--medium",
  low:    "mkt-badge mkt-badge--low",
};

// ─── Helpers ──────────────────────────────────────────────────────────────────

function formatCachedAt(iso: string): string {
  try {
    const d = new Date(iso);
    const diffMin = Math.round((Date.now() - d.getTime()) / 60_000);
    if (diffMin < 1) return "just now";
    if (diffMin < 60) return `${diffMin} min ago`;
    const diffH = Math.floor(diffMin / 60);
    if (diffH < 24) return `${diffH}h ago`;
    return d.toLocaleDateString("en-IN", { day: "numeric", month: "short" });
  } catch {
    return "";
  }
}

function isOwnBrand(brand: string): boolean {
  return OWN_BRANDS.has(brand.toLowerCase());
}

// ─── Skeleton ─────────────────────────────────────────────────────────────────

function PulseSkeleton() {
  return (
    <div className="mkt-pulse-grid">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="mkt-skeleton mkt-skeleton--card" />
      ))}
    </div>
  );
}

function LaunchSkeleton() {
  return (
    <div className="mkt-launch-scroll">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="mkt-skeleton mkt-skeleton--launch" />
      ))}
    </div>
  );
}

function CompetitorSkeleton() {
  return (
    <div className="mkt-competitor-grid">
      {[0, 1, 2, 3].map((i) => (
        <div key={i} className="mkt-skeleton mkt-skeleton--comp" />
      ))}
    </div>
  );
}

// ─── Market Pulse ─────────────────────────────────────────────────────────────

function PulseCard({ item }: { item: MarketPulseItem }) {
  const meta = CATEGORY_META[item.category] ?? { icon: "📌", label: item.category };

  return (
    <motion.article
      className="mkt-pulse-card"
      whileHover={{ y: -4, boxShadow: "0 12px 32px rgba(0,0,0,0.10)" }}
      transition={{ type: "spring", stiffness: 320, damping: 22 }}
    >
      <div className="mkt-pulse-top">
        <span className="mkt-category-icon" aria-hidden="true">
          {meta.icon}
        </span>
        <span className="mkt-category-label">{meta.label}</span>
        <span
          className={`mkt-badge ${
            item.relevance === "high" ? "mkt-badge--high" : "mkt-badge--medium"
          }`}
        >
          {item.relevance.toUpperCase()}
        </span>
      </div>
      <strong className="mkt-pulse-headline">{item.headline}</strong>
      <p className="mkt-pulse-summary">{item.summary}</p>
    </motion.article>
  );
}

function MarketPulseSection({
  items,
  isLoading,
}: {
  items: MarketPulseItem[];
  isLoading: boolean;
}) {
  return (
    <section className="mkt-section">
      <div className="mkt-section-head">
        <span className="mkt-eyebrow">Market Pulse</span>
        <h3>India smartphone signals this week</h3>
      </div>
      {isLoading ? (
        <PulseSkeleton />
      ) : (
        <div className="mkt-pulse-grid">
          {items.map((item, i) => (
            <PulseCard key={i} item={item} />
          ))}
        </div>
      )}
    </section>
  );
}

// ─── Upcoming Launches ────────────────────────────────────────────────────────

function LaunchCard({ item }: { item: UpcomingLaunchItem }) {
  const competitor = !isOwnBrand(item.brand);

  return (
    <motion.article
      className="mkt-launch-card"
      whileHover={{ y: -3, boxShadow: "0 8px 24px rgba(0,0,0,0.09)" }}
      transition={{ type: "spring", stiffness: 320, damping: 22 }}
    >
      <div className="mkt-launch-top">
        <strong className="mkt-launch-brand">{item.brand}</strong>
        {competitor && (
          <span className="mkt-badge mkt-badge--comp">Competitor</span>
        )}
      </div>
      <p className="mkt-launch-model">{item.model}</p>
      <div className="mkt-launch-meta">
        <span className="mkt-launch-date">
          <Clock size={11} />
          {item.estimated_date}
        </span>
        <span className="mkt-launch-price">{item.expected_price_inr}</span>
      </div>
      <p className="mkt-launch-feature">{item.key_feature}</p>
    </motion.article>
  );
}

function UpcomingLaunchesSection({
  items,
  isLoading,
}: {
  items: UpcomingLaunchItem[];
  isLoading: boolean;
}) {
  const sorted = [...items].sort((a, b) =>
    a.estimated_date.localeCompare(b.estimated_date)
  );

  return (
    <section className="mkt-section">
      <div className="mkt-section-head">
        <span className="mkt-eyebrow">Upcoming Launches</span>
        <h3>₹10K–₹35K segment — launches to watch</h3>
      </div>
      {isLoading ? (
        <LaunchSkeleton />
      ) : sorted.length === 0 ? (
        <p className="mkt-empty">No upcoming launches available right now.</p>
      ) : (
        <div className="mkt-launch-scroll">
          {sorted.map((item, i) => (
            <LaunchCard key={i} item={item} />
          ))}
        </div>
      )}
    </section>
  );
}

// ─── Competitor Watch ─────────────────────────────────────────────────────────

function CompetitorCard({ item }: { item: CompetitorWatchItem }) {
  return (
    <motion.article
      className="mkt-comp-card"
      whileHover={{ y: -3, boxShadow: "0 8px 24px rgba(0,0,0,0.09)" }}
      transition={{ type: "spring", stiffness: 320, damping: 22 }}
    >
      <div className="mkt-comp-top">
        <strong className="mkt-comp-brand">{item.brand}</strong>
        <span className={THREAT_COLORS[item.threat_level]}>
          {item.threat_level.toUpperCase()}
        </span>
      </div>
      <p className="mkt-comp-move">{item.recent_move}</p>
      <div className="mkt-comp-response">
        <span className="mkt-comp-response-label">Our response</span>
        <p>{item.our_response}</p>
      </div>
    </motion.article>
  );
}

function CompetitorWatchSection({
  items,
  isLoading,
}: {
  items: CompetitorWatchItem[];
  isLoading: boolean;
}) {
  return (
    <section className="mkt-section">
      <div className="mkt-section-head">
        <span className="mkt-eyebrow">Competitor Watch</span>
        <h3>What rivals are doing right now</h3>
      </div>
      {isLoading ? (
        <CompetitorSkeleton />
      ) : items.length === 0 ? (
        <p className="mkt-empty">No competitor data available.</p>
      ) : (
        <div className="mkt-competitor-grid">
          {items.map((item, i) => (
            <CompetitorCard key={i} item={item} />
          ))}
        </div>
      )}
    </section>
  );
}

// ─── Segment Trend + Consumer Shift ──────────────────────────────────────────

function InsightCards({
  segmentTrend,
  consumerShift,
  isLoading,
}: {
  segmentTrend: string;
  consumerShift: string;
  isLoading: boolean;
}) {
  return (
    <section className="mkt-section">
      <div className="mkt-section-head">
        <span className="mkt-eyebrow">Segment Insights</span>
        <h3>Macro signals for ₹10K–₹35K India</h3>
      </div>
      <div className="mkt-insight-row">
        <div className="mkt-insight-card">
          <span className="mkt-insight-quote">"</span>
          <div>
            <span className="mkt-insight-label">Segment Trend</span>
            {isLoading ? (
              <div className="mkt-skeleton mkt-skeleton--text" />
            ) : (
              <p className="mkt-insight-text">{segmentTrend}</p>
            )}
          </div>
        </div>
        <div className="mkt-insight-card">
          <span className="mkt-insight-quote">"</span>
          <div>
            <span className="mkt-insight-label">Consumer Shift</span>
            {isLoading ? (
              <div className="mkt-skeleton mkt-skeleton--text" />
            ) : (
              <p className="mkt-insight-text">{consumerShift}</p>
            )}
          </div>
        </div>
      </div>
    </section>
  );
}

// ─── Main export ──────────────────────────────────────────────────────────────

const FADE_UP = {
  initial: { opacity: 0, y: 16 },
  animate: { opacity: 1, y: 0 },
  transition: { duration: 0.4, ease: "easeOut" as const },
};

export default function MarketTab() {
  const {
    data,
    isLoading,
    isError,
    refetch,
    isFetching,
  } = useQuery<MarketIntelligence>({
    queryKey: ["marketIntelligence"],
    queryFn: fetchMarketIntelligence,
    staleTime: 24 * 60 * 60 * 1000,
    refetchOnWindowFocus: false,
    retry: 1,
    placeholderData: keepPreviousData,
  });

  const pulse = data?.market_pulse ?? [];
  const launches = data?.upcoming_launches ?? [];
  const competitors = data?.competitor_watch ?? [];
  const segmentTrend = data?.segment_trend ?? "";
  const consumerShift = data?.consumer_shift ?? "";
  const cachedAt = data?.cached_at ?? "";
  const hasError = isError || data?.error === true;

  return (
    <motion.div className="mkt-root" {...FADE_UP}>
      {/* Header */}
      <div className="mkt-header">
        <div>
          <p className="irip-eyebrow">Market</p>
          <h2>Live Market Intelligence</h2>
        </div>
        <div className="mkt-header-right">
          {cachedAt && (
            <span className="mkt-cached-label">
              <Clock size={12} />
              Updated {formatCachedAt(cachedAt)}
            </span>
          )}
          <button
            className="mkt-refresh-btn"
            type="button"
            onClick={() => refetch()}
            disabled={isFetching}
            aria-label="Refresh market data"
          >
            <RefreshCw size={14} className={isFetching ? "mkt-spin" : ""} />
            {isFetching ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </div>

      {/* Error banner */}
      <AnimatePresence>
        {hasError && (
          <motion.div
            className="mkt-error-banner"
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
          >
            <AlertTriangle size={15} />
            <span>
              Market data temporarily unavailable — cached data shown.{" "}
              {data?.error_message ? `(${data.error_message.slice(0, 80)})` : ""}
            </span>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Sections */}
      <MarketPulseSection items={pulse} isLoading={isLoading} />
      <UpcomingLaunchesSection items={launches} isLoading={isLoading} />
      <CompetitorWatchSection items={competitors} isLoading={isLoading} />
      <InsightCards
        segmentTrend={segmentTrend}
        consumerShift={consumerShift}
        isLoading={isLoading}
      />
    </motion.div>
  );
}
