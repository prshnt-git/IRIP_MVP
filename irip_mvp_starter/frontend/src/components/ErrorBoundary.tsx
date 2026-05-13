import { Component, type ReactNode, type ErrorInfo } from "react";
import { AlertTriangle, Database, RefreshCw } from "lucide-react";

// ─── Error Boundary (class component — only class components can be error boundaries) ──

type BoundaryProps = {
  children: ReactNode;
  label?: string;
};

type BoundaryState = {
  hasError: boolean;
  error: Error | null;
};

export default class ErrorBoundary extends Component<BoundaryProps, BoundaryState> {
  constructor(props: BoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): BoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("[IRIP ErrorBoundary]", this.props.label ?? "tab", error, info.componentStack);
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div className="eb-root">
          <div className="eb-card">
            <AlertTriangle size={28} className="eb-icon" />
            <h3>Something went wrong loading this section.</h3>
            <p>Try refreshing, or contact <strong>Prashant Tiwari</strong> at yes.prshnt@gmail.com if the issue persists.</p>
            <div className="eb-actions">
              <button
                className="eb-refresh-btn"
                type="button"
                onClick={() => window.location.reload()}
              >
                <RefreshCw size={13} />
                Refresh
              </button>
              <a
                className="eb-report-link"
                href={`mailto:yes.prshnt@gmail.com?subject=${encodeURIComponent("IRIP Error Report")}&body=${encodeURIComponent(`Error: ${this.state.error?.message ?? "unknown"}\nTab: ${this.props.label ?? "unknown"}`)}`}
              >
                Report Issue
              </a>
            </div>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// ─── Shared empty state for tabs that require reviews ──────────────────────────

export function EmptyReviewsCard() {
  return (
    <div className="empty-reviews-state">
      <Database size={36} className="empty-reviews-icon" />
      <h3>No reviews yet for this product</h3>
      <p>
        Data will appear after the next automated scrape (runs daily at 8 AM IST).
      </p>
    </div>
  );
}
