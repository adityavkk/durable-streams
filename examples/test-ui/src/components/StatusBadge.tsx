import type { ReaderStatus } from "../lib/live-reader"

const STATUS_LABELS: Record<ReaderStatus, string> = {
  connecting: `connecting`,
  "catching-up": `catching up`,
  live: `live`,
  offline: `offline`,
  error: `error`,
}

export function StatusBadge({ status }: { status: ReaderStatus }) {
  return (
    <span className={`status-badge status-${status}`}>
      <span className="status-dot" />
      {STATUS_LABELS[status]}
    </span>
  )
}
