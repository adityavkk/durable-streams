import { StatusBadge } from "./StatusBadge"
import type { ReaderStatus } from "../lib/live-reader"

export interface StreamToolbarProps {
  status: ReaderStatus
  entries: number
  bytes: number
  ratePerMin: number
  isJsonStream: boolean
  filter: string
  filterError: boolean
  onFilterChange: (v: string) => void
  /** Hide the demo-producer button (e.g. on system streams). Default true. */
  showDemo?: boolean
  demoRunning: boolean
  onToggleDemo: () => void
  offline: boolean
  onToggleConnection: () => void
  laneOpen: boolean
  onToggleLane: () => void
  pendingRequests: number
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

export function StreamToolbar(props: StreamToolbarProps) {
  return (
    <div className="stream-toolbar">
      <StatusBadge status={props.status} />
      <span className="toolbar-stats">
        {props.entries} entries · {fmtBytes(props.bytes)} · {props.ratePerMin}
        /min
      </span>
      {props.isJsonStream && (
        <input
          className={
            props.filterError
              ? `filter-input filter-input-error`
              : `filter-input`
          }
          placeholder={`filter: item.token !== ""`}
          value={props.filter}
          spellCheck={false}
          title="JavaScript expression over (item, i)"
          onChange={(e) => props.onFilterChange(e.target.value)}
        />
      )}
      {props.showDemo !== false && (
        <button
          className={
            props.demoRunning ? `toolbar-btn toolbar-btn-active` : `toolbar-btn`
          }
          onClick={props.onToggleDemo}
          title="Append a simulated LLM token stream"
        >
          {props.demoRunning ? `■ Stop demo` : `▶ Demo producer`}
        </button>
      )}
      <button
        className={
          props.offline
            ? `toolbar-btn toolbar-btn-active toolbar-btn-accent`
            : `toolbar-btn`
        }
        onClick={props.onToggleConnection}
        title={
          props.offline
            ? `Reconnect and catch up from the stored offset`
            : `Simulate a network drop — the stream keeps growing, resume catches up from the stored offset`
        }
      >
        {props.offline ? `↺ Resume` : `⏻ Disconnect`}
      </button>
      <button
        className={
          props.laneOpen ? `toolbar-btn toolbar-btn-active` : `toolbar-btn`
        }
        onClick={props.onToggleLane}
        title="Show the long-poll request lane"
      >
        {`⇅ Requests`}
        {props.pendingRequests > 0 && (
          <span className="pending-chip">{props.pendingRequests}</span>
        )}
      </button>
    </div>
  )
}
