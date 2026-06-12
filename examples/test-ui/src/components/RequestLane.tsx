import { useEffect, useState } from "react"
import type { RequestRecord } from "../lib/live-reader"

export interface RequestLaneProps {
  requests: Array<RequestRecord>
  open: boolean
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / (1024 * 1024)).toFixed(1)} MB`
}

function pad2(n: number): string {
  return n.toString().padStart(2, `0`)
}

function fmtTime(epochMs: number): string {
  const d = new Date(epochMs)
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}`
}

function fmtDuration(ms: number): string {
  if (ms < 1000) return `${Math.max(0, Math.round(ms))}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

function middleTruncate(s: string): string {
  if (s.length <= 18) return s
  return `${s.slice(0, 8)}…${s.slice(-8)}`
}

function outcomeLabel(req: RequestRecord): string {
  switch (req.outcome) {
    case `pending`:
      return `parked…`
    case `data`:
      return req.bytes >= 0 ? `data · ${fmtBytes(req.bytes)}` : `data`
    case `empty`:
      return `empty`
    case `error`:
      return req.status ? `err ${req.status}` : `err`
    default:
      return `aborted`
  }
}

export function RequestLane({ requests, open }: RequestLaneProps) {
  const hasPending = requests.some((r) => r.outcome === `pending`)
  const [, setTick] = useState(0)

  useEffect(() => {
    if (!open || !hasPending) return
    const id = setInterval(() => setTick((t) => t + 1), 250)
    return () => clearInterval(id)
  }, [open, hasPending])

  if (!open) return null

  const rows = requests.slice(-50).reverse()
  const now = Date.now()

  return (
    <div className="request-lane">
      <div className="request-lane-header">
        long-poll requests{` `}
        <span className="request-lane-hint">
          each row is one GET (last 50); parked rows are the server holding the
          poll open
        </span>
      </div>
      <div className="request-lane-rows">
        {rows.length === 0 && (
          <div className="request-row">
            <span className="req-time">—</span>
            <span className="req-offset">no requests yet</span>
          </div>
        )}
        {rows.map((req) => (
          <div
            key={req.id}
            className={
              req.outcome === `pending`
                ? `request-row request-row-pending`
                : `request-row`
            }
          >
            <span className="req-time">{fmtTime(req.startedAt)}</span>
            <span className="req-offset" title={req.offsetParam ?? undefined}>
              {req.offsetParam === null ? `—` : middleTruncate(req.offsetParam)}
            </span>
            <span className="req-duration">
              {fmtDuration((req.endedAt ?? now) - req.startedAt)}
            </span>
            <span className={`outcome-chip outcome-${req.outcome}`}>
              {outcomeLabel(req)}
            </span>
            {req.upToDate ? (
              <span className="uptodate-dot" title="Stream-Up-To-Date: true" />
            ) : null}
          </div>
        ))}
      </div>
    </div>
  )
}
