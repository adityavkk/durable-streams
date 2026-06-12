export interface ScrubberProps {
  total: number
  position: number
  isLive: boolean
  replaying: boolean
  onScrub: (pos: number) => void
  onReplay: () => void
  onStopReplay: () => void
  onGoLive: () => void
  onRefetch: () => void
}

export function Scrubber(props: ScrubberProps) {
  const empty = props.total === 0
  return (
    <div className="scrubber">
      <button
        className="toolbar-btn"
        onClick={props.replaying ? props.onStopReplay : props.onReplay}
        title={
          props.replaying
            ? `Stop the replay at the current position`
            : `Replay the stream from the beginning`
        }
        disabled={empty}
      >
        {props.replaying ? `■` : `▶ Replay`}
      </button>
      <input
        type="range"
        className="scrubber-range"
        aria-label="Scrub position in the stream history"
        min={0}
        max={props.total}
        value={props.position}
        onChange={(e) => props.onScrub(Number(e.target.value))}
        disabled={empty}
      />
      <span className="scrubber-counter">
        {props.position} / {props.total}
      </span>
      <button
        className={props.isLive ? `live-pill live-pill-on` : `live-pill`}
        onClick={props.onGoLive}
        title="Jump to live tail"
      >
        {`● live`}
      </button>
      <button
        className="toolbar-btn"
        onClick={props.onRefetch}
        title="Protocol-level replay: clear local state and re-read the whole stream from offset -1 (watch the request lane)"
      >
        {`↻ Re-fetch`}
      </button>
    </div>
  )
}
