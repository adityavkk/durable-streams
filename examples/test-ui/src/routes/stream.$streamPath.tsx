import { createFileRoute, redirect } from "@tanstack/react-router"
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react"
import { DurableStream, IdempotentProducer } from "@durable-streams/client"
import { and, eq, gt, useLiveQuery } from "@tanstack/react-db"
import { useVirtualizer } from "@tanstack/react-virtual"
import ReactJson from "react-json-view"
import { useStreamDB } from "../lib/stream-db-context"
import { useTypingIndicator } from "../hooks/useTypingIndicator"
import { getReader } from "../lib/live-reader"
import { StreamToolbar } from "../components/StreamToolbar"
import { RequestLane } from "../components/RequestLane"
import { Scrubber } from "../components/Scrubber"

const SERVER_URL = `http://${typeof window !== `undefined` ? window.location.hostname : `localhost`}:4437`

// Derive an empty-valued skeleton from a JSON value so the write box can be
// prefilled with an entry of the same shape as what's already in the stream.
function skeletonOf(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.length > 0 ? [skeletonOf(value[0])] : []
  }
  if (value !== null && typeof value === `object`) {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([k, v]) => [
        k,
        skeletonOf(v),
      ])
    )
  }
  if (typeof value === `string`) return ``
  if (typeof value === `number`) return 0
  if (typeof value === `boolean`) return false
  return null
}

const truncateOffset = (offset: string): string =>
  offset.length > 18 ? `${offset.slice(0, 8)}…${offset.slice(-8)}` : offset

// Token source for the demo producer — appended one word at a time to
// simulate an LLM token stream.
const DEMO_WORDS =
  `Durable streams turn ephemeral transport into a durable log : every append is persisted , ordered , and addressable by offset . Any reader can catch up from history , tail live updates over long-polling , or replay the whole stream — without losing a byte across reconnects .`.split(
    ` `
  )

interface JsonEntry {
  value: unknown
  /** Next-offset after this entry's chunk — the protocol resume point */
  chunkOffset: string
  /** Offset BEFORE this entry's chunk — re-reading from here includes it */
  readFromOffset: string
  /** Epoch ms the chunk arrived */
  at: number
}

export const Route = createFileRoute(`/stream/$streamPath`)({
  loader: async ({ params }) => {
    try {
      const streamMetadata = new DurableStream({
        url: `${SERVER_URL}/v1/stream/${params.streamPath}`,
      })
      const metadata = await streamMetadata.head()
      if (!metadata.exists) {
        throw redirect({ to: `/` })
      }
      const stream = new DurableStream({
        url: `${SERVER_URL}/v1/stream/${params.streamPath}`,
        contentType: metadata.contentType || undefined,
      })
      return {
        contentType: metadata.contentType || undefined,
        stream,
      }
    } catch {
      throw redirect({ to: `/` })
    }
  },
  component: StreamViewer,
})

function StreamViewer() {
  const { streamPath } = Route.useParams()
  const { contentType, stream } = Route.useLoaderData()
  const { presenceDB } = useStreamDB()
  const { startTyping } = useTypingIndicator(streamPath)
  const [writeInput, setWriteInput] = useState(``)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState(``)
  const [laneOpen, setLaneOpen] = useState(false)
  const [scrubPos, setScrubPos] = useState<number | null>(null) // null = live
  const [replaying, setReplaying] = useState(false)
  const [demoRunning, setDemoRunning] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const parentRef = useRef<HTMLDivElement>(null)
  const [now, setNow] = useState(Date.now())

  // Demo producer timer — declared before the write producer so its
  // onError can stop a runaway demo against a failed stream.
  const demoTimerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const stopDemo = useCallback(() => {
    if (demoTimerRef.current !== null) {
      clearInterval(demoTimerRef.current)
      demoTimerRef.current = null
    }
    setDemoRunning(false)
  }, [])
  useEffect(() => stopDemo, [stopDemo, streamPath])

  // Create IdempotentProducer for exactly-once write semantics
  const producerRef = useRef<IdempotentProducer | null>(null)
  useEffect(() => {
    let disposed = false
    const makeProducer = (): IdempotentProducer => {
      // Generate a unique producer ID per browser session
      const producerId = `test-ui-${crypto.randomUUID().slice(0, 8)}`
      const producer: IdempotentProducer = new IdempotentProducer(
        stream,
        producerId,
        {
          autoClaim: true,
          lingerMs: 0, // Send immediately for interactive UI
          // append() is fire-and-forget: server rejections surface here,
          // not at the call site. A failed producer also rejects all later
          // appends, so replace it to keep the write box usable.
          onError: (err) => {
            setError(`Write rejected by server: ${err.message}`)
            stopDemo()
            // One failure fires onError once per in-flight batch; only the
            // producer that errored is replaced, so the burst can't detach
            // its healthy replacement.
            if (!disposed && producerRef.current === producer) {
              void producer.detach()
              producerRef.current = makeProducer()
            }
          },
        }
      )
      return producer
    }
    producerRef.current = makeProducer()

    return () => {
      // detach, not close: close() would mark the stream itself closed (EOF
      // for every reader) just because this view unmounted.
      disposed = true
      void producerRef.current?.detach()
    }
  }, [stream, stopDemo])

  // Instrumented reader: chunks + request log + connection status
  const reader = useMemo(
    () => getReader(streamPath, stream),
    [streamPath, stream]
  )
  const subscribeReader = useCallback(
    (cb: () => void) => reader.subscribe(cb),
    [reader]
  )
  const getReaderSnapshot = useCallback(() => reader.getSnapshot(), [reader])
  const { chunks, requests, status, lastError } = useSyncExternalStore(
    subscribeReader,
    getReaderSnapshot
  )

  // The loader's head() just confirmed this stream exists. If the cached
  // reader errored on a previous visit (e.g. the stream was deleted and
  // recreated at the same path), give it one fresh start per visit.
  useEffect(() => {
    if (reader.getSnapshot().status === `error`) {
      reader.resume()
    }
  }, [reader])

  // Reset per-stream inspector state when navigating between streams — the
  // route component stays mounted across $streamPath changes.
  useEffect(() => {
    setError(null)
    setFilter(``)
    setScrubPos(null)
    setReplaying(false)
    setWriteInput(``)
  }, [streamPath])

  const isRegistryStream =
    streamPath === `__registry__` || streamPath === `__presence__`
  const isJsonStream = contentType?.includes(`application/json`)

  // Flatten chunks into individual JSON items, each tagged with the offset
  // of the chunk it arrived in (its protocol resume point).
  const jsonItems = useMemo<Array<JsonEntry>>(() => {
    if (!isJsonStream) return []
    const items: Array<JsonEntry> = []
    let readFromOffset = `-1`
    for (const chunk of chunks) {
      try {
        const parsed = JSON.parse(chunk.data)
        const values = Array.isArray(parsed) ? parsed : [parsed]
        for (const value of values) {
          items.push({
            value,
            chunkOffset: chunk.offset,
            readFromOffset,
            at: chunk.at,
          })
        }
      } catch {
        // Skip chunks that don't parse — never take the whole view down.
      }
      readFromOffset = chunk.offset
    }
    return items
  }, [chunks, isJsonStream])

  // Programmable filter (Redpanda-style): a JS expression over (item, i)
  const filterFn = useMemo(() => {
    const src = filter.trim()
    if (!src) return null
    try {
      return new Function(`item`, `i`, `return ( ${src} )`) as (
        item: unknown,
        i: number
      ) => unknown
    } catch {
      return `invalid` as const
    }
  }, [filter])
  const filterError = filterFn === `invalid`

  const visibleItems = useMemo(() => {
    if (!filterFn || filterFn === `invalid`) return jsonItems
    return jsonItems.filter((item, i) => {
      try {
        return Boolean(filterFn(item.value, i))
      } catch {
        return false
      }
    })
  }, [jsonItems, filterFn])

  // Scrub/replay state: position counts visible items (JSON) or chunks (text)
  const total = isJsonStream ? visibleItems.length : chunks.length
  const position = scrubPos === null ? total : Math.min(scrubPos, total)
  const isLive = scrubPos === null

  const displayedItems = isLive ? visibleItems : visibleItems.slice(0, position)
  const displayedText = useMemo(() => {
    if (isJsonStream) return ``
    const slice = isLive ? chunks : chunks.slice(0, position)
    return slice.map((chunk) => chunk.data).join(``)
  }, [isJsonStream, chunks, isLive, position])

  const totalRef = useRef(total)
  totalRef.current = total
  const posRef = useRef(position)
  posRef.current = position

  useEffect(() => {
    if (!replaying) return
    const timer = setInterval(() => {
      const next = posRef.current + 1
      if (next >= totalRef.current) {
        setScrubPos(null)
        setReplaying(false)
      } else {
        setScrubPos(next)
      }
    }, 40)
    return () => clearInterval(timer)
  }, [replaying])

  // Stats for the toolbar
  const totalBytes = useMemo(
    () => chunks.reduce((sum, chunk) => sum + chunk.bytes, 0),
    [chunks]
  )
  const entries = isJsonStream ? jsonItems.length : chunks.length
  const ratePerMin = useMemo(() => {
    const cutoff = now - 60000
    return isJsonStream
      ? jsonItems.filter((item) => item.at >= cutoff).length
      : chunks.filter((chunk) => chunk.at >= cutoff).length
  }, [isJsonStream, jsonItems, chunks, now])

  const pendingRequests = useMemo(
    () => requests.filter((r) => r.outcome === `pending`).length,
    [requests]
  )

  // Template entry matching the shape of the latest item in the stream,
  // so writes conform to what the stream already holds.
  const template = useMemo(() => {
    if (!isJsonStream) return null
    if (jsonItems.length === 0) return JSON.stringify({})
    return JSON.stringify(skeletonOf(jsonItems[jsonItems.length - 1].value))
  }, [isJsonStream, jsonItems])

  // Demo producer: append one word at a time through the idempotent
  // producer, shaped to match the stream (template-aware for JSON).
  const demoEntry = useCallback(
    (word: string): string => {
      if (template) {
        try {
          const shaped = JSON.parse(template)
          if (
            shaped !== null &&
            typeof shaped === `object` &&
            !Array.isArray(shaped)
          ) {
            const record = shaped as Record<string, unknown>
            const stringKey = Object.keys(record).find(
              (key) => typeof record[key] === `string`
            )
            record[stringKey ?? `token`] = word
            return JSON.stringify(record)
          }
          if (typeof shaped === `string`) return JSON.stringify(word)
        } catch {
          // fall through to the default shape
        }
      }
      return JSON.stringify({ token: word })
    },
    [template]
  )

  const toggleDemo = () => {
    if (demoRunning) {
      stopDemo()
      return
    }
    let i = 0
    setDemoRunning(true)
    demoTimerRef.current = setInterval(() => {
      if (i >= DEMO_WORDS.length) {
        stopDemo()
        return
      }
      const word = DEMO_WORDS[i++]
      const producer = producerRef.current
      if (!producer) return
      try {
        producer.append(
          isJsonStream
            ? demoEntry(word) + `\n`
            : `${word}${i % 14 === 0 ? `\n` : ` `}`
        )
      } catch {
        stopDemo()
      }
    }, 60)
  }

  // Set up virtualizer for JSON streams
  const virtualizer = useVirtualizer({
    count: displayedItems.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 100, // Estimate item height
    overscan: 5, // Render 5 items outside viewport
  })

  // Custom theme matching app colors
  const jsonTheme = {
    base00: `#ffffff`, // bg-card
    base01: `#f5f1e8`, // bg-main
    base02: `#e5dfd5`, // border-subtle
    base03: `#6b5d54`, // text-dim (comments)
    base04: `#4a4543`, // text-secondary
    base05: `#2d2a28`, // text-primary (default text)
    base06: `#2d2a28`, // text-primary
    base07: `#2d2a28`, // text-primary
    base08: `#d4704b`, // accent-primary (null, undefined, regex)
    base09: `#c8886d`, // accent-warm (numbers, booleans)
    base0A: `#7a9a7e`, // accent-secondary (functions)
    base0B: `#d4704b`, // accent-primary (strings)
    base0C: `#7a9a7e`, // accent-secondary (dates)
    base0D: `#4a4543`, // text-secondary (keys)
    base0E: `#c8886d`, // accent-warm (keywords)
    base0F: `#d4704b`, // accent-primary (deprecation)
  }

  // Update "now" every 5 seconds to re-evaluate stale typing indicators
  // and the entries-per-minute rate.
  useEffect(() => {
    const interval = setInterval(() => {
      setNow(Date.now())
    }, 5000)
    return () => clearInterval(interval)
  }, [])

  // Query typing users for this stream
  const { data: typers = [] } = useLiveQuery(
    (q) =>
      q
        .from({ presence: presenceDB.collections.presence })
        .where(({ presence }) =>
          and(
            eq(presence.streamPath, streamPath),
            eq(presence.isTyping, true),
            gt(presence.lastSeen, now - 60000)
          )
        ),
    [streamPath, now]
  )

  // Auto-scroll to bottom when new messages arrive — only while live, never
  // while the user is scrubbed back into history.
  useEffect(() => {
    if (!isLive) return
    if (isJsonStream && displayedItems.length > 0) {
      // Defer scroll to avoid flushSync warning
      queueMicrotask(() => {
        virtualizer.scrollToIndex(displayedItems.length - 1, { align: `end` })
      })
    } else if (!isJsonStream) {
      messagesEndRef.current?.scrollIntoView({ behavior: `smooth` })
    }
  }, [isLive, isJsonStream, displayedItems.length, virtualizer])

  const writeToStream = () => {
    if (!writeInput.trim() || !producerRef.current) return

    // The server rejects non-JSON appends to application/json streams with
    // a 400, so validate before sending instead of failing silently.
    if (isJsonStream) {
      try {
        JSON.parse(writeInput)
      } catch {
        setError(
          `Not valid JSON — this stream is application/json, so every entry must parse. ` +
            `Try the Template button${template ? `, e.g. ${template}` : ``}.`
        )
        return
      }
    }

    try {
      setError(null)
      producerRef.current.append(writeInput + `\n`)
      setWriteInput(``)
    } catch (err: any) {
      setError(`Failed to write to stream: ${err.message}`)
    }
  }

  const banner =
    error ??
    (status === `error`
      ? `Reader error: ${lastError ?? `unknown`} — use Resume to reconnect.`
      : null)

  return (
    <div className="stream-view">
      {banner && <div className="error">{banner}</div>}
      <div className="header">
        <h2>{decodeURIComponent(streamPath)}</h2>
      </div>
      <StreamToolbar
        status={status}
        entries={entries}
        bytes={totalBytes}
        ratePerMin={ratePerMin}
        isJsonStream={Boolean(isJsonStream)}
        filter={filter}
        filterError={filterError}
        onFilterChange={setFilter}
        showDemo={!isRegistryStream}
        demoRunning={demoRunning}
        onToggleDemo={toggleDemo}
        offline={status === `offline` || status === `error`}
        onToggleConnection={() => {
          if (status === `offline` || status === `error`) {
            reader.resume()
          } else {
            reader.disconnect()
          }
        }}
        laneOpen={laneOpen}
        onToggleLane={() => setLaneOpen((open) => !open)}
        pendingRequests={pendingRequests}
      />
      <div className="messages" ref={parentRef}>
        {total === 0 && (
          <div className="filter-empty-note">
            {filter.trim() && entries > 0
              ? `No entries match the filter.`
              : `Listening for new messages...`}
          </div>
        )}
        {total !== 0 ? (
          isJsonStream ? (
            <div
              style={{
                height: `${virtualizer.getTotalSize()}px`,
                width: `100%`,
                position: `relative`,
              }}
            >
              {virtualizer.getVirtualItems().map((virtualItem) => {
                const item = displayedItems[virtualItem.index]
                return (
                  <div
                    key={virtualItem.key}
                    data-index={virtualItem.index}
                    ref={virtualizer.measureElement}
                    style={{
                      position: `absolute`,
                      top: 0,
                      left: 0,
                      width: `100%`,
                      transform: `translateY(${virtualItem.start}px)`,
                    }}
                  >
                    <div className="message json-message">
                      <button
                        className="offset-chip"
                        title={`Offset after this entry's batch: ${item.chunkOffset}\nClick to re-read the stream from this batch onward (watch the request lane).`}
                        onClick={() => {
                          setScrubPos(null)
                          setReplaying(false)
                          reader.restartFrom(item.readFromOffset)
                        }}
                      >
                        {truncateOffset(item.chunkOffset)}
                      </button>
                      <ReactJson
                        src={item.value as object}
                        collapsed={1}
                        name={false}
                        displayDataTypes={false}
                        enableClipboard={false}
                        theme={jsonTheme}
                      />
                    </div>
                  </div>
                )
              })}
            </div>
          ) : (
            <div className="message">
              <pre>{displayedText}</pre>
            </div>
          )
        ) : null}
        <div ref={messagesEndRef} />
      </div>
      <Scrubber
        total={total}
        position={position}
        isLive={isLive && !replaying}
        replaying={replaying}
        onScrub={(pos) => {
          setReplaying(false)
          setScrubPos(pos >= total ? null : pos)
        }}
        onReplay={() => {
          if (total === 0) return
          setScrubPos(0)
          setReplaying(true)
        }}
        onStopReplay={() => setReplaying(false)}
        onGoLive={() => {
          setReplaying(false)
          setScrubPos(null)
        }}
        onRefetch={() => {
          setReplaying(false)
          setScrubPos(null)
          reader.restartFrom(`-1`)
        }}
      />
      <RequestLane requests={requests} open={laneOpen} />
      {!isRegistryStream && (
        <>
          {typers.length > 0 && (
            <div className="typing-indicator">
              {typers.map((t) => t.userId.slice(0, 8)).join(`, `)} typing...
            </div>
          )}
          <div className="write-section">
            <textarea
              placeholder={
                isJsonStream
                  ? `JSON entry, e.g. ${template} (Shift+Enter for new line)...`
                  : `Type your message (Shift+Enter for new line)...`
              }
              value={writeInput}
              onChange={(e) => {
                setWriteInput(e.target.value)
                startTyping()
              }}
              onKeyDown={(e) => {
                if (e.key === `Enter` && !e.shiftKey) {
                  e.preventDefault()
                  writeToStream()
                }
              }}
            />
            {isJsonStream && (
              <button
                type="button"
                className="template-btn"
                title="Prefill an entry shaped like the latest item in this stream"
                onClick={() => setWriteInput(template ?? `{}`)}
              >
                ⌁ Template
              </button>
            )}
            <button onClick={writeToStream}>▸ Send</button>
          </div>
        </>
      )}
    </div>
  )
}
