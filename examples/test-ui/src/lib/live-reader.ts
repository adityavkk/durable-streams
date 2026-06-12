import { stream as openStream } from "@durable-streams/client"
import type { DurableStream } from "@durable-streams/client"

/**
 * Instrumented stream reader for the Durable Streams test UI.
 *
 * Replaces the old naive `streamStore` follow loop with a reader that
 * records every chunk and every HTTP request (via a fetch wrapper), and
 * exposes a stable snapshot compatible with React's useSyncExternalStore.
 *
 * Note: `DurableStream#stream()` ignores a per-call `fetch` option (it always
 * forwards the handle-level fetch), so the follow loop uses the standalone
 * `stream()` function from @durable-streams/client, which does honor
 * `options.fetch`. Test-ui handles are constructed with only `url` (and
 * `contentType`), so no handle-level headers/params are lost.
 */

export type ReaderStatus =
  | `connecting`
  | `catching-up`
  | `live`
  | `offline`
  | `error`

export interface ChunkRecord {
  /** Raw text of the chunk */
  data: string
  /** Next-offset AFTER this chunk (the resume point) */
  offset: string
  /** Epoch-ms timestamp when received */
  at: number
  /** data.length */
  bytes: number
  /** Stream was up-to-date as of this chunk */
  upToDate: boolean
}

export interface RequestRecord {
  id: number
  /** ?offset= param of the GET, null if absent */
  offsetParam: string | null
  /** Best-effort: ?live= param present on the GET */
  live: boolean
  /** Epoch ms */
  startedAt: number
  /** Epoch ms; null while in flight */
  endedAt: number | null
  /** HTTP status; null while in flight or on network error */
  status: number | null
  outcome: `pending` | `data` | `empty` | `error` | `aborted`
  /** Content-Length if present, else -1 */
  bytes: number
  /** Stream-Up-To-Date response header === 'true' */
  upToDate: boolean
}

export interface ReaderSnapshot {
  chunks: Array<ChunkRecord>
  /** Oldest first, capped at 100 */
  requests: Array<RequestRecord>
  status: ReaderStatus
  lastError: string | null
}

const MAX_REQUESTS = 100
const START_OFFSET = `-1`

const isAbortError = (err: unknown): boolean => {
  if (!(err instanceof Error)) return false
  if (err.name === `AbortError`) return true
  const message = err.message.toLowerCase()
  return message.includes(`abort`)
}

const parseRequestUrl = (urlString: string): URL | null => {
  try {
    return new URL(urlString)
  } catch {
    try {
      // Relative URL — base only used to make searchParams parseable.
      return new URL(urlString, `http://relative.invalid`)
    } catch {
      return null
    }
  }
}

export class LiveReader {
  readonly #stream: DurableStream
  readonly #streamPath: string
  readonly #listeners = new Set<() => void>()
  #abortController: AbortController | null = null
  #nextRequestId = 1

  #snapshot: ReaderSnapshot = {
    chunks: [],
    requests: [],
    status: `connecting`,
    lastError: null,
  }

  constructor(stream: DurableStream, streamPath: string) {
    this.#stream = stream
    this.#streamPath = streamPath
  }

  /**
   * Register a listener. Lazily starts the follow loop on first subscriber.
   * Deliberate `offline`/`error` states are NOT auto-resumed — use resume().
   */
  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener)

    const status = this.#snapshot.status
    if (
      this.#abortController === null &&
      status !== `offline` &&
      status !== `error`
    ) {
      this.#start(this.#resumeOffset())
    }

    return () => {
      this.#listeners.delete(listener)
      // Pause the follow loop when the last listener leaves: one parked
      // long-poll per visited stream would pin one of the browser's ~6
      // HTTP/1.1 connections to the server forever. Chunks and the resume
      // offset are retained, so re-subscribing catches up instantly.
      if (this.#listeners.size === 0 && this.#abortController !== null) {
        const requests = this.#stopLoop()
        this.#commit({ requests })
      }
    }
  }

  /**
   * Stable snapshot for useSyncExternalStore: the same object reference is
   * returned until state actually changes.
   */
  getSnapshot(): ReaderSnapshot {
    return this.#snapshot
  }

  /**
   * Abort the follow loop. Retains chunks/requests and the resume offset.
   */
  disconnect(): void {
    const requests = this.#stopLoop()
    this.#commit({ requests, status: `offline` })
  }

  /**
   * Re-follow from the last chunk's offset (or `-1` if no chunks).
   * No-op unless currently offline or errored.
   */
  resume(): void {
    const status = this.#snapshot.status
    if (status !== `offline` && status !== `error`) return
    this.#commit({ status: `connecting`, lastError: null })
    this.#start(this.#resumeOffset())
  }

  /**
   * Abort the current loop, clear chunks (requests are kept) and follow
   * again from the given offset.
   */
  restartFrom(offset: string): void {
    const requests = this.#stopLoop()
    this.#commit({
      chunks: [],
      requests,
      status: `connecting`,
      lastError: null,
    })
    this.#start(offset)
  }

  /**
   * Abort the follow loop and remove this reader from the registry.
   */
  dispose(): void {
    const requests = this.#stopLoop()
    this.#commit({ requests, status: `offline` })
    this.#listeners.clear()
    registry.delete(this.#streamPath)
  }

  // ==========================================================================
  // Internals
  // ==========================================================================

  #resumeOffset(): string {
    const chunks = this.#snapshot.chunks
    return chunks.length > 0 ? chunks[chunks.length - 1].offset : START_OFFSET
  }

  #start(offset: string): void {
    const controller = new AbortController()
    this.#abortController = controller
    void this.#follow(offset, controller)
  }

  async #follow(offset: string, controller: AbortController): Promise<void> {
    try {
      const response = await openStream({
        url: this.#stream.url,
        offset,
        live: `long-poll`,
        signal: controller.signal,
        fetch: this.#instrumentedFetch,
      })

      const isJson =
        this.#stream.contentType?.includes(`application/json`) ?? false

      response.subscribeText((chunk) => {
        // Drop chunks from a superseded loop: an in-flight response body
        // can still deliver after restartFrom()/disconnect() cleared or
        // froze the view.
        if (controller.signal.aborted || this.#abortController !== controller) {
          return
        }
        // JSON-mode no-data responses carry a literal `[]` body (chronicle
        // returns 200 with it instead of a 204). Treat it as an up-to-date
        // signal, not data — otherwise phantom chunks pollute the timeline.
        if (chunk.text === `` || (isJson && chunk.text.trim() === `[]`)) {
          if (chunk.upToDate) this.#deriveStatus(true)
          return
        }
        this.#pushChunk({
          data: chunk.text,
          offset: chunk.offset,
          at: Date.now(),
          bytes: chunk.text.length,
          upToDate: chunk.upToDate,
        })
      })

      // Resolves on cancellation/stream close; rejects on terminal errors.
      await response.closed
    } catch (err) {
      // Swallow abort errors — expected on disconnect/restart/dispose.
      if (!controller.signal.aborted && !isAbortError(err)) {
        this.#fail(err)
      }
    } finally {
      if (this.#abortController === controller) {
        this.#abortController = null
      }
    }
  }

  /**
   * Wraps globalThis.fetch to record a RequestRecord per network attempt.
   * The original response is returned untouched — its body is consumed by
   * the streaming client, so it is never read or cloned here.
   */
  readonly #instrumentedFetch: typeof globalThis.fetch = async (
    input,
    init
  ) => {
    const id = this.#nextRequestId++

    let urlString: string
    if (typeof input === `string`) {
      urlString = input
    } else if (input instanceof URL) {
      urlString = input.toString()
    } else {
      urlString = input.url
    }

    const parsed = parseRequestUrl(urlString)
    const record: RequestRecord = {
      id,
      offsetParam: parsed?.searchParams.get(`offset`) ?? null,
      live: parsed?.searchParams.has(`live`) ?? false,
      startedAt: Date.now(),
      endedAt: null,
      status: null,
      outcome: `pending`,
      bytes: -1,
      upToDate: false,
    }
    this.#pushRequest(record)

    try {
      const response = await globalThis.fetch(input, init)

      const contentLength = response.headers.get(`content-length`)
      let bytes = -1
      if (contentLength !== null) {
        const parsedLength = Number.parseInt(contentLength, 10)
        bytes = Number.isNaN(parsedLength) ? -1 : parsedLength
      }
      // Presence header per the protocol (any value means up-to-date) —
      // matches how the client itself reads it.
      const upToDate = response.headers.has(`Stream-Up-To-Date`)

      const isJsonBody = (response.headers.get(`content-type`) ?? ``).includes(
        `application/json`
      )

      let outcome: RequestRecord[`outcome`]
      if (response.status >= 400) {
        outcome = `error`
      } else if (
        response.status === 204 ||
        bytes === 0 ||
        // JSON-mode no-data responses are 200 with a literal 2-byte `[]`
        // body; the smallest possible response WITH data is larger.
        (isJsonBody && bytes === 2)
      ) {
        outcome = `empty`
      } else {
        outcome = `data`
      }

      this.#completeRequest(
        id,
        {
          endedAt: Date.now(),
          status: response.status,
          bytes,
          upToDate,
          outcome,
        },
        outcome === `error` ? null : upToDate
      )

      return response
    } catch (err) {
      const aborted = (init?.signal?.aborted ?? false) || isAbortError(err)
      if (aborted) {
        this.#completeRequest(
          id,
          { endedAt: Date.now(), outcome: `aborted` },
          null
        )
      } else {
        const message = err instanceof Error ? err.message : String(err)
        this.#completeRequest(
          id,
          { endedAt: Date.now(), outcome: `error` },
          null,
          message
        )
        // The client's backoff retries network failures indefinitely, so a
        // total outage never rejects the follow loop. Reflect it honestly:
        // we are (re)connecting, not live.
        this.#noteRetrying()
      }
      throw err
    }
  }

  #pushChunk(chunk: ChunkRecord): void {
    const next: Partial<ReaderSnapshot> = {
      chunks: [...this.#snapshot.chunks, chunk],
    }
    const derived = this.#derivedStatus(chunk.upToDate)
    if (derived !== null && derived !== this.#snapshot.status) {
      next.status = derived
    }
    this.#commit(next)
  }

  #pushRequest(record: RequestRecord): void {
    const requests = [...this.#snapshot.requests, record]
    if (requests.length > MAX_REQUESTS) {
      requests.splice(0, requests.length - MAX_REQUESTS)
    }
    this.#commit({ requests })
  }

  /**
   * Patch a pending RequestRecord (immutably) and optionally derive status
   * from the response's up-to-date flag and record an error message.
   * Records that are no longer pending (e.g. marked aborted by disconnect)
   * are left untouched.
   */
  #completeRequest(
    id: number,
    patch: Partial<RequestRecord>,
    upToDateForStatus: boolean | null,
    lastError?: string
  ): void {
    const index = this.#snapshot.requests.findIndex(
      (record) => record.id === id && record.outcome === `pending`
    )

    const next: Partial<ReaderSnapshot> = {}
    if (index !== -1) {
      const requests = [...this.#snapshot.requests]
      requests[index] = { ...requests[index], ...patch }
      next.requests = requests
    }
    if (upToDateForStatus !== null) {
      const derived = this.#derivedStatus(upToDateForStatus)
      if (derived !== null && derived !== this.#snapshot.status) {
        next.status = derived
      }
    }
    if (lastError !== undefined) {
      next.lastError = lastError
    }
    if (Object.keys(next).length > 0) {
      this.#commit(next)
    }
  }

  /**
   * `live` once the latest chunk/request reports up-to-date, otherwise
   * `catching-up`. Never resurrects deliberate `offline`/`error` states.
   */
  #derivedStatus(upToDate: boolean): ReaderStatus | null {
    const status = this.#snapshot.status
    if (status === `offline` || status === `error`) return null
    return upToDate ? `live` : `catching-up`
  }

  #deriveStatus(upToDate: boolean): void {
    const derived = this.#derivedStatus(upToDate)
    if (derived !== null && derived !== this.#snapshot.status) {
      this.#commit({ status: derived })
    }
  }

  /**
   * Reflect a retried network failure as `connecting` (the client's backoff
   * keeps polling). A later successful response re-derives live/catching-up.
   */
  #noteRetrying(): void {
    const status = this.#snapshot.status
    if (status === `offline` || status === `error`) return
    if (status !== `connecting`) {
      this.#commit({ status: `connecting` })
    }
  }

  #fail(err: unknown): void {
    if (this.#snapshot.status === `offline`) return
    const message = err instanceof Error ? err.message : String(err)
    // A 404 means the stream is gone (deleted, possibly recreated later):
    // drop its chunks so a future resume() re-reads from `-1` instead of a
    // foreign offset belonging to the dead incarnation.
    const gone =
      (err as { status?: unknown }).status === 404 || message.includes(`404`)
    this.#commit({
      status: `error`,
      lastError: message,
      ...(gone ? { chunks: [] } : {}),
    })
  }

  /**
   * Abort the current loop and mark any pending requests as aborted.
   * Returns the patched requests array; callers commit it themselves so a
   * status change lands in the same snapshot.
   */
  #stopLoop(): Array<RequestRecord> {
    const controller = this.#abortController
    this.#abortController = null
    controller?.abort()

    const now = Date.now()
    return this.#snapshot.requests.map((record) =>
      record.outcome === `pending`
        ? { ...record, outcome: `aborted` as const, endedAt: now }
        : record
    )
  }

  /**
   * Replace the cached snapshot with a new object (new array references for
   * whatever changed) and notify listeners — useSyncExternalStore contract.
   */
  #commit(patch: Partial<ReaderSnapshot>): void {
    this.#snapshot = { ...this.#snapshot, ...patch }
    for (const listener of [...this.#listeners]) {
      listener()
    }
  }
}

// ============================================================================
// Registry — one LiveReader per streamPath, kept alive across navigation
// ============================================================================

const registry = new Map<string, LiveReader>()

export function getReader(
  streamPath: string,
  stream: DurableStream
): LiveReader {
  const existing = registry.get(streamPath)
  if (existing) return existing
  const reader = new LiveReader(stream, streamPath)
  registry.set(streamPath, reader)
  return reader
}
