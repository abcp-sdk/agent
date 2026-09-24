import { APICallError, StreamProviderError, ToolChoiceViolationError } from 'ai'

/**
 * Provider-retry classification + backoff for the agent turn loop.
 *
 * The AI SDK already retries:
 *   - request-start failures via `maxRetries` (it honors `isRetryable` and the
 *     `Retry-After` / `retry-after-ms` headers), and
 *   - mid-stream provider ERROR EVENTS via `streamRetries` (it re-runs only the
 *     current step and DISCARDS the failed attempt's tool parts, so tools never
 *     re-execute).
 *
 * What escapes the SDK is a raw TRANSPORT throw while reading the stream (a
 * dropped socket / ECONNRESET / `fetch failed`) — it is not a provider error
 * event, so neither budget covers it. This module classifies those thrown
 * errors and computes a backoff so the turn loop can retry them itself.
 */

/** Initial backoff before the first retry. */
export const RETRY_INITIAL_DELAY_MS = 2_000
/** Exponential factor between attempts. */
export const RETRY_BACKOFF_FACTOR = 2
/** Jitter fraction (0..1) applied to the exponential delay. */
export const RETRY_JITTER_FACTOR = 0.25
/** Cap for the computed exponential delay (header delays are honored up to
 *  the 32-bit setTimeout ceiling instead). */
export const RETRY_MAX_DELAY_NO_HEADERS_MS = 30_000
/** setTimeout ceiling (32-bit signed). */
const RETRY_MAX_DELAY_MS = 2_147_483_647

/** HTTP statuses the provider SDKs treat as transient. */
function isRetryableStatus(status: number | undefined): boolean {
  return (
    status !== undefined &&
    (status === 408 || status === 409 || status === 429 || status >= 500)
  )
}

/** Network/transport error message patterns worth retrying. */
const RETRYABLE_MESSAGE_PATTERNS = [
  /\bterminated\b|fetch failed|failed to fetch/i,
  /socket hang up|socket connection was closed|connection (?:reset|refused|lost)/i,
  /\beconnreset\b|\beconnrefused\b|\betimedout\b|\bepipe\b|\benotfound\b|\beai_again\b/i,
  /reset before headers|upstream connect|network[-_\s]error/i,
  /\b(?:request|response|connection|network|stream|read) (?:timeout|timed out|time out)\b/i,
]

function messageOf(err: unknown): string {
  if (typeof err === 'string') return err
  if (err instanceof Error) return err.message
  if (err !== null && typeof err === 'object' && 'message' in err) {
    return String((err as { message: unknown }).message)
  }
  return ''
}

/**
 * True when a THROWN error (one that escaped the SDK's own retry budgets) is
 * worth retrying at the turn level. Deliberately conservative: a provider error
 * event (handled by `streamRetries`) is not classified here.
 */
export function isRetryableThrown(err: unknown): boolean {
  if (err === null || err === undefined) return false
  // The SDK's own error classes carry the authoritative flag.
  if (APICallError.isInstance(err)) return err.isRetryable === true
  if (StreamProviderError.isInstance(err)) return err.isRetryable === true
  const msg = messageOf(err)
  if (isRetryableStatus(statusOf(err))) return true
  return RETRYABLE_MESSAGE_PATTERNS.some(p => p.test(msg))
}

/** Extract an HTTP status from an error shape, when present. */
function statusOf(err: unknown): number | undefined {
  if (err === null || typeof err !== 'object') return undefined
  const o = err as Record<string, unknown>
  const raw = o['statusCode'] ?? o['status'] ?? o['code']
  const n = typeof raw === 'number' ? raw : Number.parseInt(String(raw), 10)
  return Number.isFinite(n) ? n : undefined
}

/** Read `retry-after` / `retry-after-ms` off an error's response headers. */
function headerDelayMs(err: unknown): number | undefined {
  if (err === null || typeof err !== 'object') return undefined
  const o = err as Record<string, unknown>
  const cause =
    o['cause'] !== null && typeof o['cause'] === 'object'
      ? (o['cause'] as Record<string, unknown>)
      : undefined
  const headers = (o['responseHeaders'] ?? cause?.['responseHeaders']) as
    | Record<string, string>
    | undefined
  if (headers === undefined || headers === null) return undefined
  const ms = headers['retry-after-ms']
  if (ms !== undefined) {
    const n = Number.parseFloat(ms)
    if (!Number.isNaN(n) && n >= 0) return n
  }
  const after = headers['retry-after']
  if (after !== undefined) {
    const secs = Number.parseFloat(after)
    if (!Number.isNaN(secs) && secs >= 0) return Math.ceil(secs * 1000)
    const at = Date.parse(after) - Date.now()
    if (!Number.isNaN(at) && at > 0) return at
  }
  return undefined
}

/**
 * Backoff before retry attempt [attempt] (1-based). Honors a `Retry-After`
 * header when present; otherwise exponential + jitter, capped at 30s (header
 * delays are honored up to the setTimeout ceiling). [random] is injectable for
 * deterministic tests.
 */
export function retryDelayMs(
  attempt: number,
  err?: unknown,
  random: number = Math.random(),
): number {
  const header = headerDelayMs(err)
  if (header !== undefined) return Math.min(header, RETRY_MAX_DELAY_MS)
  const base =
    RETRY_INITIAL_DELAY_MS * RETRY_BACKOFF_FACTOR ** Math.max(0, attempt - 1)
  const jittered = base + base * RETRY_JITTER_FACTOR * random
  return Math.min(Math.ceil(jittered), RETRY_MAX_DELAY_NO_HEADERS_MS)
}

/** True when [err] is a tool-choice violation (never retried by the SDK). */
export function isToolChoiceViolation(err: unknown): boolean {
  return ToolChoiceViolationError.isInstance(err)
}
