import { randomUUID } from 'node:crypto'
import type { Bus } from './bus.js'
import {
  BUCKET_SESSION_RUN,
  natsToken,
  sseSubject,
  tenantKVKey,
} from './bus.js'
import { logger } from './logger.js'

export interface AgentEventDeps {
  bus: Bus
}

/**
 * Record the session's ACTIVE turn (run-id + wall-clock start). Replay uses
 * the start time to window exactly one turn (independent of how many events
 * other sessions produced); it is cleared when the turn ends.
 */
export interface ActiveRun {
  runId: string
  startedAtMs: number
}

/** Lease/active-run KV key: `t.<tenant>.<sessionToken>`. */
function runKey(tenant: string, sid: string): string {
  return tenantKVKey(tenant, natsToken(sid))
}

export function markActiveRun(
  bus: Bus,
  tenant: string,
  sid: string,
  runId: string,
  startedAtMs: number,
): void {
  const value = JSON.stringify({ runId, startedAtMs } satisfies ActiveRun)
  void bus
    .kvPut(BUCKET_SESSION_RUN, runKey(tenant, sid), value, 0)
    .catch(err => {
      logger.warn({ tenant, sid, err: String(err) }, 'markActiveRun failed')
    })
}

/** Clear the session's active-turn marker (turn ended / aborted). */
export function clearActiveRun(bus: Bus, tenant: string, sid: string): void {
  void bus.kvDelete(BUCKET_SESSION_RUN, runKey(tenant, sid)).catch(err => {
    logger.warn({ tenant, sid, err: String(err) }, 'clearActiveRun failed')
  })
}

/** Read the session's active turn, or null when idle. */
export async function readActiveRun(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<ActiveRun | null> {
  try {
    const raw = await bus.kvGet(BUCKET_SESSION_RUN, runKey(tenant, sid))
    if (raw === null || raw === '') return null
    const v = JSON.parse(raw) as Partial<ActiveRun>
    if (typeof v.runId !== 'string' || v.runId === '') return null
    return {
      runId: v.runId,
      startedAtMs:
        typeof v.startedAtMs === 'number' ? v.startedAtMs : Date.now(),
    }
  } catch {
    return null
  }
}

/**
 * Publish one SSE event for a session onto the durable stream.
 * Every event carries a unique `eid` so replay/live consumers can dedup.
 * When `runId` is given it is stamped into params so replay can filter to a
 * single turn.
 */
export function pushEvent(
  bus: Bus,
  tenant: string,
  sid: string,
  event: string,
  params: unknown = {},
  runId?: string,
): void {
  const p =
    runId !== undefined && params !== null && typeof params === 'object'
      ? { ...(params as Record<string, unknown>), run_id: runId }
      : params
  void bus
    .inboxPublish(
      sseSubject(tenant, sid),
      { event, params: p, eid: randomUUID() },
      { id: randomUUID(), tenant },
    )
    .catch(err => {
      logger.warn({ tenant, sid, err: String(err) }, 'sse publish failed')
    })
}

/**
 * Announce that a session's MESSAGE CHAIN changed out-of-band (an undo /
 * retry withdraw moved the tip backwards). Published on the same per-session
 * event stream as turn events but WITHOUT a `run_id`, so `watchSession`'s
 * live-run filter must exempt it. Every other client viewing the session
 * reacts by re-fetching the authoritative chain — this is what makes a revert
 * converge across devices.
 */
export function pushChainChanged(
  bus: Bus,
  tenant: string,
  sid: string,
  tipId: string | null,
  reason: string,
): void {
  void bus
    .inboxPublish(
      sseSubject(tenant, sid),
      {
        event: 'chain-changed',
        params: { tip_id: tipId ?? '', reason },
        eid: randomUUID(),
      },
      { id: randomUUID(), tenant },
    )
    .catch(err => {
      logger.warn(
        { tenant, sid, err: String(err) },
        'chain-changed publish failed',
      )
    })
}

export const events = {
  status: (type: string) => ({ event: 'status', params: { type } }),
  textDelta: (text: string) => ({ event: 'text-delta', params: { text } }),
  toolResult: (toolUseId: string, content: string) => ({
    event: 'tool-result',
    params: { tool_use_id: toolUseId, content },
  }),
  error: (message: string) => ({ event: 'error', params: { message } }),
  compacted: (reason: 'manual' | 'overflow') => ({
    event: 'compacted',
    params: { reason },
  }),
  turnComplete: (reason: string) => ({
    event: 'turn-complete',
    params: { reason },
  }),
}

/** Lifecycle event kinds published on `abc.<tenant>.session.lifecycle.{kind}`. */
export type LifecycleEvent = 'created' | 'forked' | 'renamed' | 'deleted'

/**
 * Announce that a session's mutable state changed (a message landed or its
 * settings were edited) so list watchers can refetch that one session.
 *
 * Durable (`inboxPublish`, same as lifecycle): the list watcher replays recent
 * events from a snapshot anchor on connect, so a nudge dropped by a transient
 * core-NATS hiccup is recovered rather than only on the next full snapshot.
 * A watcher that connects much later still gets current state from its initial
 * DB snapshot, so replay is bounded to the anchor window.
 */
export function publishSessionChanged(
  bus: Bus,
  tenant: string,
  sid: string,
): void {
  void bus
    .inboxPublish(
      `abc.${tenant}.session.changed`,
      { session_name: sid },
      { id: randomUUID(), tenant },
    )
    .catch(err => {
      logger.warn(
        { tenant, sid, err: String(err) },
        'session-changed publish failed',
      )
    })
}

/**
 * Trigger hook: after a session lifecycle action commits, notify the durable
 * stream so any service (e.g. repo-extension workspaces) can react.
 * Best-effort by design — consumers must tolerate missed events and converge
 * via their own reconciliation.
 */
export function publishLifecycle(
  bus: Bus,
  tenant: string,
  event: LifecycleEvent,
  payload: Record<string, unknown>,
): void {
  void bus
    .inboxPublish(
      `abc.${tenant}.session.lifecycle.${event}`,
      {
        kind: event,
        tenant,
        ...payload,
      },
      { id: randomUUID(), tenant },
    )
    .catch(err => {
      logger.warn(
        { tenant, event, err: String(err) },
        'lifecycle publish failed',
      )
    })
}
