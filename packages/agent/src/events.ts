import { randomUUID } from 'node:crypto'
import type { Bus } from './bus.js'
import { BUCKET_SESSION_RUN, natsToken, sseSubject } from './bus.js'
import { logger } from './logger.js'

export interface AgentEventDeps {
  bus: Bus
}

/**
 * Record the session's ACTIVE turn run-id (best-effort). Replay uses this to
 * hand back only the live turn; it is cleared when the turn ends.
 */
export function markActiveRun(bus: Bus, sid: string, runId: string): void {
  void bus
    .kvPut(BUCKET_SESSION_RUN, natsToken(sid), runId, 0)
    .catch(err => {
      logger.warn({ sid, err: String(err) }, 'markActiveRun failed')
    })
}

/** Clear the session's active-turn marker (turn ended / aborted). */
export function clearActiveRun(bus: Bus, sid: string): void {
  void bus
    .kvDelete(BUCKET_SESSION_RUN, natsToken(sid))
    .catch(err => {
      logger.warn({ sid, err: String(err) }, 'clearActiveRun failed')
    })
}

/** Read the session's active-turn run-id, or null when idle. */
export async function readActiveRun(
  bus: Bus,
  sid: string,
): Promise<string | null> {
  try {
    return await bus.kvGet(BUCKET_SESSION_RUN, natsToken(sid))
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
      sseSubject(sid),
      { event, params: p, eid: randomUUID() },
      { id: randomUUID() },
    )
    .catch(err => {
      logger.warn({ sid, err: String(err) }, 'sse publish failed')
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

/** Lifecycle event kinds published on `abc.session.lifecycle.{kind}`. */
export type LifecycleEvent = 'created' | 'forked' | 'renamed' | 'deleted'

/**
 * Trigger hook: after a session lifecycle action commits, notify the durable
 * stream so any service (e.g. repo-extension workspaces) can react.
 * Best-effort by design — consumers must tolerate missed events and converge
 * via their own reconciliation.
 */
export function publishLifecycle(
  bus: Bus,
  event: LifecycleEvent,
  payload: Record<string, unknown>,
): void {
  void bus
    .inboxPublish(
      `abc.session.lifecycle.${event}`,
      {
        kind: event,
        ...payload,
      },
      { id: randomUUID() },
    )
    .catch(err => {
      logger.warn({ event, err: String(err) }, 'lifecycle publish failed')
    })
}
