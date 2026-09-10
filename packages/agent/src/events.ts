import { randomUUID } from 'node:crypto'
import type { Bus } from './bus.js'
import { BUCKET_SESSION_RUN, natsToken, sseSubject } from './bus.js'
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

export function markActiveRun(
  bus: Bus,
  sid: string,
  runId: string,
  startedAtMs: number,
): void {
  const value = JSON.stringify({ runId, startedAtMs } satisfies ActiveRun)
  void bus
    .kvPut(BUCKET_SESSION_RUN, natsToken(sid), value, 0)
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

/** Read the session's active turn, or null when idle. */
export async function readActiveRun(
  bus: Bus,
  sid: string,
): Promise<ActiveRun | null> {
  try {
    const raw = await bus.kvGet(BUCKET_SESSION_RUN, natsToken(sid))
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

const SESSION_CHANGED_SUBJECT = 'abc.session.changed'

/**
 * Announce that a session's mutable state changed (a message landed or its
 * settings were edited) so list watchers can refetch that one session. A live
 * `pub` (not durable): a watcher that connects later gets current state from
 * its initial snapshot, so only connected watchers need the nudge.
 */
export function publishSessionChanged(bus: Bus, sid: string): void {
  void bus
    .publish(SESSION_CHANGED_SUBJECT, { session_name: sid })
    .catch(err => {
      logger.warn({ sid, err: String(err) }, 'session-changed publish failed')
    })
}

export const SESSION_CHANGED = SESSION_CHANGED_SUBJECT

/** Lifecycle event kinds published on `abc.session.lifecycle.{kind}`. */

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
