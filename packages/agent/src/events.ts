import { randomUUID } from 'node:crypto'
import type { Bus } from './bus.js'
import { sseSubject } from './bus.js'
import { logger } from './logger.js'

export interface AgentEventDeps {
  bus: Bus
}

/**
 * Best-effort durable publish with a bounded retry. JetStream publish has a 5s
 * timeout; under a transient NATS stall (leader election, slow disk) a single
 * attempt can time out. These events are fire-and-forget notifications that
 * consumers tolerate missing, but retrying once or twice recovers the common
 * transient case instead of dropping the nudge entirely (which left, e.g., a
 * forked workspace's branch unmaterialized until the next reconcile).
 */
async function publishBestEffort(
  bus: Bus,
  subject: string,
  payload: unknown,
  opts: { id: string; tenant: string },
  label: string,
  attempts = 3,
): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await bus.inboxPublish(subject, payload, opts)
      return
    } catch (err) {
      if (attempt >= attempts) {
        logger.warn({ tenant: opts.tenant, err: String(err) }, label)
        return
      }
      await new Promise(r => setTimeout(r, 100 * attempt))
    }
  }
}

/**
 * The session's ACTIVE run is now carried by the run LOCK record
 * (`session-lock.ts`, `abc-session-state`), not a separate bucket: the lock
 * already keys the session by `t.<tenant>.<token>` and holds the owner, so
 * folding `runId`/`startedAtMs` into it removes the former `abc-session-run`
 * key (and its crash-remnant/TTL problem). `readActiveRun`/`markActiveRun`
 * live in `session-lock.ts` and are re-exported through `session-state.ts`.
 */

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
 * Announce that the CURRENT step is being RETRIED after a transient provider
 * failure (mid-stream disconnect / 429 / 5xx). This is a durable, run-scoped
 * event carrying the SAME `message_id` the step already announced, so clients
 * CLEAR that streaming bubble's partial parts before the retried attempt's
 * deltas arrive — otherwise the two attempts' text would concatenate
 * ("onetwo"). No new message id is minted: the step keeps its identity, so the
 * eventual `persistStep` writes exactly ONE chain row.
 *
 * Awaited so the reset is durably ordered BEFORE the retry's first delta (the
 * caller emits it from `onError`, which the SDK awaits before re-calling the
 * provider).
 */
export async function pushRetryNow(
  bus: Bus,
  tenant: string,
  sid: string,
  messageId: string,
  attempt: number,
  delayMs: number,
  reason: string,
  runId?: string,
): Promise<void> {
  const params: Record<string, unknown> = {
    message_id: messageId,
    attempt,
    delay_ms: delayMs,
    reason,
  }
  if (runId !== undefined) params['run_id'] = runId
  try {
    await bus.inboxPublish(
      sseSubject(tenant, sid),
      { event: 'retry', params, eid: randomUUID() },
      { id: randomUUID(), tenant },
    )
  } catch (err) {
    logger.warn({ tenant, sid, err: String(err) }, 'retry publish failed')
  }
}

/**
 * Awaited variant of [pushEvent]. Ordered publication matters for the
 * session-terminal `status:idle`: it must be durably enqueued BEFORE the run
 * lease is released, otherwise a mailbox wake for the next turn can claim the
 * freed lease and emit `status:busy`, leaving this idle to land AFTER the new
 * busy — watchers then close the live turn mid-flight. Callers that need
 * ordering (not best-effort) must await this.
 */
export async function pushEventNow(
  bus: Bus,
  tenant: string,
  sid: string,
  event: string,
  params: unknown = {},
): Promise<void> {
  // Awaited AND retried: ordering matters (see above), and a transient 5s
  // JetStream publish timeout must not drop the terminal/ordering event.
  await publishBestEffort(
    bus,
    sseSubject(tenant, sid),
    { event, params, eid: randomUUID() },
    { id: randomUUID(), tenant },
    'sse publish failed',
  )
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
  void publishBestEffort(
    bus,
    sseSubject(tenant, sid),
    {
      event: 'chain-changed',
      params: { tip_id: tipId ?? '', reason },
      eid: randomUUID(),
    },
    { id: randomUUID(), tenant },
    'chain-changed publish failed',
  )
}

/**
 * Announce a message in a session's chain, carrying its SERVER-AUTHORED id and
 * chain anchor. This is the single source of truth for message identity and
 * position: clients never mint their own ids or guess the chain — they group
 * streamed deltas by `message_id` and place each message after `prev_id`.
 *
 * Emitted twice per message lifecycle:
 *   - `streaming: true`  — an assistant STEP is about to stream. Sent BEFORE
 *     any delta of that step, so a client can create the bubble under the id
 *     the deltas will carry.
 *   - `streaming: false` — the message was persisted (user prompt, or a
 *     completed step). Awaited by the caller when ordering matters.
 *
 * Run-less for user prompts (so `watchSession`'s live-run filter must exempt
 * it); assistant steps pass their `runId` so they stay scoped to the live turn.
 */
export interface MessageAddedParams {
  messageId: string
  prevId: string
  role: string
  streaming: boolean
  /** ORIGIN of the message ('' for agent-authored rows): user /
   *  session:{name} / system:{name} / extension-defined. */
  source?: string
}

/**
 * Publish a message-added event, AWAITED. Use when ordering against the
 * subsequent streamed deltas is required (assistant step start).
 */
export async function pushMessageAddedNow(
  bus: Bus,
  tenant: string,
  sid: string,
  p: MessageAddedParams,
  runId?: string,
): Promise<void> {
  const params: Record<string, unknown> = {
    message_id: p.messageId,
    prev_id: p.prevId,
    role: p.role,
    streaming: p.streaming,
    source: p.source ?? '',
  }
  if (runId !== undefined) params['run_id'] = runId
  try {
    await bus.inboxPublish(
      sseSubject(tenant, sid),
      { event: 'message-added', params, eid: randomUUID() },
      { id: randomUUID(), tenant },
    )
  } catch (err) {
    logger.warn(
      { tenant, sid, err: String(err) },
      'message-added publish failed',
    )
  }
}

/** Fire-and-forget [pushMessageAddedNow] (best-effort ordering). */
export function pushMessageAdded(
  bus: Bus,
  tenant: string,
  sid: string,
  p: MessageAddedParams,
  runId?: string,
): void {
  void pushMessageAddedNow(bus, tenant, sid, p, runId)
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
  void publishBestEffort(
    bus,
    `abc.${tenant}.session.changed`,
    { session_name: sid },
    { id: randomUUID(), tenant },
    'session-changed publish failed',
  )
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
  void publishBestEffort(
    bus,
    `abc.${tenant}.session.lifecycle.${event}`,
    {
      kind: event,
      tenant,
      ...payload,
    },
    { id: randomUUID(), tenant },
    'lifecycle publish failed',
  )
}
