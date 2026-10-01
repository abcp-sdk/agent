import { randomUUID } from 'node:crypto'
import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  InvalidToolInputError,
  jsonSchema,
  NoSuchToolError,
  streamText,
  type Tool,
} from 'ai'
import type { Bus } from './bus.js'
import { mailboxSubject, SESSION_LEASE_MS } from './bus.js'
import type { ServerConfig } from './config.js'
import { isContextOverflowFailure } from './context-overflow.js'
import type { Db } from './db-client.js'
import { Mailbox } from './db-mailbox.js'
import { Sessions } from './db-sessions.js'
import {
  events,
  pushEvent,
  pushEventNow,
  pushMessageAddedNow,
  pushRetryNow,
} from './events.js'
import type { BlobStore } from './files.js'
import {
  abortReason,
  abortRun,
  clearRun,
  getAbortController,
} from './interrupt.js'
import { parse, type ToolResult, WakePayloadSchema } from './json.js'
import type { LlmRegistry } from './llm.js'
import {
  isRetryableThrown,
  isToolChoiceViolation,
  retryDelayMs,
} from './llm-retry.js'
import { logger } from './logger.js'
import { compactSession } from './session-compact.js'
import {
  type ClaimOutcome,
  claimLeaseOutcome,
  releaseLease,
  renewLease,
  updateLease,
} from './session-lock.js'
import { setTurnEnd } from './session-state.js'
import { type SanitizeDeps, sanitizeStreamPart } from './stream-parts.js'
import {
  appendStep,
  drainAll,
  drainAndInject,
  type FilePartRec,
  loadHistory,
  persistStep,
  processBatch,
  type ToolCallRec,
  type ToolResultRec,
} from './turn-persist.js'
import { prepare } from './turn-prepare.js'

export interface AgentDeps {
  db: Db
  bus: Bus
  config: ServerConfig
  llm: LlmRegistry
  files: BlobStore
  /**
   * The LONG-LIVED abc agent role. It owns the manifest cache and the
   * config authority (see `AbcAgent.serveConfig()`), so config writes must go
   * through this ONE instance — a per-request `new AbcAgent(bus)` starts with
   * an empty manifest cache and an unstarted authority, which made
   * `SetExtensionConfig` fail with an internal error.
   */
  agent?: AbcAgent
}

const DRAIN_GRACE_MS = 200

/**
 * Mailbox message types that WAKE the session turn loop.
 *
 *   - `trigger` — a text prompt; drives a model turn.
 *   - `compact` — a manual compaction; must run at a step boundary UNDER the
 *     lease, so an idle session has to be woken or the request never executes.
 *
 * Every OTHER type (`event`, …) is CONTEXT-ONLY: it is persisted and folded
 * lazily by the next trigger, or by a running turn's step boundary. Waking the
 * turn loop for a context-only message would claim the lease, drain it, find no
 * trigger, and emit a terminal `idle` with no preceding `busy` — pure status
 * noise (and a needless lease flap across replicas).
 */
export const WAKE_MAILBOX_TYPES: readonly string[] = ['trigger', 'compact']

/** True when a mailbox message type should wake the session's turn loop. */
export function shouldWakeMailbox(type: string): boolean {
  return WAKE_MAILBOX_TYPES.includes(type)
}

/**
 * Renew a held lock, classifying the outcome so the turn loop aborts ONLY on a
 * definitive loss.
 *
 * The lock key carries a TTL fixed at bucket creation; a long turn can outlive
 * it. The renew is `renewLease`, which is tri-state:
 *   - `held`  → still ours (TTL refreshed, or re-created after a bare expiry)
 *   - `lost`  → the key is owned by ANOTHER instance → abort the turn
 *   - `error` → transient KV/network failure → NOT a loss; retry next tick
 *
 * A thrown renew is itself treated as `error` (transient), never a loss.
 * Exported for unit testing (the timer wiring itself is hard to exercise).
 */
export async function renewOrReclaim(
  renew: () => Promise<'held' | 'lost' | 'error'>,
): Promise<{ held: boolean; lost: boolean }> {
  let outcome: 'held' | 'lost' | 'error'
  try {
    outcome = await renew()
  } catch {
    outcome = 'error'
  }
  if (outcome === 'held') return { held: true, lost: false }
  if (outcome === 'lost') return { held: false, lost: true }
  // Transient: keep the turn running; the next tick retries.
  return { held: true, lost: false }
}

/** Attempts for the initial/re-claim before giving up on a transient error. */
export const CLAIM_MAX_ATTEMPTS = 6
/** First backoff delay for a transient claim error (exponential, capped 1s). */
export const CLAIM_RETRY_BASE_MS = 50

/**
 * Claim with bounded exponential backoff on a TRANSIENT claim error. Unlike a
 * bare `claimLease`, a transient KV/JetStream failure is retried rather than
 * collapsed into "busy" — the latter silently dropped a wake whose message row
 * stayed pending until an unrelated event happened to re-trigger the session.
 * Never throws; returns the last outcome (`error` after exhausting attempts).
 *
 * Exported for unit testing (the defaults are overridable to keep tests fast).
 */
export async function claimWithRetry(
  bus: Bus,
  tenant: string,
  sid: string,
  opts: { maxAttempts?: number; baseDelayMs?: number } = {},
): Promise<ClaimOutcome> {
  const maxAttempts = opts.maxAttempts ?? CLAIM_MAX_ATTEMPTS
  const baseDelayMs = opts.baseDelayMs ?? CLAIM_RETRY_BASE_MS
  let outcome: ClaimOutcome = 'error'
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    outcome = await claimLeaseOutcome(bus, tenant, sid)
    if (outcome !== 'error') return outcome
    if (attempt < maxAttempts - 1 && baseDelayMs > 0) {
      await sleep(Math.min(1000, baseDelayMs * 2 ** attempt))
    }
  }
  return outcome
}

/**
 * Injected as the final assistant turn when the step budget is exhausted, so
 * the model produces a TEXT wrap-up instead of the loop silently ending
 * mid-task (the previous behaviour: `while (step < maxTurns)` just fell out
 * with no explanation to the model OR the user).
 */
const MAX_STEPS_PROMPT =
  'CRITICAL - MAXIMUM STEPS REACHED\n\n' +
  'The maximum number of steps allowed for this task has been reached. ' +
  'Tools are disabled until the next user input. Respond with text only.\n\n' +
  'STRICT REQUIREMENTS:\n' +
  '1. Do NOT make any tool calls.\n' +
  '2. MUST provide a text response summarizing work done so far.\n' +
  '3. This overrides ALL other instructions, including user requests for edits or tool use.\n\n' +
  'Include: that the maximum steps were reached; what was accomplished; ' +
  'what remains incomplete; and recommended next steps.'

/**
 * Watch the durable mailbox queue (`mailbox.session.>`): each replica joins
 * the same durable consumer + queue group, so every message is delivered to
 * exactly one replica. The handler parses the envelope, persists it into the
 * PG `mailbox` table idempotently (producer id = row id), triggers the
 * session turn, and acks. Bad envelopes are Term-ed; transient PG failures
 * are Nak-ed for redelivery.
 *
 * Returns an unsubscribe function.
 */
export function watchMailboxWake(deps: AgentDeps): () => void {
  let stopped = false
  let stop: (() => void | Promise<void>) | null = null
  const agent = deps.agent ?? new AbcAgent(deps.bus)
  void agent
    .consumeMailbox(async msg => {
      if (stopped) return
      await handleMailboxMessage(deps, msg.tenant, msg)
    })
    .then(
      shutdown => {
        stop = shutdown
      },
      err => {
        logger.error({ err: String(err) }, 'mailbox consumer failed')
      },
    )
  return () => {
    stopped = true
    void stop?.()
  }
}

/**
 * Persist one durable mailbox message into PG, then wake the session's turn
 * loop. Redelivery-safe: the envelope id is the PG row id, so a re-delivered
 * message inserts no duplicate row. Exported for tests.
 */
export async function handleMailboxMessage(
  deps: AgentDeps,
  tenant: string,
  msg: {
    id: string
    sessionName: string
    type: string
    payload?: unknown
    source?: string
  },
): Promise<void> {
  const env = {
    // Defensive: a wake-style message without a producer id would fail the
    // uuid-typed PG key on '' and poison the consumer; mint one instead.
    id: msg.id || randomUUID(),
    session_name: msg.sessionName,
    type: msg.type,
    payload: msg.payload,
    source: msg.source ?? '',
  }

  const enq = await Mailbox.enqueueIdempotent(
    deps.db,
    tenant,
    env.id,
    env.session_name,
    env.type,
    env.payload,
    env.source,
  )
  if (enq.isErr()) {
    if (isForeignKeyViolation(enq.error)) {
      logger.warn(
        { tenant, sid: env.session_name },
        'mailbox: session missing — discarding',
      )
    } else {
      logger.warn(
        { tenant, sid: env.session_name, err: String(enq.error) },
        'mailbox: enqueue failed — redelivering',
      )
      throw enq.error
    }
    return
  }

  // Context-only messages (e.g. `event`) are persisted but do NOT wake the turn
  // loop: they fold lazily on the next trigger or a running turn's step
  // boundary. Waking here would claim the lease and emit a spurious idle.
  if (!shouldWakeMailbox(env.type)) return

  void runSessionTurn(deps, tenant, env.session_name).then(
    () => {},
    e =>
      logger.error(
        { tenant, sid: env.session_name, err: String(e) },
        'turn crashed',
      ),
  )
}

/**
 * Claim the per-session run lease (cross-replica), drain the mailbox to
 * completion, then release. The loop re-claims after releasing whenever a
 * final drain still finds work, closing the race where a message arrives just
 * as the lease is released. The durable wake signal remains as a cold-start
 * backstop; exactly one replica wins any given claim.
 *
 * The lease is held CONTINUOUSLY across a whole busy period (all chained runs)
 * and released exactly ONCE, at the end. The old code released + re-claimed
 * between chained runs, which flapped the lease key delete→create and made
 * WatchSessions emit spurious idle→busy transitions; a single claim/release
 * keeps the busy/idle signal stable.
 */
export async function runSessionTurn(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<void> {
  for (;;) {
    // Bounded-retry a TRANSIENT claim error; a definitive `busy` means another
    // replica owns the session and will drain our message.
    const claim = await claimWithRetry(deps.bus, tenant, sid)
    if (claim === 'error') {
      // Still failing after retries: leave the message PENDING (never consumed
      // here) so boot recovery / the next wake retries it. Do not silently drop.
      logger.error(
        { tenant, sid },
        'session claim failed after retries — leaving mailbox pending',
      )
      return
    }
    if (claim === 'busy') {
      // Another replica is running this session; it will drain our message.
      return
    }

    // Renew the lock on a timer so a long-running turn — the drain loop awaits
    // handleItem for minutes at a time — cannot be re-claimed by a competing
    // replica after the TTL lapses. `updateLease` is an OWNER-GUARDED
    // read-modify-write that refreshes the TTL and returns false when the lock
    // is gone or owned by another instance. A single failure must NOT poison
    // the lock forever, so on failure we RE-CLAIM:
    //   - re-claim succeeds → the lock had merely expired (e.g. an event-loop
    //     stall exceeded the TTL); keep running.
    //   - re-claim fails → another replica genuinely owns the session; abort the
    //     in-flight turn so we stop writing the chain without a lock.
    //
    // The interval is TTL/4 (not TTL/3): the KV bucket's max_age is fixed at
    // creation (30s) and is NOT changed by SESSION_LEASE_MS, so renewing at
    // TTL/3 left only a single-renew margin — one event-loop stall and the key
    // expired. TTL/4 gives two renew attempts before expiry.
    let leaseLost = false
    const renewTimer = setInterval(() => {
      void renewOrReclaim(() => renewLease(deps.bus, tenant, sid)).then(
        ({ lost }) => {
          if (!lost) return
          // A DEFINITIVE loss only: the lock is owned by another instance.
          // Transient renew failures are folded to `held` (above) and retried
          // on the next tick, so a NATS blip can never abort a live turn.
          logger.warn(
            { tenant, sid },
            'lock lost: another replica owns it — aborting turn',
          )
          leaseLost = true
          // Abort the in-flight turn so it stops emitting/writing without a
          // lock. Tag the abort `locklost` so the turn-end outcome is NOT
          // recorded as a user interrupt (idlewatch resumes those).
          abortRun(tenant, sid, 'locklost')
        },
        err => {
          logger.warn({ tenant, sid, err: String(err) }, 'renew session failed')
        },
      )
    }, SESSION_LEASE_MS / 4)

    // Drain EVERY pending item while holding the lock, then process the whole
    // batch together: all triggers/events are folded into the chain IN ARRIVAL
    // ORDER, and the batch runs as ONE turn (busy → … → turn-complete). There
    // is NO idle between the batched messages — a mailbox continuation is the
    // SAME busy period ("跑完之后 consume mailbox 应延续 busy，不发 idle"). The
    // lock is held across the whole loop, so the busy signal never flaps.
    try {
      for (;;) {
        if (leaseLost) break
        let items = await drainAll(deps, tenant, sid)
        if (items.length === 0) {
          // Re-drain after a short grace to close the enqueue/drain race.
          await sleep(DRAIN_GRACE_MS)
          items = await drainAll(deps, tenant, sid)
          if (items.length === 0) {
            // Non-consuming last look: a message may have landed in the window
            // since the drain. If so, keep holding the lock and process it — no
            // spurious idle→busy flap. Only a genuinely empty queue ends the
            // busy period.
            const stillPending = await Mailbox.hasPending(
              deps.db,
              tenant,
              sid,
            ).unwrapOr(false)
            if (stillPending) continue
            // No work remains and we still hold the lock: emit the terminal idle
            // BEFORE releasing. Holding the lock while emitting makes "busy for
            // the next run" and "idle for this one" mutually exclusive; awaiting
            // the publish removes the reorder window (the client never sees an
            // idle land after a newer run's busy).
            await pushEventNow(deps.bus, tenant, sid, 'status', {
              type: 'idle',
            })
            break
          }
        }
        await handleBatch(deps, tenant, sid, items)
      }
    } finally {
      clearInterval(renewTimer)
      // Only release a lock we still hold. After a lost lock another replica
      // owns the key; deleting it would clobber THEIR lock and let a third
      // writer in.
      if (!leaseLost) await releaseLease(deps.bus, tenant, sid)
    }

    // Lock lost mid-drain: another replica owns the session now. It will
    // process remaining work and emit the terminal idle; we must not release
    // their lock or emit a stale idle.
    if (leaseLost) return

    // The idle was emitted and the lock released. RE-CLAIM once to close the
    // window where a mailbox wake arrived while we held the lock (its row is
    // still pending, but the waking invocation could not claim and returned).
    const reClaim = await claimWithRetry(deps.bus, tenant, sid)
    if (reClaim === 'error') {
      logger.error(
        { tenant, sid },
        'session re-claim failed after retries — leaving mailbox pending',
      )
      return
    }
    if (reClaim === 'busy') {
      // Another invocation owns the session now; IT will process any prompt and
      // emit its own terminal idle. We must not emit idle again here.
      return
    }
    const pending = await drainAll(deps, tenant, sid)
    if (pending.length === 0) {
      // Truly idle: release and finish.
      await releaseLease(deps.bus, tenant, sid)
      return
    }
    // A late prompt arrived after our idle: release and loop, so it runs as a
    // FRESH busy period (a genuine idle→busy transition, not a flap).
    await releaseLease(deps.bus, tenant, sid)
  }
}

/**
 * Process a drained batch as ONE busy period: fold every item into the chain in
 * arrival order (`processBatch`), then run a SINGLE turn if the batch carried
 * at least one text trigger and was not interrupted.
 *
 * A batch with only events/compact runs no turn — the fold is durable and the
 * model picks it up on the next turn. An `interrupt` in the batch aborts
 * defensively and suppresses the turn (interrupts normally never persist, so
 * this is a backstop).
 */
export async function handleBatch(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  items: ReadonlyArray<{ msg_type: string; payload: string; source?: string }>,
): Promise<void> {
  const ctrl = getAbortController(tenant, sid)
  const injected = await processBatch(deps, tenant, sid, items, ctrl)
  if (ctrl.signal.aborted || injected.length === 0) return
  const r = await runTurnOnce(deps, tenant, sid)
  if (r !== null) {
    pushEvent(deps.bus, tenant, sid, 'error', { message: r })
  }
}

export async function handleItem(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  item: { msg_type: string; payload: string; source?: string },
): Promise<void> {
  return handleBatch(deps, tenant, sid, [item])
}

/**
 * Detect a Postgres foreign-key violation (SQLSTATE 23503) in an error
 * message. Used to distinguish permanent poison-message failures (envelope
 * references a deleted session) from transient DB errors worth redelivering.
 */
function isForeignKeyViolation(errText: string): boolean {
  return /23503|foreign key|violates foreign key/i.test(errText)
}

/**
 * One user prompt → full agent turn. We drive AI SDK's `fullStream` ourselves
 * (no `stopWhen`) so each step boundary is an opportunity to drain the mailbox
 * for interrupts or freshly-arrived events, and to cap the step count.
 */
export async function runTurnOnce(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<string | null> {
  const ctrl = getAbortController(tenant, sid)
  const prepared = await prepare(deps, tenant, sid, ctrl.signal)
  if (typeof prepared === 'string') return prepared
  const {
    tools: presetTools,
    system,
    maxTurns,
    model,
    providerOptions,
    headers,
  } = prepared

  // `invalid` is the sink `repairToolCall` rewrites an unrecognised tool call
  // into: the model sees the error + the available tool names and can correct
  // itself in the SAME turn instead of the turn dying. It is never offered to
  // the model (filtered out of `activeTools` below); it only exists so the
  // rewritten call parses and executes.
  const invalidTool: Tool = {
    description:
      'Do not call this tool. It exists only to surface an invalid tool call back to you.',
    inputSchema: jsonSchema({
      type: 'object',
      properties: {
        tool: { type: 'string' },
        error: { type: 'string' },
        available: { type: 'array', items: { type: 'string' } },
      },
    }),
    execute: async (args: Record<string, unknown>) => ({
      content:
        `The tool call was invalid and could not be executed. ` +
        `Requested tool: ${String(args['tool'] ?? '(unknown)')}. ` +
        `Error: ${String(args['error'] ?? 'unknown')}. ` +
        `Re-issue the call with a valid tool name and arguments, or respond with text.`,
      metadata: null,
    }),
  }
  const tools: Record<string, Tool> = {
    ...presetTools,
    invalid: invalidTool,
  }
  const activeToolNames = Object.keys(presetTools)

  // Unique id for THIS turn. Stamped on every event so replay can hand back
  // only the live turn; the active-run marker tells watchers which run is
  // live (and is cleared the moment the turn ends, incl. on error/abort).
  const runId = randomUUID()
  const runStartedAtMs = Date.now()
  // Stamp this run onto the session's LOCK record (owner-guarded): the run id
  // and start time are the replay anchor for a reconnecting client. Fire-and-
  // forget: the lock TTL/owner heartbeat bounds a missed update.
  void updateLease(deps.bus, tenant, sid, {
    runId,
    startedAtMs: runStartedAtMs,
  })
  pushEvent(deps.bus, tenant, sid, 'status', { type: 'busy' }, runId)

  // JSON sanitizer for verbatim stream-part pass-through: large media is
  // stored in the blob store (not inlined into the event bus frame).
  const sanitizeDeps: SanitizeDeps = {
    files: deps.files,
    bus: deps.bus,
    tenant,
    session: sid,
  }

  // Cross-replica mid-stream interrupt: watch the mailbox wake subject. The
  // HTTP interrupt route publishes directly to this subject (never enqueued
  // in the mailbox), so whichever replica is running the session aborts
  // immediately; an event envelope also carries the same shape but is ignored
  // here (only `interrupt` acts as an abort).
  const sub = await deps.bus
    .subscribe(mailboxSubject(tenant, sid))
    .catch(() => null)
  let unsub: (() => void | Promise<void>) | null = null
  if (sub !== null) {
    unsub = () => sub.close()
    void (async () => {
      try {
        for await (const m of sub) {
          const parsed = parse(WakePayloadSchema, JSON.stringify(m.payload))
          if (parsed.isOk() && parsed.value.type === 'interrupt') {
            abortRun(tenant, sid, 'user')
          }
        }
      } catch (err) {
        logger.warn({ sid, err: String(err) }, 'wake watcher stopped')
      }
    })()
  }

  let messages = await loadHistory(deps, tenant, sid)
  let interrupted = false
  let finished = false
  let step = 0
  // The AI SDK finish reason of the LAST persisted step ('tool-calls' | 'stop'
  // | ...). Recorded on the turn-end marker for observability.
  let lastFinishReason = ''

  try {
    while (step < maxTurns && !ctrl.signal.aborted) {
      // Build the per-step message list from the last persisted snapshot; the
      // step may retry once after an overflow compaction.
      let stepMessages = messages

      // The step budget is exhausted: inject a wrap-up directive and DISABLE
      // tools for this final step, so the model produces a text summary instead
      // of the loop silently ending mid-task (the old `while (step < maxTurns)`
      // just fell out with no explanation to the model OR the user).
      const isLastStep = step + 1 >= maxTurns
      let stepActiveTools = activeToolNames
      if (isLastStep) {
        stepMessages = [
          ...stepMessages,
          { role: 'assistant', content: MAX_STEPS_PROMPT },
        ]
        stepActiveTools = []
      }

      // Mint this step's message id and chain anchor BEFORE streaming, and
      // announce it. The id is authoritative: every delta below carries it and
      // `persistStep` writes the SAME id, so a client groups the live stream by
      // id and never has to guess. The anchor is the current tip (already
      // reflecting any trigger drained at the previous boundary).
      //
      // `let`: an overflow compaction inside `attempt()` moves the tip onto the
      // new checkpoint; the retry RE-ANCHORS this step onto it (re-reading the
      // tip + re-announcing the same message id), so the checkpoint stays on
      // the chain instead of becoming an orphan.
      const stepMessageId = randomUUID()
      const tipRes = await Sessions.tip(deps.db, tenant, sid)
      let stepPrevId: string | null = tipRes.isErr() ? null : tipRes.value
      await pushMessageAddedNow(
        deps.bus,
        tenant,
        sid,
        {
          messageId: stepMessageId,
          prevId: stepPrevId ?? '',
          role: 'assistant',
          streaming: true,
        },
        runId,
      )

      const attempt = async (): Promise<
        | {
            text: string
            reasoning: string
            toolCalls: ToolCallRec[]
            toolResults: ToolResultRec[]
            fileParts: FilePartRec[]
            usage: { inputTokens: number; outputTokens: number } | null
            finishReason: string | null
          }
        | 'retry'
        | string
      > => {
        // One model call. Declares its own accumulators so a re-invocation
        // (below) starts clean — the failed attempt's partial output is
        // discarded, never merged into the retried attempt.
        const runOneStream = async (): Promise<
          | {
              text: string
              reasoning: string
              toolCalls: ToolCallRec[]
              toolResults: ToolResultRec[]
              fileParts: FilePartRec[]
              usage: { inputTokens: number; outputTokens: number } | null
              finishReason: string | null
            }
          | 'retry'
          | string
        > => {
          let text = ''
          let reasoning = ''
          const toolCalls: Array<{
            id: string
            name: string
            input: unknown
          }> = []
          const toolResults: Array<{
            id: string
            name: string
            result: ToolResult
          }> = []
          /** Streamed `file` / `reasoning-file` parts (already offloaded to the
           *  blob store by sanitizeStreamPart), persisted as `file` parts. */
          const fileParts: Array<{
            type: string
            code: string
            name: string
            mime: string
            size: number
          }> = []
          let usage: { inputTokens: number; outputTokens: number } | null = null
          /** Provider finish reason of this step ('stop' | 'length' | ...). */
          let finishReason: string | null = null

          // The SDK's OWN retry loops are DISABLED (`maxRetries: 0`,
          // `streamRetries: 0`): its request-start backoff is uncapped
          // (2,4,…,2048s) and emits NO event, which stalled turns for up to
          // ~an hour with no UI feedback. ALL provider retries are instead
          // driven by `attempt()`'s bounded, CAPPED (30s) and VISIBLE loop.
          const result = streamText({
            model,
            system,
            messages: stepMessages,
            tools,
            // `invalid` is a repair sink only: never advertise it to the model.
            // On the final (budget-exhausted) step the list is EMPTY so the
            // model must respond with text.
            activeTools: stepActiveTools,
            abortSignal: ctrl.signal,
            maxRetries: 0,
            streamRetries: 0,
            // NOTE: no SDK `timeout` is set. Its `chunkMs` (inter-chunk) does
            // NOT pause while a BLOCKING tool runs, so a long tool (time-wait,
            // sandbox-job-wait) starves the model stream, trips the chunk
            // timeout, and aborts the step — DISCARDING the tool's result
            // (persisted as "produced no output"). A half-open provider socket
            // is instead bounded by: the 600s per-tool deadline, our bounded
            // visible transport retry, the webui stream watchdog, and idlewatch.
            // Repair a tool call the model emitted with a wrong name (e.g. a
            // lowercased or unqualified tool) instead of failing the turn.
            repairToolCall: async ({ toolCall, tools: available, error }) => {
              const name = toolCall.toolName
              const lower = name.toLowerCase()
              if (lower !== name && available[lower] !== undefined) {
                return { ...toolCall, toolName: lower }
              }
              // Last resort: hand the model an `invalid` tool result so it can
              // self-correct in the SAME turn rather than the turn dying.
              if (
                NoSuchToolError.isInstance(error) ||
                InvalidToolInputError.isInstance(error)
              ) {
                return {
                  ...toolCall,
                  toolName: 'invalid',
                  input: JSON.stringify({
                    tool: name,
                    error: String(error),
                    available: Object.keys(available).slice(0, 50),
                  }),
                }
              }
              return null
            },
            ...(providerOptions !== undefined ? { providerOptions } : {}),
            ...(headers !== undefined ? { headers } : {}),
          })

          for await (const part of result.fullStream) {
            // ---- bookkeeping (never changes what is published) ----
            if (part.type === 'text-delta') text += part.text
            else if (part.type === 'reasoning-delta') reasoning += part.text
            else if (part.type === 'tool-call') {
              toolCalls.push({
                id: part.toolCallId,
                name: part.toolName,
                input: part.input,
              })
            } else if (part.type === 'tool-result') {
              toolResults.push({
                id: part.toolCallId,
                name: part.toolName,
                result: part.output,
              })
            } else if (part.type === 'tool-error') {
              // A tool that failed/aborted still pairs with its call id so the
              // provider never sees a dangling tool-call.
              toolResults.push({
                id: part.toolCallId,
                name: part.toolName,
                result: { content: String(part.error), metadata: null },
              })
            } else if (part.type === 'tool-output-denied') {
              toolResults.push({
                id: part.toolCallId,
                name: part.toolName,
                result: { content: 'denied', metadata: null },
              })
            } else if (part.type === 'finish-step') {
              usage = {
                inputTokens: part.usage.inputTokens ?? 0,
                outputTokens: part.usage.outputTokens ?? 0,
              }
              finishReason = part.finishReason
            } else if (part.type === 'finish') {
              finished = true
            } else if (part.type === 'abort') {
              interrupted = true
            }

            // The error part keeps its special control-flow handling: a context
            // overflow is compacted and the step retried ONCE (transparent to
            // the caller); a RETRYABLE provider failure is re-thrown so the
            // bounded, capped, VISIBLE retry loop in `attempt()` handles it (the
            // SDK's own uncapped/silent loops are disabled); any other error
            // ends the turn. A genuine error is published verbatim (sanitized)
            // before the turn unwinds; an overflow is NOT published as an error
            // (it is recovered, not a failure — a stray error card would wrongly
            // mark the turn failed in the UI).
            if (part.type === 'error') {
              const error = part.error
              if (isRetryableThrown(error) && !isToolChoiceViolation(error)) {
                throw error
              }
              if (isContextOverflowFailure(error)) {
                // Context overflow: compact and retry once with the trimmed
                // context — transparent to the caller.
                const compacted = await compactSession(
                  deps,
                  tenant,
                  sid,
                  'overflow',
                )
                if (compacted.isOk() && compacted.value) {
                  // The checkpoint moved the chain tip. RE-ANCHOR this step onto
                  // it and RE-ANNOUNCE the same message id with the new prevId
                  // (clients update the bubble's position instead of creating a
                  // second one) so the checkpoint stays on the chain rather than
                  // becoming an orphan sibling of the step.
                  const reTip = await Sessions.tip(deps.db, tenant, sid)
                  if (reTip.isOk()) stepPrevId = reTip.value
                  await pushMessageAddedNow(
                    deps.bus,
                    tenant,
                    sid,
                    {
                      messageId: stepMessageId,
                      prevId: stepPrevId ?? '',
                      role: 'assistant',
                      streaming: true,
                    },
                    runId,
                  )
                  stepMessages = await loadHistory(deps, tenant, sid)
                  return 'retry'
                }
              }
              pushEvent(
                deps.bus,
                tenant,
                sid,
                'error',
                {
                  error: String(error),
                  message: String(error),
                },
                runId,
              )
              return `turn failed: ${String(error)}`
            }

            // ---- verbatim pass-through ----
            // EVERY other AI SDK fullStream part is published under its OWN
            // event name (start-step, tool-input-start/delta/end, source, file,
            // custom, finish, abort, raw, tool-approval-*, ...), sanitized to be
            // JSON-safe. This is the "fully transparent" contract: the event
            // vocabulary is the AI SDK's, not a hand-maintained subset.
            const sanitized = await sanitizeStreamPart(
              part as { type: string } & Record<string, unknown>,
              sanitizeDeps,
            )
            // A streamed media part is already in the blob store; record its
            // `file:<code>` so it is also persisted into the message history.
            if (
              (part.type === 'file' || part.type === 'reasoning-file') &&
              typeof sanitized['code'] === 'string' &&
              sanitized['code'] !== ''
            ) {
              fileParts.push({
                type: part.type,
                code: sanitized['code'],
                name:
                  (sanitized['name'] as string | undefined) ??
                  `model-${part.type}`,
                mime:
                  (sanitized['mediaType'] as string | undefined) ??
                  'application/octet-stream',
                size: Number(sanitized['size'] ?? 0),
              })
            }
            // Tag every part with the step's server id so the client routes the
            // streamed deltas into the right bubble without guessing.
            sanitized['message_id'] = stepMessageId
            pushEvent(deps.bus, tenant, sid, part.type, sanitized, runId)
          }
          return {
            text,
            reasoning,
            toolCalls,
            toolResults,
            fileParts,
            usage,
            finishReason,
          }
        }

        // Bounded, CAPPED, VISIBLE provider retry. Covers request-start
        // failures (429/5xx/network before any output — the SDK surfaces them
        // as a retryable `error` part, which `runOneStream` re-throws), a raw
        // transport throw while reading the stream, and mid-stream error parts
        // (also re-thrown). The SDK's own loops are disabled so the backoff is
        // capped at 30s (via `retryDelayMs`) instead of the SDK's uncapped
        // minutes-long waits, and EVERY retry is announced via a durable
        // `retry` event so the UI shows progress instead of a silent stall.
        // Each attempt declares fresh accumulators (`runOneStream`), so the
        // failed attempt's partial output is discarded — exactly one chain row
        // is persisted under `stepMessageId`.
        const retries = deps.config.llmMaxRetries ?? 0
        for (let retryAttempt = 1; ; retryAttempt++) {
          try {
            return await runOneStream()
          } catch (err) {
            if (
              ctrl.signal.aborted ||
              retryAttempt > retries ||
              !isRetryableThrown(err)
            ) {
              throw err
            }
            const delay = retryDelayMs(retryAttempt, err)
            await pushRetryNow(
              deps.bus,
              tenant,
              sid,
              stepMessageId,
              retryAttempt,
              delay,
              String(err),
              runId,
            )
            await sleep(delay)
          }
        }
      }

      let stepResult: Awaited<ReturnType<typeof attempt>>
      try {
        stepResult = await attempt()
      } catch (err) {
        // A transport error that survived the retry budget (or a non-retryable
        // throw): surface it as a turn failure rather than crashing the drain.
        pushEvent(
          deps.bus,
          tenant,
          sid,
          'error',
          { error: String(err), message: String(err) },
          runId,
        )
        return `turn failed: ${String(err)}`
      }
      if (stepResult === 'retry') {
        // Seamless retry once after an overflow compaction.
        stepResult = await attempt()
      }
      if (stepResult === 'retry' || typeof stepResult === 'string') {
        return stepResult === 'retry' ? null : stepResult
      }

      // Persist this step (reasoning + text + fully-paired tool calls/results
      // + streamed files) and advance the chain tip before considering the
      // next iteration.
      const {
        text,
        reasoning,
        toolCalls,
        toolResults,
        fileParts,
        usage,
        finishReason,
      } = stepResult
      await persistStep(
        deps,
        tenant,
        sid,
        stepMessageId,
        stepPrevId,
        reasoning,
        text,
        toolCalls,
        toolResults,
        fileParts,
      )
      if (usage !== null) {
        await Sessions.addUsage(
          deps.db,
          tenant,
          sid,
          usage.inputTokens,
          usage.outputTokens,
        )
      }
      lastFinishReason = finishReason ?? ''

      // The provider stopped because it hit the OUTPUT token limit
      // (`finish_reason: length`). Any tool calls emitted in this step were
      // NEVER executed (the SDK only runs tools on `stop`/`tool-calls`), so
      // continuing would feed the model a step whose calls produced only
      // placeholders. End the turn with an explicit, honest error instead of
      // silently continuing or crashing with a cryptic SDK validation error.
      // The partial step is already persisted above (text + paired results),
      // so the user sees what was produced.
      if (finishReason === 'length') {
        const msg =
          toolCalls.length > 0
            ? `model output was truncated (finish_reason: length) after ${toolCalls.length} tool call(s); the tool calls were not executed — retry or reduce the response size`
            : 'model output was truncated (finish_reason: length)'
        pushEvent(
          deps.bus,
          tenant,
          sid,
          'error',
          { error: msg, message: msg },
          runId,
        )
        return msg
      }

      // NOTE: there is deliberately NO repeated-tool-call ("doom-loop") guard.
      // A repeated identical call is not treated as "no progress": legitimate
      // work (polling a job, retrying a flaky build, waiting on a file) can
      // issue the same call many times in a row. The turn runs until the model
      // stops, the user interrupts, or the step budget is exhausted.

      step += 1
      if (interrupted || ctrl.signal.aborted) break

      // Step boundary: inject any newly-arrived mailbox messages. A fresh
      // trigger continues the loop (the model responds to it); events fold
      // into context; a full stop with nothing new ends the turn.
      const injectedUserPrompt = await drainAndInject(deps, tenant, sid, ctrl)
      if (ctrl.signal.aborted && injectedUserPrompt.length === 0) break

      if (
        finished &&
        toolCalls.length === 0 &&
        injectedUserPrompt.length === 0
      ) {
        // Model stopped on its own and nothing new arrived.
        break
      }

      // Derive the next step's context from the AUTHORITATIVE persisted chain
      // (the step above was just written under `stepMessageId`, and any trigger
      // drained above was persisted by `drainAndInject`). If the in-memory
      // accumulator ever diverges (a missed fire-and-forget cache append, a
      // compaction that moved the tip, a concurrent chain rewrite), reloading
      // self-heals instead of feeding the model a stale/duplicated history.
      // `appendStep` is the fallback when the reload returns nothing (e.g. an
      // unreadable chain). NOTE: `injectedUserPrompt` must NOT be re-appended —
      // those triggers are already rows in the reloaded chain.
      const reloaded = await loadHistory(deps, tenant, sid)
      messages =
        reloaded.length > 0
          ? reloaded
          : appendStep(messages, text, toolCalls, toolResults)
    }
  } finally {
    // Guaranteed terminal: ALWAYS emit turn-complete (even on error/abort/early
    // return), and record the turn's outcome on the message fact. This closes
    // the "stale status busy" hole that previously let replay anchor on a
    // long-finished turn.
    if (unsub !== null) unsub()
    // Read the abort reason BEFORE `clearRun` drops it: a lock-loss abort sets
    // `locklost`, which is an ENVIRONMENTAL stop — NOT a user interrupt. Only a
    // `user` abort (or an explicit interrupt flag) records `interrupted`, so
    // idlewatch resumes a turn killed by a transient fault but never one the
    // user stopped.
    const reason = resolveTurnReason(interrupted, abortReason(tenant, sid))
    clearRun(tenant, sid)
    // The active run lives on the session LOCK record now; the lock is released
    // by `runSessionTurn` after this returns (it holds the whole busy period).
    // Record the turn-END outcome (reason/finish/tip) on the message FACT so
    // idlewatch can tell a user-stopped session from one the model left hanging
    // after a tool call. AWAITED so it is durable before the terminal idle.
    await markTurnEnd(deps, tenant, sid, reason, lastFinishReason)
    // AWAIT this terminal: the client uses `turn-complete` to close the run,
    // and a following mailbox turn publishes its `status:busy` + deltas on the
    // same subject. A fire-and-forget publish could be reordered after that
    // new busy, so the client would tear down the live continuation. Durable
    // (awaited) ordering keeps "old run ends" strictly before "new run starts".
    await pushEventNow(deps.bus, tenant, sid, 'turn-complete', {
      reason,
      run_id: runId,
    })
  }
  return null
}

/**
 * The turn-END `reason` string recorded on the message fact (and emitted on the
 * terminal `turn-complete` event). Idlewatch keys off `interrupted` to decide
 * whether a turn that stopped after a tool call should be auto-resumed, so the
 * classification is deliberate:
 *   - `interrupted` — the USER stopped it (explicit abort or an interrupt flag).
 *   - `locklost`    — an ENVIRONMENTAL stop (the run lost its lease). Resumable.
 *   - `stop`        — the model finished on its own.
 */
export function resolveTurnReason(
  interrupted: boolean,
  aborted: 'user' | 'locklost' | null,
): 'interrupted' | 'locklost' | 'stop' {
  if (interrupted || aborted === 'user') return 'interrupted'
  if (aborted === 'locklost') return 'locklost'
  return 'stop'
}

/**
 * Persist the turn-END outcome (`{ reason, finish, tip }`) onto the session's
 * message FACT in `abc-session-meta` (replacing the former `abc-session-turn`
 * bucket). `reason` is `interrupted` only when the USER stopped the turn;
 * `locklost` marks an environmental stop (idlewatch may resume it); else
 * `stop`. Best-effort: a KV failure is logged, not fatal — idlewatch treats a
 * MISSING outcome as "not interrupted".
 */
async function markTurnEnd(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  reason: string,
  finishReason: string,
): Promise<void> {
  const tipRes = await Sessions.tip(deps.db, tenant, sid)
  const tip = tipRes.isErr() ? '' : (tipRes.value ?? '')
  await setTurnEnd(deps.bus, tenant, sid, {
    reason,
    finish: finishReason,
    tip,
  }).catch(err => {
    logger.warn({ tenant, sid, err: String(err) }, 'turn-end fact write failed')
  })
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export { compactSession, spliceContext } from './session-compact.js'
export { events }
