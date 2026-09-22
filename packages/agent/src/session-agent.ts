import { randomUUID } from 'node:crypto'
import {
  Agent as AbcAgent,
  claimSession,
  releaseSession,
  renewSession,
} from '@abc-protocol/sdk'
import { streamText } from 'ai'
import type { Bus } from './bus.js'
import { mailboxSubject, SESSION_LEASE_MS } from './bus.js'
import type { ServerConfig } from './config.js'
import { isContextOverflowFailure } from './context-overflow.js'
import type { Db } from './db-client.js'
import { Mailbox } from './db-mailbox.js'
import { Sessions } from './db-sessions.js'
import {
  clearActiveRun,
  events,
  markActiveRun,
  pushEvent,
  pushEventNow,
  pushMessageAddedNow,
} from './events.js'
import type { BlobStore } from './files.js'
import { clearRun, getAbortController, interruptRun } from './interrupt.js'
import {
  ContentPayloadSchema,
  parse,
  type ToolResult,
  WakePayloadSchema,
} from './json.js'
import type { LlmRegistry } from './llm.js'
import { logger } from './logger.js'
import { compactSession } from './session-compact.js'
import { type SanitizeDeps, sanitizeStreamPart } from './stream-parts.js'
import {
  appendStep,
  drainAndInject,
  drainOne,
  type FilePartRec,
  loadHistory,
  persistEvent,
  persistStep,
  persistUserPrompt,
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
 */
export async function runSessionTurn(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<void> {
  for (;;) {
    const _agent = deps.agent ?? new AbcAgent(deps.bus)
    let revision: number | null
    try {
      revision = await claimSession(deps.bus, tenant, sid)
    } catch (e) {
      logger.warn({ tenant, sid, err: String(e) }, 'claim error')
      return
    }
    if (revision === null) {
      // Another replica is running this session; it will drain our message.
      return
    }

    // Renew the lease on a timer (TTL/3) so a long-running turn — the drain
    // loop awaits handleItem for minutes at a time — cannot be re-claimed by
    // a competing replica after the 30s TTL lapses. Each successful renew
    // returns the NEW revision, which must be fed into the next renew; using
    // the original revision forever would fail every update after the first.
    const renewTimer = setInterval(() => {
      void renewSession(deps.bus, tenant, sid, revision as number).then(
        next => {
          if (next === null) {
            logger.warn(
              { tenant, sid },
              'lease lost: another replica may be running it',
            )
            return
          }
          revision = next
        },
        err => {
          logger.warn({ tenant, sid, err: String(err) }, 'renew session failed')
        },
      )
    }, SESSION_LEASE_MS / 3)

    // Drain every pending item while holding the lease. Each prompt runs as
    // its own run (busy → … → turn-complete); there is NO idle between them —
    // a mailbox continuation is the SAME busy period, exactly as the user
    // expects ("跑完之后 consume mailbox 应延续 busy，不发 idle").
    try {
      for (;;) {
        const item = await drainOne(deps, tenant, sid)
        if (item === null) {
          // Re-drain after a short grace to close the enqueue/drain race.
          await sleep(DRAIN_GRACE_MS)
          const again = await drainOne(deps, tenant, sid)
          if (again === null) break
          await handleItem(deps, tenant, sid, again)
          continue
        }
        await handleItem(deps, tenant, sid, item)
      }
    } finally {
      clearInterval(renewTimer)
      await releaseSession(deps.bus, tenant, sid)
    }

    // The drain is empty. Release, then RE-CLAIM: the release opens a window
    // where a mailbox wake for a late prompt could not claim (we held the
    // lease) and returned, so its row may still be pending. Owning the lease
    // again lets us decide the lifecycle atomically.
    let reRevision: number | null
    try {
      reRevision = await claimSession(deps.bus, tenant, sid)
    } catch (e) {
      logger.warn({ tenant, sid, err: String(e) }, 're-claim error')
      return
    }
    if (reRevision === null) {
      // Another invocation owns the session now; IT will process any prompt and
      // emit the terminal idle when it finishes. We must not emit idle here.
      return
    }
    const pending = await drainOne(deps, tenant, sid)
    if (pending !== null) {
      // A late prompt arrived: run it under THIS lease. No idle is emitted
      // between the runs, so the continuation stays one busy period.
      await handleItem(deps, tenant, sid, pending)
      await releaseSession(deps.bus, tenant, sid)
      continue
    }
    // No work remains and we own the lease: emit the terminal idle WHILE STILL
    // HOLDING it, and AWAIT the publish. Holding the lease makes "busy for the
    // next run" and "idle for this one" mutually exclusive; awaiting removes
    // the reorder window. The client therefore never sees an idle land after a
    // newer run's busy (which used to tear down the live continuation).
    await pushEventNow(deps.bus, tenant, sid, 'status', { type: 'idle' })
    await releaseSession(deps.bus, tenant, sid)
    return
  }
}

export async function handleItem(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  item: { msg_type: string; payload: string; source?: string },
): Promise<void> {
  if (item.msg_type === 'interrupt') {
    // Interrupt is handled out-of-band by the wake watcher; ignore here.
    interruptRun(tenant, sid)
    return
  }

  if (item.msg_type === 'compact') {
    // Manual compaction is queued through the mailbox so it runs UNDER THE RUN
    // LEASE at a step boundary — exactly like a prompt — which serializes it
    // against the turn's chain writes (a concurrent compact would fork the
    // chain). `handleItem` is always called while this session's lease is held.
    const r = await compactSession(deps, tenant, sid, 'manual')
    if (r.isErr()) {
      pushEvent(deps.bus, tenant, sid, 'error', { message: r.error })
    }
    return
  }

  if (item.msg_type === 'trigger') {
    // Persist the prompt into the chain BEFORE running the turn. The mailbox
    // is the single writer: the HTTP Prompt route publishes the envelope and
    // never writes the chain, and a mailbox-delivered trigger
    // (subsession-create's handoff, mail-send's result) arrives here too.
    const payload = parse(ContentPayloadSchema, item.payload)
    const text = payload.isOk()
      ? (payload.value.text ?? payload.value.prompt ?? item.payload)
      : item.payload
    const messageId = payload.isOk() ? (payload.value.message_id ?? '') : ''
    const attachments = payload.isOk() ? (payload.value.attachments ?? []) : []
    if (text !== '' || attachments.length > 0) {
      await persistUserPrompt(
        deps,
        tenant,
        sid,
        text,
        messageId,
        attachments,
        item.source ?? '',
      )
    }
    const r = await runTurnOnce(deps, tenant, sid)
    if (r !== null) {
      pushEvent(deps.bus, tenant, sid, 'error', { message: r })
    }
    return
  }

  // Everything else is an event: fold into the chain so it reaches the model.
  await persistEvent(deps, tenant, sid, item.payload)
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
async function runTurnOnce(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<string | null> {
  const ctrl = getAbortController(tenant, sid)
  const prepared = await prepare(deps, tenant, sid, ctrl.signal)
  if (typeof prepared === 'string') return prepared
  const { tools, system, maxTurns, model, providerOptions, headers } = prepared

  // Unique id for THIS turn. Stamped on every event so replay can hand back
  // only the live turn; the active-run marker tells watchers which run is
  // live (and is cleared the moment the turn ends, incl. on error/abort).
  const runId = randomUUID()
  const runStartedAtMs = Date.now()
  markActiveRun(deps.bus, tenant, sid, runId, runStartedAtMs)
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
          if (parsed.isOk() && parsed.value.type === 'interrupt') ctrl.abort()
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

  try {
    while (step < maxTurns && !ctrl.signal.aborted) {
      // Build the per-step message list from the last persisted snapshot; the
      // step may retry once after an overflow compaction.
      let stepMessages = messages

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
          }
        | 'retry'
        | string
      > => {
        const result = streamText({
          model,
          system,
          messages: stepMessages,
          tools,
          abortSignal: ctrl.signal,
          maxRetries: 0,
          ...(providerOptions !== undefined ? { providerOptions } : {}),
          ...(headers !== undefined ? { headers } : {}),
        })

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
          } else if (part.type === 'finish') {
            finished = true
          } else if (part.type === 'abort') {
            interrupted = true
          }

          // The error part keeps its special control-flow handling: a context
          // overflow is compacted and the step retried ONCE (transparent to
          // the caller); any other error ends the turn. A genuine error is
          // published verbatim (sanitized) before the turn unwinds; an overflow
          // is NOT published as an error (it is recovered, not a failure — a
          // stray error card would wrongly mark the turn failed in the UI).
          if (part.type === 'error') {
            const error = part.error
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
        return { text, reasoning, toolCalls, toolResults, fileParts, usage }
      }

      let stepResult = await attempt()
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
      const { text, reasoning, toolCalls, toolResults, fileParts, usage } =
        stepResult
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

      // Carry this step forward into the next iteration's message list.
      messages = appendStep(messages, text, toolCalls, toolResults)
      if (injectedUserPrompt.length > 0) {
        messages.push({
          role: 'user',
          content: injectedUserPrompt.join('\n'),
        })
      }
    }
  } finally {
    // Guaranteed terminal: clear the active-run marker and ALWAYS emit
    // turn-complete (even on error/abort/early return). This closes the
    // "stale status busy" hole that previously let replay anchor on a
    // long-finished turn.
    if (unsub !== null) unsub()
    clearRun(tenant, sid)
    clearActiveRun(deps.bus, tenant, sid)
    // AWAIT this terminal: the client uses `turn-complete` to close the run,
    // and a following mailbox turn publishes its `status:busy` + deltas on the
    // same subject. A fire-and-forget publish could be reordered after that
    // new busy, so the client would tear down the live continuation. Durable
    // (awaited) ordering keeps "old run ends" strictly before "new run starts".
    await pushEventNow(deps.bus, tenant, sid, 'turn-complete', {
      reason: interrupted ? 'interrupted' : 'stop',
      run_id: runId,
    })
  }
  return null
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export { compactSession, spliceContext } from './session-compact.js'
export { events }
