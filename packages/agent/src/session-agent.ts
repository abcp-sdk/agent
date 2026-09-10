import { randomUUID } from 'node:crypto'
import {
  Agent as AbcAgent,
  claimSession,
  releaseSession,
  renewSession,
  setSessionVariable,
} from '@abc-protocol/sdk'
import type { PartRow, PresetRow } from '@easylab-agent/schema'
import type { ModelMessage, Tool } from 'ai'
import { streamText } from 'ai'
import { err, ok, type Result } from 'neverthrow'
import { z } from 'zod'
import { fireAndForget } from './async.js'
import type { Bus } from './bus.js'
import { mailboxSubject, SESSION_LEASE_MS } from './bus.js'
import {
  COMPACTION_ROLE,
  checkpointContent,
  foldQA,
  splitScan,
} from './compaction.js'
import type { ServerConfig } from './config.js'
import { isContextOverflowFailure } from './context-overflow.js'
import type { Db } from './db-client.js'
import { nowStr } from './db-client.js'
import { Mailbox } from './db-mailbox.js'
import { type ChainMessage, Messages } from './db-messages.js'
import { Parts } from './db-parts.js'
import { Sessions } from './db-sessions.js'
import { events, pushEvent } from './events.js'
import { renderTemplate } from './extensions.js'
import type { BlobStore } from './files.js'
import { rebuildHistory } from './history.js'
import { pickLocalized, resolveLocale } from './i18n.js'
import { clearRun, getAbortController, interruptRun } from './interrupt.js'
import {
  ContentPayloadSchema,
  parse,
  SummaryPartDataSchema,
  stringify,
  TextPartDataSchema,
  type ToolResult,
  WakePayloadSchema,
} from './json.js'
import { Config, Presets } from './kv-store.js'
import { type LlmRegistry, parseProviderModelRef } from './llm.js'
import { catalogModel, findVariant, type JsonObject, resolveContextLimit } from './variants.js'
import { logger } from './logger.js'
import { factFromPersist, projectMessageFact } from './session-state.js'
import {
  appendSessionId,
  getModelsDev,
  getSessionIds,
  putSessionIds,
} from './store.js'
import {
  buildAiTools,
  discoverToolsCached,
  toolQualifiedName,
  toolsBlockedByMissingRequired,
} from './tools.js'

export interface AgentDeps {
  db: Db
  bus: Bus
  config: ServerConfig
  llm: LlmRegistry
  files: BlobStore
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
  const agent = new AbcAgent(deps.bus)
  void agent
    .consumeMailbox(async msg => {
      if (stopped) return
      await handleMailboxMessage(deps, msg)
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
  msg: { id: string; sessionName: string; type: string; payload?: unknown },
): Promise<void> {
  const env = {
    // Defensive: a wake-style message without a producer id would fail the
    // uuid-typed PG key on '' and poison the consumer; mint one instead.
    id: msg.id || randomUUID(),
    session_name: msg.sessionName,
    type: msg.type,
    payload: msg.payload,
  }

  const enq = await Mailbox.enqueueIdempotent(
    deps.db,
    env.id,
    env.session_name,
    env.type,
    env.payload,
  )
  if (enq.isErr()) {
    if (isForeignKeyViolation(enq.error)) {
      logger.warn(
        { sid: env.session_name },
        'mailbox: session missing — discarding',
      )
    } else {
      logger.warn(
        { sid: env.session_name, err: String(enq.error) },
        'mailbox: enqueue failed — redelivering',
      )
      throw enq.error
    }
    return
  }

  void runSessionTurn(deps, env.session_name).then(
    () => {},
    e =>
      logger.error({ sid: env.session_name, err: String(e) }, 'turn crashed'),
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
  sid: string,
): Promise<void> {
  for (;;) {
    const agent = new AbcAgent(deps.bus)
    let revision: number | null
    try {
      revision = await claimSession(deps.bus, sid)
    } catch (e) {
      logger.warn({ sid, err: String(e) }, 'claim error')
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
      void renewSession(deps.bus, sid, revision as number).then(
        next => {
          if (next === null) {
            logger.warn(
              { sid },
              'lease lost: another replica may be running it',
            )
            return
          }
          revision = next
        },
        err => {
          logger.warn({ sid, err: String(err) }, 'renew session failed')
        },
      )
    }, SESSION_LEASE_MS / 3)

    try {
      for (;;) {
        const item = await drainOne(deps, sid)
        if (item === null) {
          // Re-drain after a short grace to close the enqueue/drain race.
          await sleep(DRAIN_GRACE_MS)
          const again = await drainOne(deps, sid)
          if (again === null) break
          await handleItem(deps, sid, again)
          continue
        }
        await handleItem(deps, sid, item)
      }
    } finally {
      clearInterval(renewTimer)
      await releaseSession(deps.bus, sid)
    }

    // Released: confirm nothing arrived during the release window. If it did,
    // loop and re-claim (the lease is now free, so the claim wins).
    const leftover = await drainOne(deps, sid)
    if (leftover === null) break
    await handleItem(deps, sid, leftover)
  }
  pushEvent(deps.bus, sid, 'status', { type: 'idle' })
}

async function handleItem(
  deps: AgentDeps,
  sid: string,
  item: { msg_type: string; payload: string },
): Promise<void> {
  if (item.msg_type === 'interrupt') {
    // Interrupt is handled out-of-band by the wake watcher; ignore here.
    interruptRun(sid)
    return
  }

  if (item.msg_type === 'user_prompt') {
    const r = await runTurnOnce(deps, sid)
    if (r !== null) {
      pushEvent(deps.bus, sid, 'error', { message: r })
    }
    return
  }

  // Everything else is an event: fold into the chain so it reaches the model.
  await persistEvent(deps, sid, item.payload)
}

async function drainOne(deps: AgentDeps, sid: string) {
  const r = await Mailbox.drainOne(deps.db, sid)
  return r.isErr() ? null : r.value
}

/**
 * Detect a Postgres foreign-key violation (SQLSTATE 23503) in an error
 * message. Used to distinguish permanent poison-message failures (envelope
 * references a deleted session) from transient DB errors worth redelivering.
 */
function isForeignKeyViolation(errText: string): boolean {
  return /23503|foreign key|violates foreign key/i.test(errText)
}

interface TurnCtx {
  tools: Record<string, Tool>
  system: string
  maxTurns: number
  model: import('ai').LanguageModel
  providerOptions: Record<string, import('./variants.js').JsonObject> | undefined
  headers: Record<string, string> | undefined
}

/**
 * One user prompt → full agent turn. We drive AI SDK's `fullStream` ourselves
 * (no `stopWhen`) so each step boundary is an opportunity to drain the mailbox
 * for interrupts or freshly-arrived events, and to cap the step count.
 */
async function runTurnOnce(
  deps: AgentDeps,
  sid: string,
): Promise<string | null> {
  const ctrl = getAbortController(sid)
  const prepared = await prepare(deps, sid, ctrl.signal)
  if (typeof prepared === 'string') return prepared
  const { tools, system, maxTurns, model, providerOptions, headers } = prepared

  pushEvent(deps.bus, sid, 'status', { type: 'busy' })

  // Cross-replica mid-stream interrupt: watch the mailbox wake subject. The
  // HTTP interrupt route publishes directly to this subject (never enqueued
  // in the mailbox), so whichever replica is running the session aborts
  // immediately; an event envelope also carries the same shape but is ignored
  // here (only `interrupt` acts as an abort).
  const sub = await deps.bus.subscribe(mailboxSubject(sid)).catch(() => null)
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

  let messages = await loadHistory(deps, sid)
  let interrupted = false
  let finished = false
  let step = 0

  try {
    while (step < maxTurns && !ctrl.signal.aborted) {
      // Build the per-step message list from the last persisted snapshot; the
      // step may retry once after an overflow compaction.
      let stepMessages = messages

      const attempt = async (): Promise<
        | {
            text: string
            toolCalls: ToolCallRec[]
            toolResults: ToolResultRec[]
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
          temperature: deps.config.defaultTemperature,
          maxOutputTokens: deps.config.defaultMaxTokens,
          abortSignal: ctrl.signal,
          maxRetries: 0,
          ...(providerOptions !== undefined ? { providerOptions } : {}),
          ...(headers !== undefined ? { headers } : {}),
        })

        let text = ''
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
        let usage: { inputTokens: number; outputTokens: number } | null = null

        for await (const part of result.fullStream) {
          switch (part.type) {
            case 'start-step':
              pushEvent(deps.bus, sid, 'step-start', {})
              break
            case 'text-start':
              pushEvent(deps.bus, sid, 'text-start', { id: 't0' })
              break
            case 'text-delta':
              text += part.text
              pushEvent(deps.bus, sid, 'text-delta', {
                id: 't0',
                text: part.text,
              })
              break
            case 'text-end':
              pushEvent(deps.bus, sid, 'text-end', { id: 't0' })
              break
            case 'tool-call':
              toolCalls.push({
                id: part.toolCallId,
                name: part.toolName,
                input: part.input,
              })
              pushEvent(deps.bus, sid, 'tool-call', {
                toolCallId: part.toolCallId,
                toolName: part.toolName,
                input: part.input,
              })
              break
            case 'tool-result':
              toolResults.push({
                id: part.toolCallId,
                name: part.toolName,
                result: part.output,
              })
              pushEvent(deps.bus, sid, 'tool-result', {
                toolCallId: part.toolCallId,
                toolName: part.toolName,
                formatted: part.output.content,
                change_id:
                  typeof part.output.metadata?.change_id === 'string'
                    ? part.output.metadata.change_id
                    : undefined,
              })
              break
            case 'tool-error':
              // A tool that failed/aborted still pairs with its call id so the
              // provider never sees a dangling tool-call.
              toolResults.push({
                id: part.toolCallId,
                name: part.toolName,
                result: { content: String(part.error), metadata: null },
              })
              pushEvent(deps.bus, sid, 'tool-error', {
                toolCallId: part.toolCallId,
                error: String(part.error),
              })
              break
            case 'tool-output-denied':
              toolResults.push({
                id: part.toolCallId,
                name: part.toolName,
                result: { content: 'denied', metadata: null },
              })
              break
            case 'finish-step':
              usage = {
                inputTokens: part.usage.inputTokens ?? 0,
                outputTokens: part.usage.outputTokens ?? 0,
              }
              break
            case 'finish':
              finished = true
              break
            case 'error': {
              const error = part.error
              if (isContextOverflowFailure(error)) {
                // Context overflow: compact and retry once with the trimmed
                // context — transparent to the caller.
                const compacted = await compactSession(deps, sid, 'overflow')
                if (compacted.isOk() && compacted.value) {
                  stepMessages = await loadHistory(deps, sid)
                  return 'retry'
                }
              }
              return `turn failed: ${String(error)}`
            }
            case 'abort':
              interrupted = true
              break
            default:
              break
          }
        }
        return { text, toolCalls, toolResults, usage }
      }

      let stepResult = await attempt()
      if (stepResult === 'retry') {
        // Seamless retry once after an overflow compaction.
        stepResult = await attempt()
      }
      if (stepResult === 'retry' || typeof stepResult === 'string') {
        return stepResult === 'retry' ? null : stepResult
      }

      // Persist this step (text + fully-paired tool calls/results) and advance
      // the chain tip before considering the next iteration.
      const { text, toolCalls, toolResults, usage } = stepResult
      await persistStep(deps, sid, text, toolCalls, toolResults)
      if (usage !== null) {
        await Sessions.addUsage(
          deps.db,
          sid,
          usage.inputTokens,
          usage.outputTokens,
        )
      }

      step += 1
      if (interrupted || ctrl.signal.aborted) break

      // Step boundary: inject any newly-arrived mailbox messages. A fresh
      // user_prompt continues the loop (the model responds to it); events fold
      // into context; a full stop with nothing new ends the turn.
      const injectedUserPrompt = await drainAndInject(deps, sid, ctrl)
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
    if (unsub !== null) unsub()
    clearRun(sid)
  }

  pushEvent(deps.bus, sid, 'turn-complete', {
    reason: interrupted ? 'interrupted' : 'stop',
  })
  return null
}

async function prepare(
  deps: AgentDeps,
  sid: string,
  abortSignal: AbortSignal,
): Promise<TurnCtx | string> {
  const sessionRes = await Sessions.get(deps.db, sid)
  if (sessionRes.isErr()) return sessionRes.error
  const session = sessionRes.value
  if (session === null) return 'session not found'

  const presetRow =
    session.preset !== ''
      ? (await Presets.get(deps.bus, session.preset)).unwrapOr(null)
      : null
  const presetTools = parse(z.array(z.string()), presetRow?.tools ?? '[]')
  const toolNames = presetTools.isOk() ? presetTools.value : []
  const whitelist = toolNames.length > 0 ? new Set(toolNames) : null

  // Effective locale: session → KV config (live) → env → "en". The KV read
  // makes `PUT /api/v1/config {key:"locale"}` take effect on the very next
  // turn (no restart), while per-session `PATCH /sessions/{id}/settings`
  // {locale} still wins. Used to localize tool descriptions and the system
  // prompt, and projected as vars.agent.locale.
  const configLocale = (await Config.get(deps.bus, 'locale')).unwrapOr(null)
  const locale = resolveLocale(
    session.locale,
    configLocale,
    process.env.LOCALE ?? 'en',
  )

  const discovered = await discoverToolsCached(deps.bus)
  const active =
    whitelist === null
      ? discovered
      : discovered.filter(t => whitelist.has(toolQualifiedName(discovered, t)))
  logger.info(
    {
      sid,
      tools: active.map(t => toolQualifiedName(discovered, t)),
      whitelisted: whitelist !== null,
    },
    'tools prepared for turn',
  )
  // Hard-disable tools whose required config is unset: the model must not call
  // a tool it cannot run. Reads the `cfg` bucket per turn (cheap, few knobs).
  const blocked = await toolsBlockedByMissingRequired(deps.bus, active)
  if (blocked.size > 0) {
    logger.info(
      { sid, blocked: [...blocked] },
      'tools blocked (required config unset)',
    )
  }
  const tools = buildAiTools(
    active,
    deps.bus,
    deps.config.toolTimeoutMs,
    sid,
    abortSignal,
    locale,
    blocked,
  )

  // Session-level settings (PATCH /sessions/{id}/settings) override the
  // preset; the preset overrides the config default.
  const systemPrompt =
    session.system_prompt !== ''
      ? session.system_prompt
      : presetRow !== null
        ? presetPromptFor(presetRow, locale)
        : 'You are a helpful assistant.'
  const env = [
    '<env>',
    `  Today's date: ${new Date().toISOString().slice(0, 10)}`,
    '</env>',
  ].join('\n')

  // Render extension-provided template variables ({{ext.<id>.<name>}}) and
  // built-ins ({{date}}/{{datetime}}) into the system prompt. Unresolvable
  // variables are left as literal placeholders.
  const sessionName = sid
  const renderedPrompt = await renderTemplate(
    systemPrompt,
    deps.bus,
    sessionName,
  )

  const maxTurns =
    session.max_turns > 0
      ? session.max_turns
      : presetRow !== null && presetRow.max_turns > 0
        ? presetRow.max_turns
        : deps.config.defaultMaxTurns

  const resolved = await deps.llm.resolve(deps.db, session.model)
  if (resolved.isErr()) return resolved.error

  // Resolve the selected reasoning variant (if any) into AI-SDK
  // providerOptions + request headers. Strict provider+model lookup against
  // the models.dev catalog; no variant ⇒ no providerOptions sent.
  const { providerOptions, headers } = await resolveVariantOptions(
    deps,
    resolved.value.providerId,
    resolved.value.modelId,
    resolved.value.apiType,
    session.variant,
  )

  // Project the effective locale as a session variable so extensions can
  // localize their tool-result text. Written by the agent (provider "agent")
  // into the shared vars bucket during each turn.
  void setSessionVariable(deps.bus, 'agent', sid, 'locale', locale)

  return {
    tools,
    system: `${renderedPrompt}\n\n${env}`,
    maxTurns,
    model: resolved.value.model,
    providerOptions,
    headers,
  }
}

/** Fall back to a helper 'You are a helpful assistant.' marker. */
const DEFAULT_SYSTEM_PROMPT = 'You are a helpful assistant.'

/** Resolve the preset's system prompt honoring the effective locale. */
function presetPromptFor(preset: PresetRow, locale: string): string {
  // Parse `system_prompt_i18n` as { locale: template }; fall back to the
  // default `system_prompt` (English) when absent or unmatched.
  if (preset.system_prompt_i18n && preset.system_prompt_i18n !== '{}') {
    const map = parse(
      z.record(z.string(), z.string()),
      preset.system_prompt_i18n,
    )
    if (map.isOk()) {
      const picked = pickLocalized(map.value, locale)
      if (picked !== null) return picked
    }
  }
  return preset.system_prompt || DEFAULT_SYSTEM_PROMPT
}

interface ToolCallRec {
  id: string
  name: string
  input: unknown
}
interface ToolResultRec {
  id: string
  name: string
  result: ToolResult
}

/** Persist one step: chained assistant message + text/tool/tool_result parts. */
async function persistStep(
  deps: AgentDeps,
  sid: string,
  text: string,
  toolCalls: ToolCallRec[],
  toolResults: ToolResultRec[],
): Promise<void> {
  if (text === '' && toolCalls.length === 0) return

  const tipRes = await Sessions.tip(deps.db, sid)
  const prevId = tipRes.isErr() ? null : tipRes.value
  const insert = await Messages.insert(deps.db, 'assistant', prevId)
  if (insert.isErr()) {
    logger.error({ sid, err: String(insert.error) }, 'persist step failed')
    return
  }
  const messageId = insert.value

  let seq = 0
  if (text !== '') {
    await Parts.insert(deps.db, messageId, 'text', seq++, { text })
  }
  for (const tc of toolCalls) {
    const result = toolResults.find(r => r.id === tc.id)?.result
    await Parts.insert(deps.db, messageId, 'tool', seq++, {
      id: tc.id,
      name: tc.name,
      input: tc.input,
    })
    // Persist the canonical ToolResult (content + opaque metadata) so the
    // history rebuild and the read API can both reproduce it verbatim. The
    // metadata blob belongs to the tool server; the agent never interprets it.
    const content =
      result !== undefined
        ? result.content
        : `tool '${tc.name}' produced no output`
    const metadata = result !== undefined ? result.metadata : null
    await Parts.insert(deps.db, messageId, 'tool_result', seq++, {
      tool_use_id: tc.id,
      content,
      metadata,
    })
  }
  await Sessions.setTip(deps.db, sid, messageId)
  // Keep the per-session context id cache in step with the write.
  fireAndForget(appendSessionId(deps.bus, sid, messageId), 'appendSessionIds')
  // Mirror the newest-message fact to the bus KV for DB-less consumers.
  projectMessageFact(
    deps.bus,
    sid,
    factFromPersist(nowStr(), 'assistant', text),
  )
}

/** Fold a mailbox event into the chain as an `event` message. */
async function persistEvent(deps: AgentDeps, sid: string, payload: string) {
  const parsed = parse(ContentPayloadSchema, payload)
  const text =
    parsed.isOk() && parsed.value.content !== undefined
      ? parsed.value.content
      : payload
  const tip = await Sessions.tip(deps.db, sid)
  const tipId = tip.isErr() ? null : tip.value
  const insert = await Messages.insert(deps.db, 'event', tipId)
  if (insert.isOk()) {
    await Parts.insert(deps.db, insert.value, 'text', 0, { text })
    await Sessions.setTip(deps.db, sid, insert.value)
    fireAndForget(
      appendSessionId(deps.bus, sid, insert.value),
      'appendSessionIds',
    )
    projectMessageFact(deps.bus, sid, factFromPersist(nowStr(), 'event', text))
  }
}

/**
 * Between steps: drain the mailbox and inject everything that arrived.
 *
 * - `user_prompt` → persisted as a `role=user` message (chained) and returned
 *   so the loop continues and the model responds to it.
 * - other event types → folded as `role=event` (as `persistEvent`).
 * - `interrupt` → handled out-of-band by the wake watcher; if one surfaces
 *   here we abort defensively.
 *
 * Returns the list of injected user prompts (may be empty).
 */
async function drainAndInject(
  deps: AgentDeps,
  sid: string,
  ctrl: AbortController,
): Promise<string[]> {
  const injected: string[] = []
  for (;;) {
    const item = await drainOne(deps, sid)
    if (item === null) break
    if (item.msg_type === 'interrupt') {
      ctrl.abort()
      continue
    }
    if (item.msg_type === 'user_prompt') {
      const payload = parse(ContentPayloadSchema, item.payload)
      const text = payload.isOk()
        ? (payload.value.text ?? payload.value.prompt ?? item.payload)
        : item.payload
      await persistUserPrompt(deps, sid, text)
      injected.push(text)
      continue
    }
    await persistEvent(deps, sid, item.payload)
  }
  return injected
}

/** Persist an injected user prompt as a chained `role=user` message. */
async function persistUserPrompt(
  deps: AgentDeps,
  sid: string,
  text: string,
): Promise<void> {
  const tip = await Sessions.tip(deps.db, sid)
  const tipId = tip.isErr() ? null : tip.value
  const insert = await Messages.insert(deps.db, 'user', tipId)
  if (insert.isOk()) {
    await Parts.insert(deps.db, insert.value, 'text', 0, { text })
    await Sessions.setTip(deps.db, sid, insert.value)
    fireAndForget(
      appendSessionId(deps.bus, sid, insert.value),
      'appendSessionIds',
    )
    projectMessageFact(deps.bus, sid, factFromPersist(nowStr(), 'user', text))
  }
}

/** Append a completed step to the in-memory message list for the next step. */
function appendStep(
  messages: ModelMessage[],
  text: string,
  toolCalls: ToolCallRec[],
  toolResults: ToolResultRec[],
): ModelMessage[] {
  const content: Array<
    | { type: 'text'; text: string }
    | {
        type: 'tool-call'
        toolCallId: string
        toolName: string
        input: unknown
      }
  > = []
  if (text !== '') content.push({ type: 'text', text })
  for (const tc of toolCalls) {
    content.push({
      type: 'tool-call',
      toolCallId: tc.id,
      toolName: tc.name,
      input: tc.input ?? {},
    })
  }
  const next: ModelMessage[] = [...messages]
  next.push({ role: 'assistant', content })
  if (toolResults.length > 0) {
    next.push({
      role: 'tool',
      content: toolResults.map(r => ({
        type: 'tool-result',
        toolCallId: r.id,
        toolName: r.name,
        output: { type: 'text', value: r.result.content },
      })),
    })
  }
  return next
}

async function loadHistory(
  deps: AgentDeps,
  sid: string,
): Promise<ModelMessage[]> {
  const tipRes = await Sessions.tip(deps.db, sid)
  const tipId = tipRes.isErr() ? null : tipRes.value
  if (tipId === null) return []

  // Cache hit: use the cached id list to fetch rows + parts directly.
  const cached = await getSessionIds(deps.bus, sid)
  if (cached !== null) {
    const rows = await Messages.byIds(deps.db, cached)
    const parts = await Parts.listByMessages(deps.db, cached)
    if (rows.isOk() && parts.isOk()) {
      return spliceContext(rows.value, parts.value)
    }
  }

  // Cache miss: bounded scan from tip, then backfill.
  const chain = await Messages.chain(deps.db, tipId, 100_000, null)
  if (chain.isErr()) return []
  const ids = chain.value.map(m => m.id)
  const parts = await Parts.listByMessages(deps.db, ids)
  if (parts.isErr()) return []

  // Backfill the cache with the full bounded id list.
  fireAndForget(putSessionIds(deps.bus, sid, ids), 'putSessionIds')

  return spliceContext(chain.value, parts.value)
}

/**
 * Build the LLM context from a bounded id list + parts. The newest compaction
 * message supplies the checkpoint summary; only messages at or after its
 * recorded tail boundary (`tail_from`) are kept verbatim — everything older
 * is represented by the checkpoint. With no compaction message the whole
 * chain is rebuilt verbatim.
 */
export function spliceContext(
  rows: ChainMessage[],
  parts: PartRow[],
): ModelMessage[] {
  interface Cm {
    summary: string
    tailFrom: string | null
  }
  const cmByMsg = new Map<string, Cm>()
  for (const p of parts) {
    if (p.type !== 'summary') continue
    const d = parse(SummaryPartDataSchema, p.data)
    if (d.isOk()) {
      cmByMsg.set(p.message_id, {
        summary: d.value.summary,
        tailFrom: d.value.tail_from ?? null,
      })
    }
  }

  // rows are oldest-first; walk from the newest end to find the latest
  // compaction message (chain may carry several).
  let cm: Cm | null = null
  let cmIndex = -1
  for (let i = rows.length - 1; i >= 0; i--) {
    const row = rows[i]
    if (row === undefined) continue
    const c = cmByMsg.get(row.id)
    if (c !== undefined) {
      cm = c
      cmIndex = i
      break
    }
  }

  if (cm === null) {
    return rebuildHistory(
      rows.filter(r => r.role !== COMPACTION_ROLE),
      parts,
    )
  }

  // Prefer the recorded tail boundary; fall back to everything after the cm
  // (legacy summaries written before tail_from existed).
  let start = cmIndex + 1
  if (cm.tailFrom !== null) {
    const idx = rows.findIndex(r => r.id === cm.tailFrom)
    if (idx >= 0) start = idx
  }

  const visibleRows = rows.slice(start).filter(r => r.role !== COMPACTION_ROLE)
  const history = rebuildHistory(visibleRows, parts)
  return [{ role: 'user', content: checkpointContent(cm.summary) }, ...history]
}

/**
 * Fold the chain prefix into a rule-based summary (no LLM) and persist a new
 * compaction message chained onto the tip.
 */
export async function compactSession(
  deps: AgentDeps,
  sid: string,
  reason: 'manual' | 'overflow' = 'manual',
): Promise<Result<boolean, string>> {
  const tipRes = await Sessions.tip(deps.db, sid)
  if (tipRes.isErr()) return err(tipRes.error)
  const tipId = tipRes.value
  if (tipId === null) return ok(false)

  const sessionRes = await Sessions.get(deps.db, sid)
  if (sessionRes.isErr()) return err(sessionRes.error)
  const session = sessionRes.value
  const modelId = session === null ? '' : session.model

  const chain = await Messages.chain(deps.db, tipId, 100_000, null)
  if (chain.isErr()) return err(chain.error)

  const ids = chain.value.map(m => m.id)
  const partsRes = await Parts.listByMessages(deps.db, ids)
  if (partsRes.isErr()) return err(partsRes.error)
  const parts = partsRes.value

  // Fold entries: text per message + tool-call count.
  const textByMsg = new Map<string, string>()
  for (const p of parts) {
    if (p.type !== 'text') continue
    const d = parse(TextPartDataSchema, p.data)
    if (d.isOk()) {
      textByMsg.set(
        p.message_id,
        (textByMsg.get(p.message_id) ?? '') + d.value.text,
      )
    }
  }
  const toolCountByMsg = new Map<string, number>()
  for (const p of parts) {
    if (p.type === 'tool') {
      toolCountByMsg.set(
        p.message_id,
        (toolCountByMsg.get(p.message_id) ?? 0) + 1,
      )
    }
  }

  const entries = chain.value.map(m => ({
    id: m.id,
    role: m.role,
    text: textByMsg.get(m.id) ?? '',
    toolCalls: toolCountByMsg.get(m.id) ?? 0,
  }))

  const limit = await contextLimit(deps, modelId)
  const { tail, folded } = splitScan(entries, limit * 0.2, limit * 0.1)
  if (folded.length === 0) return ok(false)

  const summary = foldQA(folded)
  if (summary.trim() === '') return ok(false)

  // tail is oldest-first; its first entry marks the verbatim boundary kept
  // after this checkpoint.
  const tailFromId = tail[0]?.id ?? null

  const insert = await Messages.insert(deps.db, COMPACTION_ROLE, tipId)
  if (insert.isErr()) return err(insert.error)
  const cmId = insert.value
  const part = await Parts.insertSummary(deps.db, cmId, summary, tailFromId)
  if (part.isErr()) return err(part.error)
  await Sessions.setTip(deps.db, sid, cmId)
  projectMessageFact(
    deps.bus,
    sid,
    factFromPersist(nowStr(), COMPACTION_ROLE, summary),
  )

  // Rewrite the cache to the new bounded context id list (with the cm).
  const chainAfter = await Messages.chain(deps.db, cmId, 100_000, null)
  if (chainAfter.isOk()) {
    fireAndForget(
      putSessionIds(
        deps.bus,
        sid,
        chainAfter.value.map(m => m.id),
      ),
      'putSessionIds',
    )
  }

  pushEvent(deps.bus, sid, 'compacted', { reason })
  return ok(true)
}

async function contextLimit(
  deps: AgentDeps,
  modelRef: string,
): Promise<number> {
  const catalog = await getModelsDev(deps.bus)
  const parsed = parseProviderModelRef(modelRef)
  if (catalog === null || catalog === undefined || parsed === null) {
    return deps.config.compactionContextTokens
  }
  return resolveContextLimit(
    catalog,
    parsed.providerId,
    parsed.modelId,
    deps.config.compactionContextTokens,
  )
}

/**
 * Resolve the selected reasoning variant to AI-SDK providerOptions + request
 * headers. Unknown variant or catalog miss ⇒ `{}`/undefined (provider
 * defaults; no providerOptions sent).
 */
async function resolveVariantOptions(
  deps: AgentDeps,
  providerId: string,
  modelId: string,
  apiType: string,
  variantId: string,
): Promise<{
  providerOptions: Record<string, JsonObject> | undefined
  headers: Record<string, string> | undefined
}> {
  if (variantId === '') return { providerOptions: undefined, headers: undefined }
  const catalog = await getModelsDev(deps.bus)
  const model = catalogModel(catalog, providerId, modelId)
  if (model === null) {
    logger.warn(
      { provider: providerId, model: modelId, variant: variantId },
      'variant requested but model not in catalog; ignoring',
    )
    return { providerOptions: undefined, headers: undefined }
  }
  const def = findVariant(model, apiType, variantId)
  if (def === null) {
    logger.warn(
      { provider: providerId, model: modelId, variant: variantId },
      'unknown variant; ignoring',
    )
    return { providerOptions: undefined, headers: undefined }
  }
  return {
    providerOptions:
      Object.keys(def.providerOptions).length > 0
        ? def.providerOptions
        : undefined,
    headers: def.headers,
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export { events }
