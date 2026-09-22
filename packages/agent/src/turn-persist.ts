import { randomUUID } from 'node:crypto'
import type { ModelMessage } from 'ai'
import { fireAndForget } from './async.js'
import { nowStr } from './db-client.js'
import { Mailbox } from './db-mailbox.js'
import { Messages } from './db-messages.js'
import { Parts } from './db-parts.js'
import { Sessions } from './db-sessions.js'
import { pushMessageAdded } from './events.js'
import { ContentPayloadSchema, parse, type ToolResult } from './json.js'
import { logger } from './logger.js'
import type { AgentDeps } from './session-agent.js'
import { spliceContext } from './session-compact.js'
import { factFromPersist, projectMessageFact } from './session-state.js'
import { appendSessionId, getSessionIds, putSessionIds } from './store.js'

/**
 * Chain-writer helpers for one session turn: persisting steps / user prompts /
 * events under pre-minted (already-announced) message ids, the between-step
 * mailbox drain, and the history rebuild that feeds the next LLM call.
 * Everything here runs under the session's run lease (see runSessionTurn):
 * the mailbox path is the SINGLE writer of the prompt chain.
 */

export interface ToolCallRec {
  id: string
  name: string
  input: unknown
}

/** A streamed media part to persist as a `file` part (already in the blob
 *  store; only its reference is carried here). */
export interface FilePartRec {
  type: string
  code: string
  name: string
  mime: string
  size: number
}

export interface ToolResultRec {
  id: string
  name: string
  result: ToolResult
}

/** Drain one pending mailbox item for the session (null = empty). */
export async function drainOne(deps: AgentDeps, tenant: string, sid: string) {
  const r = await Mailbox.drainOne(deps.db, tenant, sid)
  return r.isErr() ? null : r.value
}

/** Persist one step under its PRE-MINTED id: chained assistant message +
 *  file/text/tool/tool_result parts. The id was announced via message-added
 *  before streaming, so the persisted row must use the SAME id — that is what
 *  makes the client's streamed bubble turn into the server row in place. */
export async function persistStep(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  messageId: string,
  prevId: string | null,
  reasoning: string,
  text: string,
  toolCalls: ToolCallRec[],
  toolResults: ToolResultRec[],
  fileParts: FilePartRec[] = [],
): Promise<void> {
  if (
    reasoning === '' &&
    text === '' &&
    toolCalls.length === 0 &&
    fileParts.length === 0
  ) {
    return
  }

  const insert = await Messages.insertWithId(
    deps.db,
    tenant,
    messageId,
    'assistant',
    prevId,
  )
  if (insert.isErr()) {
    logger.error({ sid, err: String(insert.error) }, 'persist step failed')
    return
  }
  // Redelivery/duplicate: the row already exists — its parts are complete.
  if (!insert.value) return

  let seq = 0
  // Reasoning (thinking) is persisted for display only; it is deliberately
  // EXCLUDED from the model's rebuilt context (see rebuildHistory/appendStep).
  if (reasoning !== '') {
    await Parts.insert(deps.db, tenant, messageId, 'reasoning', seq++, {
      text: reasoning,
    })
  }
  if (text !== '') {
    await Parts.insert(deps.db, tenant, messageId, 'text', seq++, { text })
  }
  for (const tc of toolCalls) {
    const result = toolResults.find(r => r.id === tc.id)?.result
    await Parts.insert(deps.db, tenant, messageId, 'tool', seq++, {
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
    await Parts.insert(deps.db, tenant, messageId, 'tool_result', seq++, {
      tool_use_id: tc.id,
      content,
      metadata,
    })
  }
  // Streamed media parts (model-produced files) → `file` parts.
  for (const f of fileParts) {
    await Parts.insert(deps.db, tenant, messageId, 'file', seq++, {
      code: f.code,
      name: f.name,
      mime: f.mime,
      size: f.size,
    })
  }
  await Sessions.setTip(deps.db, tenant, sid, messageId)
  // Keep the per-session context id cache in step with the write.
  fireAndForget(
    appendSessionId(deps.bus, tenant, sid, messageId),
    'appendSessionIds',
  )
  // Mirror the newest-message fact to the bus KV for DB-less consumers.
  projectMessageFact(
    deps.bus,
    tenant,
    sid,
    factFromPersist(nowStr(), 'assistant', text !== '' ? text : reasoning),
  )
}

/** Fold a mailbox event into the chain as an `event` message. */
export async function persistEvent(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  payload: string,
) {
  const parsed = parse(ContentPayloadSchema, payload)
  const text =
    parsed.isOk() && parsed.value.content !== undefined
      ? parsed.value.content
      : payload
  const tip = await Sessions.tip(deps.db, tenant, sid)
  const tipId = tip.isErr() ? null : tip.value
  const insert = await Messages.insert(deps.db, tenant, 'event', tipId)
  if (insert.isOk()) {
    await Parts.insert(deps.db, tenant, insert.value, 'text', 0, { text })
    await Sessions.setTip(deps.db, tenant, sid, insert.value)
    fireAndForget(
      appendSessionId(deps.bus, tenant, sid, insert.value),
      'appendSessionIds',
    )
    projectMessageFact(
      deps.bus,
      tenant,
      sid,
      factFromPersist(nowStr(), 'event', text),
    )
  }
}

/**
 * Between steps: drain the mailbox and inject everything that arrived.
 *
 * - `trigger` → persisted as a `role=user` message (chained) and returned
 *   so the loop continues and the model responds to it.
 * - other event types → folded as `role=event` (as `persistEvent`).
 * - `interrupt` → handled out-of-band by the wake watcher; if one surfaces
 *   here we abort defensively.
 *
 * Returns the list of injected user prompts (may be empty).
 */
export async function drainAndInject(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  ctrl: AbortController,
): Promise<string[]> {
  const injected: string[] = []
  for (;;) {
    const item = await drainOne(deps, tenant, sid)
    if (item === null) break
    if (item.msg_type === 'interrupt') {
      ctrl.abort()
      continue
    }
    if (item.msg_type === 'trigger') {
      const payload = parse(ContentPayloadSchema, item.payload)
      const text = payload.isOk()
        ? (payload.value.text ?? payload.value.prompt ?? item.payload)
        : item.payload
      const messageId = payload.isOk() ? (payload.value.message_id ?? '') : ''
      const attachments = payload.isOk()
        ? (payload.value.attachments ?? [])
        : []
      // Idempotent by id: a redelivered envelope is a no-op (no duplicate
      // row). A brand-new message is stored AND injected so the running turn
      // can pivot to it.
      await persistUserPrompt(
        deps,
        tenant,
        sid,
        text,
        messageId,
        attachments,
        item.source,
      )
      // Only a text prompt continues the running turn; an attachment-only
      // message is now in the chain (its file parts render via history) and is
      // picked up by the next turn rather than injected as empty content.
      if (text !== '') injected.push(text)
      continue
    }
    await persistEvent(deps, tenant, sid, item.payload)
  }
  return injected
}

/** Resolved attachment ref carried in the trigger payload. */
interface PromptAttachment {
  code: string
  name: string
  mime: string
  size: number
}

/** Persist an injected user prompt as a chained `role=user` message.
 *
 * The mailbox is the SINGLE writer of the prompt chain: the HTTP Prompt route
 * publishes the envelope (with a pre-minted message id) and never touches the
 * DB, so every chain append happens here, under the session's run lease. That
 * serializes prompt-vs-prompt and prompt-vs-turn appends — the previous
 * double-write (route wrote the row, then this idempotently no-op'd) left the
 * route's `tip -> insert -> setTip` exposed to interleaving with `persistStep`.
 */
export async function persistUserPrompt(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  text: string,
  messageId?: string,
  attachments: PromptAttachment[] = [],
  source = '',
): Promise<void> {
  const id =
    messageId !== undefined && messageId !== '' ? messageId : randomUUID()
  const tip = await Sessions.tip(deps.db, tenant, sid)
  const tipId = tip.isErr() ? null : tip.value
  // Idempotent by id: a redelivered envelope (JetStream at-least-once) makes
  // this a no-op rather than a duplicate row.
  const created = await Messages.insertWithId(
    deps.db,
    tenant,
    id,
    'user',
    tipId,
    source,
  )
  if (created.isErr()) return
  if (!created.value) return // already persisted (redelivery)
  // File parts first (in payload order), then the text part — the same layout
  // the route used to write, so the read API/UI renders identically.
  let seq = 0
  for (const att of attachments) {
    await Parts.insert(deps.db, tenant, id, 'file', seq++, att)
  }
  if (text !== '') {
    await Parts.insert(deps.db, tenant, id, 'text', seq++, { text })
  }
  await Sessions.setTip(deps.db, tenant, sid, id)
  fireAndForget(appendSessionId(deps.bus, tenant, sid, id), 'appendSessionIds')
  const preview =
    text !== ''
      ? text
      : attachments.length > 0
        ? `[${attachments.length} attachment(s)]`
        : ''
  projectMessageFact(
    deps.bus,
    tenant,
    sid,
    factFromPersist(nowStr(), 'user', preview),
  )
  // Real-time signal: this message is now IN the chain, with its authoritative
  // id and anchor. Run-less so it passes watchSession's live-run filter.
  pushMessageAdded(deps.bus, tenant, sid, {
    messageId: id,
    prevId: tipId ?? '',
    role: 'user',
    streaming: false,
    source,
  })
}

/** Append a completed step to the in-memory message list for the next step. */
export function appendStep(
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

export async function loadHistory(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<ModelMessage[]> {
  const tipRes = await Sessions.tip(deps.db, tenant, sid)
  const tipId = tipRes.isErr() ? null : tipRes.value
  if (tipId === null) return []

  // Cache hit: use the cached id list to fetch rows + parts directly.
  const cached = await getSessionIds(deps.bus, tenant, sid)
  if (cached !== null) {
    const rows = await Messages.byIds(deps.db, tenant, cached)
    const parts = await Parts.listByMessages(deps.db, tenant, cached)
    if (rows.isOk() && parts.isOk()) {
      return spliceContext(rows.value, parts.value)
    }
  }

  // Cache miss: bounded scan from tip, then backfill.
  const chain = await Messages.chain(deps.db, tenant, tipId, 100_000, null)
  if (chain.isErr()) return []
  const ids = chain.value.map(m => m.id)
  const parts = await Parts.listByMessages(deps.db, tenant, ids)
  if (parts.isErr()) return []

  // Backfill the cache with the full bounded id list.
  fireAndForget(putSessionIds(deps.bus, tenant, sid, ids), 'putSessionIds')

  return spliceContext(chain.value, parts.value)
}
