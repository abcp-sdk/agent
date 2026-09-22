import type { PartRow } from '@abcp-agent/schema'
import type { ModelMessage } from 'ai'
import { err, ok, type Result } from 'neverthrow'
import { z } from 'zod'
import { fireAndForget } from './async.js'
import {
  COMPACTION_PREVIEW,
  COMPACTION_ROLE,
  checkpointContent,
  foldQA,
  splitScan,
} from './compaction.js'
import { nowStr } from './db-client.js'
import { type ChainMessage, Messages } from './db-messages.js'
import { Parts } from './db-parts.js'
import { Providers } from './db-providers.js'
import { Sessions } from './db-sessions.js'
import { pushEvent } from './events.js'
import { rebuildHistory } from './history.js'
import { parse, SummaryPartDataSchema, TextPartDataSchema } from './json.js'
import { parseProviderModelRef } from './llm.js'
import type { AgentDeps } from './session-agent.js'
import { factFromPersist, projectMessageFact } from './session-state.js'
import { putSessionIds } from './store.js'
import { estimateTokens } from './token.js'

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
 *
 * The CALLER MUST HOLD the session's run lease (or otherwise be the sole
 * writer of this session's chain). The chain append here is a plain
 * `tip -> insert -> setTip`; running it concurrently with a turn would fork
 * the chain (the cm and a step would share a parent and one becomes an
 * orphan). Manual compaction therefore goes through the mailbox (drained at a
 * step boundary under the lease), and the overflow path already owns the lease.
 *
 * Emits `compacted { reason, ok }` on every non-error outcome so the UI can
 * react: `ok:false` means there was nothing to fold (short history / no
 * configured context window).
 */
export async function compactSession(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  reason: 'manual' | 'overflow' = 'manual',
): Promise<Result<boolean, string>> {
  const tipRes = await Sessions.tip(deps.db, tenant, sid)
  if (tipRes.isErr()) return err(tipRes.error)
  const tipId = tipRes.value
  if (tipId === null) return skip(deps, tenant, sid, reason)

  const sessionRes = await Sessions.get(deps.db, tenant, sid)
  if (sessionRes.isErr()) return err(sessionRes.error)
  const session = sessionRes.value
  const modelId = session === null ? '' : session.model

  const chain = await Messages.chain(deps.db, tenant, tipId, 100_000, null)
  if (chain.isErr()) return err(chain.error)

  const ids = chain.value.map(m => m.id)
  const partsRes = await Parts.listByMessages(deps.db, tenant, ids)
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

  const limit = await contextLimit(deps, tenant, modelId)
  // No configured context window for this provider/model ⇒ cannot compute
  // budgets ⇒ skip compaction (never guess a window).
  if (limit <= 0) return skip(deps, tenant, sid, reason)
  const { tail, folded } = splitScan(entries, limit * 0.2, limit * 0.1)
  if (folded.length === 0) return skip(deps, tenant, sid, reason)

  const summary = foldQA(folded)
  if (summary.trim() === '') return skip(deps, tenant, sid, reason)

  // tail is oldest-first; its first entry marks the verbatim boundary kept
  // after this checkpoint.
  const tailFromId = tail[0]?.id ?? null
  const foldedTokens = folded.reduce(
    (n, e) => n + estimateTokens(e.text) + e.toolCalls * 4,
    0,
  )

  const insert = await Messages.insert(deps.db, tenant, COMPACTION_ROLE, tipId)
  if (insert.isErr()) return err(insert.error)
  const cmId = insert.value
  const part = await Parts.insertSummary(
    deps.db,
    tenant,
    cmId,
    summary,
    tailFromId,
    { reason, foldedCount: folded.length, foldedTokens },
  )
  if (part.isErr()) return err(part.error)
  await Sessions.setTip(deps.db, tenant, sid, cmId)
  // The list preview shows a FIXED sentinel for a compaction tip (the raw
  // summary would leak a wall of folded Q&A into the chat list). Clients map
  // this sentinel to a localized "History compacted" label.
  projectMessageFact(
    deps.bus,
    tenant,
    sid,
    factFromPersist(nowStr(), COMPACTION_ROLE, COMPACTION_PREVIEW),
  )

  // Rewrite the cache to the new bounded context id list (with the cm).
  const chainAfter = await Messages.chain(deps.db, tenant, cmId, 100_000, null)
  if (chainAfter.isOk()) {
    fireAndForget(
      putSessionIds(
        deps.bus,
        tenant,
        sid,
        chainAfter.value.map(m => m.id),
      ),
      'putSessionIds',
    )
  }

  pushEvent(deps.bus, tenant, sid, 'compacted', { reason, ok: true })
  return ok(true)
}

/** Nothing to fold: announce the no-op outcome (the UI may toast for a manual
 *  request) and report `false` to the caller. */
function skip(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  reason: 'manual' | 'overflow',
): Result<boolean, string> {
  pushEvent(deps.bus, tenant, sid, 'compacted', { reason, ok: false })
  return ok(false)
}

async function contextLimit(
  deps: AgentDeps,
  tenant: string,
  modelRef: string,
): Promise<number> {
  // The context window is USER-CONFIGURED per provider model (required at
  // registration). No external catalog, no fallback: an unknown provider/model
  // yields 0, which disables compaction for that session.
  const parsed = parseProviderModelRef(modelRef)
  if (parsed === null) return 0
  const rows = await Providers.list(deps.db, tenant)
  if (rows.isErr()) return 0
  const provider = rows.value.find(r => r.provider_id === parsed.providerId)
  if (provider === undefined) return 0
  const models = parse(
    z.array(
      z.object({
        id: z.string(),
        name: z.string().optional(),
        context_limit: z.number().int(),
      }),
    ),
    provider.models,
  )
  if (models.isErr()) return 0
  const hit = models.value.find(m => m.id === parsed.modelId)
  return hit?.context_limit ?? 0
}
