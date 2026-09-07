import type { FilePartData, MessageRow } from '@easylab-agent/schema'
import { eq, inArray } from 'drizzle-orm'
import { err, ok, ResultAsync } from 'neverthrow'
import { z } from 'zod'
import type { Db } from './db-client.js'
import { nowStr, q, rawAll, uuid } from './db-client.js'
import { messages, parts } from './db-schema.js'
import {
  FilePartDataSchema,
  parse,
  SummaryPartDataSchema,
  TextPartDataSchema,
  ToolPartDataSchema,
  ToolResultPartDataSchema,
} from './json.js'

const toRow = (r: typeof messages.$inferSelect): MessageRow => ({
  id: r.id,
  role: r.role,
  prev_id: r.prevId,
  created_at: r.createdAt,
})

export interface ChainMessage extends MessageRow {
  /** Pure-text view: text parts for normal messages, the summary for a
   *  compaction message (used by the read API/UI). */
  content: string
  /** Structured parts for tool calls (name/input/result) so the read API/UI
   *  can render tool steps; empty for plain text messages. */
  tool_parts: Array<{
    type: 'tool'
    name: string
    input: unknown
    result: string
    metadata?: unknown
  }>
  /** Structured user file attachments (code/name/mime/size), rendered as
   *  distinct attachment parts by the read API/UI. */
  file_parts: FilePartData[]
}

export type MessageRole = 'user' | 'assistant' | 'event' | 'compaction'

export const Messages = {
  insert(
    db: Db,
    role: MessageRole,
    prevId: string | null,
  ): ResultAsync<string, string> {
    const id = uuid()
    return q(
      () =>
        db.insert(messages).values({
          id,
          role,
          prevId,
          createdAt: nowStr(),
        }),
      'insert message',
    ).map(() => id)
  },

  get(db: Db, id: string): ResultAsync<MessageRow | null, string> {
    return q(
      () =>
        db
          .select()
          .from(messages)
          .where(eq(messages.id, id))
          .limit(1)
          .then(rows => {
            const r = rows[0]
            return r === undefined ? null : toRow(r)
          }),
      'get message',
    )
  },

  /**
   * True when `target` is reachable by walking `prev_id` backwards from
   * `tip` — i.e. `target` belongs to this session's chain (COW forks share
   * ancestors, so a message from another session's chain must not move our
   * tip onto it).
   */
  isInChain(
    db: Db,
    tipId: string,
    targetId: string,
  ): ResultAsync<boolean, string> {
    if (tipId === targetId) {
      return ResultAsync.fromSafePromise(Promise.resolve(true))
    }
    return q(async () => {
      const rows = await rawAll(
        db,
        `WITH RECURSIVE chain AS (
           SELECT id, prev_id FROM messages WHERE id = ?
           UNION
           SELECT m.id, m.prev_id
           FROM messages m JOIN chain c ON m.id = c.prev_id
         )
         SELECT 1 FROM chain WHERE id = ? LIMIT 1`,
        [tipId, targetId],
      )
      return rows.length > 0
    }, 'message in chain')
  },

  /** Messages for a set of ids, oldest-first by chain order (cache hit path). */
  byIds(db: Db, ids: string[]): ResultAsync<ChainMessage[], string> {
    if (ids.length === 0) {
      return ResultAsync.fromSafePromise(Promise.resolve<ChainMessage[]>([]))
    }
    return q(async () => {
      const rows = await db
        .select()
        .from(messages)
        .where(inArray(messages.id, ids))
      const byId = new Map(rows.map(r => [r.id, r]))
      const ordered = ids.flatMap(id => {
        const r = byId.get(id)
        return r === undefined ? [] : [toRow(r)]
      })
      const contentByMsg = await textContentByMessages(db, ids)
      const toolPartsByMsg = await toolPartsByMessages(db, ids)
      const filePartsByMsg = await filePartsByMessages(db, ids)
      return ordered.map(m => ({
        ...m,
        content: contentByMsg.get(m.id) ?? '',
        tool_parts: toolPartsByMsg.get(m.id) ?? [],
        file_parts: filePartsByMsg.get(m.id) ?? [],
      }))
    }, 'messages by ids')
  },

  /**
   * Walk the prev_id chain backwards from the tip (or from `before`'s
   * predecessor), newest → oldest, up to `limit`, then return oldest-first.
   *
   * Uses a single `WITH RECURSIVE` query instead of an N+1 loop: the whole
   * chain is pulled back in one round-trip. The recursive body joins on
   * `m.id = c.prev_id`, which is served by `idx_messages_prev` so each step
   * is an index scan. `UNION` (rather than `UNION ALL`) dedupes by row, which
   * also protects against a malformed `prev_id` cycle looping forever; `depth
   * < limit` additionally bounds the walk.
   *
   * Raw rows are validated with `safeParse` (a schema mismatch surfaces as a
   * Result error, never a throw). Each row's `content` is derived by
   * concatenating its text parts in `seq` order — messages carry no inline
   * content column.
   */
  chain(
    db: Db,
    tipId: string | null,
    limit: number,
    before: string | null,
  ): ResultAsync<ChainMessage[], string> {
    return rawChainRows(db, tipId, limit, before)
      .andThen(rows => {
        const parsed = ChainRowSchema.array().safeParse(rows)
        return parsed.success
          ? ok(parsed.data)
          : err(
              `query message chain: schema mismatch: ${z.treeifyError(parsed.error)}`,
            )
      })
      .andThen(rows => hydrateChain(db, rows))
  },
}

/** Raw row shape returned by the recursive CTE (camel-cased via drizzle). */
const ChainRowSchema = z.object({
  id: z.string(),
  role: z.string(),
  prev_id: z.string().nullable(),
  created_at: z.string(),
})

/**
 * Resolve the walk cursor (the tip, or `before`'s predecessor) and fetch the
 * raw recursive-CTE rows. Rows come back untyped (`unknown[]`) — validation
 * is the caller's explicit safeParse step.
 */
function rawChainRows(
  db: Db,
  tipId: string | null,
  limit: number,
  before: string | null,
): ResultAsync<readonly unknown[], string> {
  return q(async () => {
    let cursor: string | null
    if (before !== null) {
      const bm = await rawAll(
        db,
        `SELECT prev_id FROM messages WHERE id = ? LIMIT 1`,
        [before],
      )
      cursor = (bm[0]?.prev_id as string | null) ?? null
    } else {
      // COW: the chain is walked purely on `prev_id`, starting from the
      // session's tip. A fork shares parent messages because its tip points
      // at the same message row (zero-copy fork).
      cursor = tipId
    }
    if (cursor === null) return []
    return await rawAll(
      db,
      `WITH RECURSIVE chain AS (
         SELECT id, role, prev_id, created_at,
                0 AS depth
         FROM messages WHERE id = ?
         UNION
         SELECT m.id, m.role, m.prev_id, m.created_at, c.depth + 1
         FROM messages m JOIN chain c ON m.id = c.prev_id
       )
       SELECT id, role, prev_id, created_at
       FROM chain WHERE depth < ?
       ORDER BY depth DESC`,
      [cursor, limit],
    )
  }, 'query message chain')
}

/** Attach per-message text/summary content to validated chain rows. */
function hydrateChain(
  db: Db,
  raw: Array<z.infer<typeof ChainRowSchema>>,
): ResultAsync<ChainMessage[], string> {
  return q(async () => {
    const chainMsgs = raw.map(toChain)
    const ids = chainMsgs.map(m => m.id)
    const contentByMsg = await textContentByMessages(db, ids)
    const toolPartsByMsg = await toolPartsByMessages(db, ids)
    const filePartsByMsg = await filePartsByMessages(db, ids)
    return chainMsgs.map(m => ({
      ...m,
      content: contentByMsg.get(m.id) ?? '',
      tool_parts: toolPartsByMsg.get(m.id) ?? [],
      file_parts: filePartsByMsg.get(m.id) ?? [],
    }))
  }, 'hydrate message chain')
}

const toChain = (r: z.infer<typeof ChainRowSchema>): ChainMessage => ({
  id: r.id,
  role: r.role,
  content: '',
  tool_parts: [],
  file_parts: [],
  prev_id: r.prev_id,
  created_at: r.created_at,
})

/** Message id → concatenated text parts (seq order). */
async function textContentByMessages(
  db: Db,
  ids: string[],
): Promise<Map<string, string>> {
  const out = new Map<string, string>()
  if (ids.length === 0) return out
  const rows = await db
    .select()
    .from(parts)
    .where(inArray(parts.messageId, ids))
    .orderBy(parts.messageId, parts.seq)
  for (const p of rows) {
    if (p.type === 'text') {
      const d = parse(TextPartDataSchema, p.data)
      if (d.isOk()) {
        out.set(p.messageId, (out.get(p.messageId) ?? '') + d.value.text)
      }
    } else if (p.type === 'summary') {
      const d = parse(SummaryPartDataSchema, p.data)
      if (d.isOk()) out.set(p.messageId, d.value.summary)
    }
  }
  return out
}

/** Message id → ordered `file` part data (attachment refs), in seq order. */
async function filePartsByMessages(
  db: Db,
  ids: string[],
): Promise<Map<string, FilePartData[]>> {
  const out = new Map<string, FilePartData[]>()
  if (ids.length === 0) return out
  const rows = await db
    .select()
    .from(parts)
    .where(inArray(parts.messageId, ids))
    .orderBy(parts.messageId, parts.seq)
  for (const p of rows) {
    if (p.type !== 'file') continue
    const d = parse(FilePartDataSchema, p.data)
    if (d.isOk()) {
      const list = out.get(p.messageId) ?? []
      list.push(d.value)
      out.set(p.messageId, list)
    }
  }
  return out
}

/** Message id → structured tool-call parts (name/input + paired result). */
async function toolPartsByMessages(
  db: Db,
  ids: string[],
): Promise<Map<string, ChainMessage['tool_parts']>> {
  const out = new Map<string, ChainMessage['tool_parts']>()
  if (ids.length === 0) return out
  const rows = await db
    .select()
    .from(parts)
    .where(inArray(parts.messageId, ids))
    .orderBy(parts.messageId, parts.seq)
  const byMsg = new Map<string, (typeof rows)[number][]>()
  for (const p of rows) {
    const list = byMsg.get(p.messageId) ?? []
    list.push(p)
    byMsg.set(p.messageId, list)
  }
  for (const [messageId, ps] of byMsg) {
    const results = new Map<string, { content: string; metadata?: unknown }>()
    const tools: ChainMessage['tool_parts'] = []
    for (const p of ps) {
      if (p.type === 'tool_result') {
        const d = parse(ToolResultPartDataSchema, p.data)
        if (d.isOk()) {
          results.set(d.value.tool_use_id, {
            content: d.value.content,
            metadata: d.value.metadata,
          })
        }
      }
    }
    for (const p of ps) {
      if (p.type !== 'tool') continue
      const d = parse(ToolPartDataSchema, p.data)
      if (!d.isOk()) continue
      const r = results.get(d.value.id)
      tools.push({
        type: 'tool',
        name: d.value.name,
        input: d.value.input,
        result: r?.content ?? '',
        ...(r?.metadata !== undefined ? { metadata: r.metadata } : {}),
      })
    }
    if (tools.length > 0) out.set(messageId, tools)
  }
  return out
}
