import type { FilePartData, MessageRow } from '@easylab-agent/schema'
import { and, eq, inArray } from 'drizzle-orm'
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
    tenant: string,
    role: MessageRole,
    prevId: string | null,
  ): ResultAsync<string, string> {
    const id = uuid()
    return this.insertWithId(db, tenant, id, role, prevId).map(() => id)
  },

  /**
   * Insert a message with a CLIENT-SUPPLIED id, idempotently.
   *
   * The id is the single logical identity of a user message: a caller that has
   * already persisted it (or verified it does not exist) can hand the same id
   * to another code path and the second insert is a no-op. This closes the
   * duplicate-write hole where the HTTP Prompt route AND the agent's mailbox
   * handlers could each insert a `role=user` row for the same logical message.
   *
   * Returns true when a row was actually created, false when the id already
   * existed (deduplicated).
   */
  insertWithId(
    db: Db,
    tenant: string,
    id: string,
    role: MessageRole,
    prevId: string | null,
  ): ResultAsync<boolean, string> {
    return q(
      () =>
        rawAll(
          db,
          `INSERT INTO messages (id, tenant, role, prev_id, created_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(id) DO NOTHING
           RETURNING id`,
          [id, tenant, role, prevId, nowStr()],
        ).then(rows => rows.length > 0),
      'insert message idempotent',
    )
  },

  get(
    db: Db,
    tenant: string,
    id: string,
  ): ResultAsync<MessageRow | null, string> {
    return q(
      () =>
        db
          .select()
          .from(messages)
          .where(and(eq(messages.tenant, tenant), eq(messages.id, id)))
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
    tenant: string,
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
           SELECT id, prev_id FROM messages WHERE id = ? AND tenant = ?
           UNION
           SELECT m.id, m.prev_id
           FROM messages m JOIN chain c ON m.id = c.prev_id
           WHERE m.tenant = ?
         )
         SELECT 1 FROM chain WHERE id = ? LIMIT 1`,
        [tipId, tenant, tenant, targetId],
      )
      return rows.length > 0
    }, 'message in chain')
  },

  /** Messages for a set of ids, oldest-first by chain order (cache hit path). */
  byIds(
    db: Db,
    tenant: string,
    ids: string[],
  ): ResultAsync<ChainMessage[], string> {
    if (ids.length === 0) {
      return ResultAsync.fromSafePromise(Promise.resolve<ChainMessage[]>([]))
    }
    return q(async () => {
      const rows = await db
        .select()
        .from(messages)
        .where(and(eq(messages.tenant, tenant), inArray(messages.id, ids)))
      const byId = new Map(rows.map(r => [r.id, r]))
      const ordered = ids.flatMap(id => {
        const r = byId.get(id)
        return r === undefined ? [] : [toRow(r)]
      })
      const contentByMsg = await textContentByMessages(db, tenant, ids)
      const toolPartsByMsg = await toolPartsByMessages(db, tenant, ids)
      const filePartsByMsg = await filePartsByMessages(db, tenant, ids)
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
   */
  chain(
    db: Db,
    tenant: string,
    tipId: string | null,
    limit: number,
    before: string | null,
  ): ResultAsync<ChainMessage[], string> {
    return rawChainRows(db, tenant, tipId, limit, before)
      .andThen(rows => {
        const parsed = ChainRowSchema.array().safeParse(rows)
        return parsed.success
          ? ok(parsed.data)
          : err(
              `query message chain: schema mismatch: ${z.treeifyError(parsed.error)}`,
            )
      })
      .andThen(rows => hydrateChain(db, tenant, rows))
  },

  deltaSince(
    db: Db,
    tenant: string,
    tipId: string | null,
    anchorId: string,
    limit: number,
  ): ResultAsync<
    {
      messages: ChainMessage[]
      anchorReached: boolean
      reachedRoot: boolean
    },
    string
  > {
    if (tipId === null || tipId === '') {
      return ResultAsync.fromSafePromise(
        Promise.resolve({
          messages: [],
          anchorReached: false,
          reachedRoot: true,
        }),
      )
    }
    if (anchorId === tipId) {
      return ResultAsync.fromSafePromise(
        Promise.resolve({
          messages: [],
          anchorReached: true,
          reachedRoot: false,
        }),
      )
    }
    return q(async () => {
      // Pull `limit + 1` newest rows: the extra one lets us tell "anchor is the
      // next (older) row" from "we merely hit the limit".
      const rows = await rawAll(
        db,
        `WITH RECURSIVE chain AS (
           SELECT id, role, prev_id, created_at, 0 AS depth
           FROM messages WHERE id = ? AND tenant = ?
           UNION
           SELECT m.id, m.role, m.prev_id, m.created_at, c.depth + 1
           FROM messages m JOIN chain c ON m.id = c.prev_id
           WHERE m.tenant = ? AND c.depth < ?
         )
         SELECT id, role, prev_id, created_at, depth
         FROM chain ORDER BY depth ASC`,
        [tipId, tenant, tenant, limit],
      )
      const parsed = z
        .object({
          id: z.string(),
          role: z.string(),
          prev_id: z.string().nullable(),
          created_at: z.string(),
          depth: z.number(),
        })
        .array()
        .safeParse(rows)
      if (!parsed.success) {
        throw new Error(
          `query delta: schema mismatch: ${z.treeifyError(parsed.error)}`,
        )
      }
      const all = parsed.data // newest-first (depth ASC)
      const anchorIdx = all.findIndex(r => r.id === anchorId)
      if (anchorIdx >= 0) {
        return {
          rows: all.slice(0, anchorIdx),
          anchorReached: true,
          reachedRoot: false,
        }
      }
      const reachedRoot = all.some(r => r.prev_id === null || r.prev_id === '')
      return { rows: all, anchorReached: false, reachedRoot }
    }, 'query message delta').andThen(r =>
      hydrateChain(db, tenant, r.rows).map(messages => ({
        // Return oldest-first (the chain order the UI expects).
        messages: [...messages].reverse(),
        anchorReached: r.anchorReached,
        reachedRoot: r.reachedRoot,
      })),
    )
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
  tenant: string,
  tipId: string | null,
  limit: number,
  before: string | null,
): ResultAsync<readonly unknown[], string> {
  return q(async () => {
    let cursor: string | null
    if (before !== null) {
      const bm = await rawAll(
        db,
        `SELECT prev_id FROM messages WHERE id = ? AND tenant = ? LIMIT 1`,
        [before, tenant],
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
         FROM messages WHERE id = ? AND tenant = ?
         UNION
         SELECT m.id, m.role, m.prev_id, m.created_at, c.depth + 1
         FROM messages m JOIN chain c ON m.id = c.prev_id
         WHERE m.tenant = ?
       )
       SELECT id, role, prev_id, created_at
       FROM chain WHERE depth < ?
       ORDER BY depth DESC`,
      [cursor, tenant, tenant, limit],
    )
  }, 'query message chain')
}

/** Attach per-message text/summary content to validated chain rows. */
function hydrateChain(
  db: Db,
  tenant: string,
  raw: Array<z.infer<typeof ChainRowSchema>>,
): ResultAsync<ChainMessage[], string> {
  return q(async () => {
    const chainMsgs = raw.map(toChain)
    const ids = chainMsgs.map(m => m.id)
    const contentByMsg = await textContentByMessages(db, tenant, ids)
    const toolPartsByMsg = await toolPartsByMessages(db, tenant, ids)
    const filePartsByMsg = await filePartsByMessages(db, tenant, ids)
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
  tenant: string,
  ids: string[],
): Promise<Map<string, string>> {
  const out = new Map<string, string>()
  if (ids.length === 0) return out
  const rows = await db
    .select()
    .from(parts)
    .where(and(eq(parts.tenant, tenant), inArray(parts.messageId, ids)))
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
  tenant: string,
  ids: string[],
): Promise<Map<string, FilePartData[]>> {
  const out = new Map<string, FilePartData[]>()
  if (ids.length === 0) return out
  const rows = await db
    .select()
    .from(parts)
    .where(and(eq(parts.tenant, tenant), inArray(parts.messageId, ids)))
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
  tenant: string,
  ids: string[],
): Promise<Map<string, ChainMessage['tool_parts']>> {
  const out = new Map<string, ChainMessage['tool_parts']>()
  if (ids.length === 0) return out
  const rows = await db
    .select()
    .from(parts)
    .where(and(eq(parts.tenant, tenant), inArray(parts.messageId, ids)))
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
