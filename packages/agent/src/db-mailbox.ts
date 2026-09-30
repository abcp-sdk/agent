import type { MailboxRow } from '@abcp-agent/schema'
import { and, desc, eq, lt, or } from 'drizzle-orm'
import type { ResultAsync } from 'neverthrow'
import { z } from 'zod'
import type { Db } from './db-client.js'
import { dbBackend, nowStr, q, rawAll, uuid } from './db-client.js'
import { mailbox } from './db-schema.js'

const DrainedMailboxRowSchema = z.object({
  id: z.string(),
  tenant: z.string().optional(),
  session_name: z.string(),
  msg_type: z.string(),
  source: z.string().nullable().optional(),
  payload: z.string(),
  effective_at: z.string().nullable().optional(),
  status: z.string(),
  created_at: z.string(),
  consumed_at: z.string().nullable().optional(),
  seq: z.number().nullable().optional(),
})

const toRow = (r: typeof mailbox.$inferSelect): MailboxRow => ({
  id: r.id,
  session_name: r.sessionName,
  msg_type: r.msgType,
  source: r.source,
  payload: r.payload,
  effective_at: r.effectiveAt,
  status: r.status,
  created_at: r.createdAt,
  consumed_at: r.consumedAt,
  seq: r.seq,
})

/** A drained mailbox row with the tenant it belongs to (for turn routing). */
export interface DrainedMailbox extends MailboxRow {
  tenant: string
}

export const Mailbox = {
  enqueue(
    db: Db,
    tenant: string,
    sessionName: string,
    msgType: string,
    payload: unknown,
    source = '',
  ): ResultAsync<string, string> {
    const id = uuid()
    return q(
      () =>
        db.insert(mailbox).values({
          id,
          tenant,
          sessionName,
          msgType,
          source,
          payload: JSON.stringify(payload ?? {}),
          status: 'pending',
          createdAt: nowStr(),
        }),
      'enqueue mailbox',
    ).map(() => id)
  },

  /**
   * Insert with a producer-supplied id, ignoring a duplicate row. The agent's
   * NATS mailbox consumer uses this: a JetStream redelivery carries the same
   * envelope id, so the second insert is a no-op and the message is never
   * processed twice.
   */
  enqueueIdempotent(
    db: Db,
    tenant: string,
    id: string,
    sessionName: string,
    msgType: string,
    payload: unknown,
    source = '',
  ): ResultAsync<string, string> {
    return q(
      () =>
        db
          .insert(mailbox)
          .values({
            id,
            tenant,
            sessionName,
            msgType,
            source,
            payload: JSON.stringify(payload ?? {}),
            status: 'pending',
            createdAt: nowStr(),
          })
          .onConflictDoNothing(),
      'enqueue mailbox idempotent',
    ).map(() => id)
  },

  /**
   * NEWEST-FIRST page of a session's mailbox, for infinite scroll. Returns up
   * to `limit` rows older than `before` (exclusive; '' = the newest page) plus
   * whether more (older) rows remain.
   *
   * Ordering is `created_at DESC, id DESC`: `id` is a unique UUID, so it is a
   * stable total-order tiebreaker for identical timestamps. `before` resolves
   * to that `(created_at, id)` keyset, so paging never skips or repeats a row
   * even as new entries arrive at the head between pages.
   */
  listPage(
    db: Db,
    tenant: string,
    sessionName: string,
    limit: number,
    before: string,
  ): ResultAsync<{ rows: MailboxRow[]; hasMore: boolean }, string> {
    return q(async () => {
      const capped =
        Number.isFinite(limit) && limit > 0 ? Math.min(limit, 200) : 50
      const take = capped + 1 // one extra row tells us whether more remain
      const base = and(
        eq(mailbox.tenant, tenant),
        eq(mailbox.sessionName, sessionName),
      )
      let where = base
      if (before !== '') {
        const anchor = await db
          .select({ createdAt: mailbox.createdAt, id: mailbox.id })
          .from(mailbox)
          .where(and(base, eq(mailbox.id, before)))
          .limit(1)
          .then(rows => rows[0] ?? null)
        if (anchor !== null) {
          where = and(
            base,
            or(
              lt(mailbox.createdAt, anchor.createdAt),
              and(
                eq(mailbox.createdAt, anchor.createdAt),
                lt(mailbox.id, anchor.id),
              ),
            ),
          )!
        }
      }
      const rows = await db
        .select()
        .from(mailbox)
        .where(where)
        .orderBy(desc(mailbox.createdAt), desc(mailbox.id))
        .limit(take)
      const hasMore = rows.length > capped
      return { rows: rows.slice(0, capped).map(toRow), hasMore }
    }, 'list mailbox page')
  },

  /**
   * Sessions (with their tenant) that still have pending mailbox items worth
   * RECOVERING (startup recovery + periodic backstop).
   *
   * `types` narrows the scan to the message types that actually WAKE a turn
   * (trigger/compact). A session holding only context-only rows (e.g. a lone
   * `event`) must NOT be recovered: re-running its turn would claim the lease,
   * drain the event, find no trigger, and emit a spurious idle. When `types` is
   * omitted, EVERY pending session is returned (all types).
   */
  pendingSessions(
    db: Db,
    types?: readonly string[],
  ): ResultAsync<{ tenant: string; session_name: string }[], string> {
    const list = types ?? []
    let sql = `SELECT DISTINCT tenant, session_name FROM mailbox WHERE status = 'pending'`
    const params: unknown[] = []
    if (list.length > 0) {
      // `rawAll` converts `?` → `$n` for pg; keep the portable form here.
      sql += ` AND msg_type IN (${list.map(() => '?').join(', ')})`
      params.push(...list)
    }
    return q(
      () =>
        rawAll(db, sql, params).then(rows =>
          rows.map(r => ({
            tenant: String(r.tenant ?? 'default'),
            session_name: String(r.session_name),
          })),
        ),
      'pending sessions',
    )
  },

  /** True when the tenant+session has at least one PENDING mailbox row.
   *  Non-consuming (unlike drainOne), used by the turn loop to decide whether
   *  to KEEP holding the run lease for a chained continuation instead of
   *  releasing + re-claiming (which flapped the busy/idle status). */
  hasPending(
    db: Db,
    tenant: string,
    sessionName: string,
  ): ResultAsync<boolean, string> {
    const isPg = dbBackend(db) === 'pg'
    const now = nowStr()
    // Must mirror `drainAll`'s deliverability gate, otherwise a future-dated
    // row would read "pending" while `drainAll` returns empty — an infinite
    // hold-the-lease loop.
    const sql = isPg
      ? `SELECT 1 AS one FROM mailbox WHERE tenant = $1 AND session_name = $2 AND status = 'pending' AND (effective_at IS NULL OR effective_at <= $3) LIMIT 1`
      : `SELECT 1 AS one FROM mailbox WHERE tenant = ? AND session_name = ? AND status = 'pending' AND (effective_at IS NULL OR effective_at <= ?) LIMIT 1`
    return q(
      () =>
        rawAll(db, sql, [tenant, sessionName, now]).then(
          rows => rows.length > 0,
        ),
      'has pending mailbox',
    )
  },

  /**
   * Atomically pop the next pending item (ordered) for one tenant+session.
   * Kept for single-item callers/tests; delegates to `drainAll`.
   */
  drainOne(
    db: Db,
    tenant: string,
    sessionName: string,
  ): ResultAsync<DrainedMailbox | null, string> {
    return this.drainAll(db, tenant, sessionName).map(rows => rows[0] ?? null)
  },

  /**
   * Atomically pop EVERY deliverable pending item (ordered) for one
   * tenant+session in a single statement. Ordered by the same key as
   * `drainOne` so a batch preserves arrival order.
   *
   * The per-session run lease guarantees exactly one writer drains a session's
   * mailbox at a time, so a whole-batch UPDATE needs no `SKIP LOCKED` (and
   * SQLite is single-writer anyway). `effective_at` gates delivery: a row whose
   * effective time is in the future stays pending (NULL = deliver now). A
   * scheduled wake-up is NOT yet implemented, so writing a future
   * `effective_at` without a timer would strand the row until the next event —
   * the column is currently unused.
   */
  drainAll(
    db: Db,
    tenant: string,
    sessionName: string,
  ): ResultAsync<DrainedMailbox[], string> {
    const now = nowStr()
    const pgSQL = `UPDATE mailbox SET status = 'consumed', consumed_at = $3
       WHERE tenant = $1 AND session_name = $2 AND status = 'pending'
         AND (effective_at IS NULL OR effective_at <= $3)
       RETURNING id, tenant, session_name, msg_type, source, payload, effective_at, status, created_at, consumed_at, seq`
    const sqliteSQL = `UPDATE mailbox SET status = 'consumed', consumed_at = ?
       WHERE tenant = ? AND session_name = ? AND status = 'pending'
         AND (effective_at IS NULL OR effective_at <= ?)
       RETURNING id, tenant, session_name, msg_type, source, payload, effective_at, status, created_at, consumed_at, seq`
    const isPg = dbBackend(db) === 'pg'
    return q(
      () =>
        (isPg
          ? rawAll(db, pgSQL, [tenant, sessionName, now])
          : rawAll(db, sqliteSQL, [now, tenant, sessionName, now])
        ).then(res => {
          const rows = res.flatMap(r => {
            const parsed = DrainedMailboxRowSchema.safeParse(r)
            if (!parsed.success) return []
            const d = parsed.data
            return [
              {
                id: d.id,
                tenant: d.tenant ?? tenant,
                session_name: d.session_name,
                msg_type: d.msg_type,
                source: d.source ?? '',
                payload: d.payload,
                effective_at: d.effective_at ?? null,
                status: d.status,
                created_at: d.created_at,
                consumed_at: d.consumed_at ?? null,
                seq: d.seq ?? null,
              } satisfies DrainedMailbox,
            ]
          })
          // UPDATE ... RETURNING does not guarantee row order; sort by the same
          // key the WHERE selected on so the batch is deterministic.
          rows.sort((a, b) => {
            const ak = a.effective_at ?? a.created_at
            const bk = b.effective_at ?? b.created_at
            if (ak !== bk) return ak < bk ? -1 : 1
            const as = a.seq ?? 0
            const bs = b.seq ?? 0
            if (as !== bs) return as - bs
            return a.created_at < b.created_at
              ? -1
              : a.created_at > b.created_at
                ? 1
                : 0
          })
          return rows
        }),
      'drain mailbox all',
    )
  },

  hasPendingInterrupt(
    db: Db,
    tenant: string,
    sessionName: string,
  ): ResultAsync<boolean, string> {
    const pgSQL = `SELECT EXISTS(
         SELECT 1 FROM mailbox
         WHERE tenant = $1 AND session_name = $2 AND msg_type = 'interrupt' AND status = 'pending'
       ) AS ok`
    const sqliteSQL = `SELECT EXISTS(
         SELECT 1 FROM mailbox
         WHERE tenant = ? AND session_name = ? AND msg_type = 'interrupt' AND status = 'pending'
       ) AS ok`
    const isPg = dbBackend(db) === 'pg'
    return q(
      () =>
        (isPg
          ? rawAll(db, pgSQL, [tenant, sessionName])
          : rawAll(db, sqliteSQL, [tenant, sessionName])
        ).then(res => {
          const ok = res[0]?.ok
          // pg returns boolean; sqlite returns 1/0
          return ok === true || ok === 1 || ok === '1'
        }),
      'has pending interrupt',
    )
  },

  /**
   * Retention sweep: consumed rows are audit-only history, so they are
   * pruned after a retention window. `consumed_at` is `YYYY-MM-DD HH:MM:SS`
   * (lexicographically comparable). Returns the deleted row count.
   */
  purgeConsumed(db: Db, retentionDays: number): ResultAsync<number, string> {
    const cutoff = new Date(Date.now() - retentionDays * 86_400_000)
      .toISOString()
      .slice(0, 19)
      .replace('T', ' ')
    const isPg = dbBackend(db) === 'pg'
    const sql = isPg
      ? `DELETE FROM mailbox WHERE status = 'consumed' AND consumed_at < $1 RETURNING id`
      : `DELETE FROM mailbox WHERE status = 'consumed' AND consumed_at < ? RETURNING id`
    return q(
      () => rawAll(db, sql, [cutoff]).then(rows => rows.length),
      'purge consumed mailbox',
    )
  },
}
