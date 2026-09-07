import type { MailboxRow } from '@easylab-agent/schema'
import { eq } from 'drizzle-orm'
import type { ResultAsync } from 'neverthrow'
import { z } from 'zod'
import type { Db } from './db-client.js'
import { dbBackend, nowStr, q, rawAll, uuid } from './db-client.js'
import { mailbox } from './db-schema.js'

const DrainedMailboxRowSchema = z.object({
  id: z.string(),
  session_name: z.string(),
  msg_type: z.string(),
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
  payload: r.payload,
  effective_at: r.effectiveAt,
  status: r.status,
  created_at: r.createdAt,
  consumed_at: r.consumedAt,
  seq: r.seq,
})

export const Mailbox = {
  enqueue(
    db: Db,
    sessionName: string,
    msgType: string,
    payload: unknown,
  ): ResultAsync<string, string> {
    const id = uuid()
    return q(
      () =>
        db.insert(mailbox).values({
          id,
          sessionName,
          msgType,
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
    id: string,
    sessionName: string,
    msgType: string,
    payload: unknown,
  ): ResultAsync<string, string> {
    return q(
      () =>
        db
          .insert(mailbox)
          .values({
            id,
            sessionName,
            msgType,
            payload: JSON.stringify(payload ?? {}),
            status: 'pending',
            createdAt: nowStr(),
          })
          .onConflictDoNothing(),
      'enqueue mailbox idempotent',
    ).map(() => id)
  },

  list(db: Db, sessionName: string): ResultAsync<MailboxRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(mailbox)
          .where(eq(mailbox.sessionName, sessionName))
          .then(rows => rows.map(toRow)),
      'list mailbox',
    )
  },

  /** Sessions that still have pending mailbox items (startup recovery). */
  pendingSessions(db: Db): ResultAsync<string[], string> {
    return q(
      () =>
        rawAll(
          db,
          `SELECT DISTINCT session_name FROM mailbox WHERE status = 'pending'`,
        ).then(rows => rows.map(r => String(r.session_name))),
      'pending sessions',
    )
  },

  /**
   * Atomically pop the next pending item (ordered). The UPDATE-with-subquery
   * keeps concurrent replicas from consuming the same row. On Postgres the
   * subquery takes `FOR UPDATE SKIP LOCKED` so replicas never fight; SQLite is
   * single-writer (WAL), so the UPDATE itself serializes the pop — SKIP LOCKED
   * is a no-op there and is omitted.
   */
  drainOne(
    db: Db,
    sessionName: string,
  ): ResultAsync<MailboxRow | null, string> {
    const now = nowStr()
    const pgSQL = `UPDATE mailbox SET status = 'consumed', consumed_at = $2
       WHERE id = (
         SELECT id FROM mailbox
         WHERE session_name = $1 AND status = 'pending'
         ORDER BY COALESCE(effective_at, created_at) ASC, COALESCE(seq, 0) ASC, created_at ASC
         LIMIT 1
         FOR UPDATE SKIP LOCKED
       )
       RETURNING id, session_name, msg_type, payload, effective_at, status, created_at, consumed_at, seq`
    const sqliteSQL = `UPDATE mailbox SET status = 'consumed', consumed_at = ?
       WHERE id = (
         SELECT id FROM mailbox
         WHERE session_name = ? AND status = 'pending'
         ORDER BY COALESCE(effective_at, created_at) ASC, COALESCE(seq, 0) ASC, created_at ASC
         LIMIT 1
       )
       RETURNING id, session_name, msg_type, payload, effective_at, status, created_at, consumed_at, seq`
    const isPg = dbBackend(db) === 'pg'
    return q(
      () =>
        (isPg
          ? rawAll(db, pgSQL, [sessionName, now])
          : rawAll(db, sqliteSQL, [now, sessionName])
        ).then(res => {
          const r = res[0]
          if (r === undefined) return null
          const parsed = DrainedMailboxRowSchema.safeParse(r)
          if (!parsed.success) return null
          const d = parsed.data
          return {
            id: d.id,
            session_name: d.session_name,
            msg_type: d.msg_type,
            payload: d.payload,
            effective_at: d.effective_at ?? null,
            status: d.status,
            created_at: d.created_at,
            consumed_at: d.consumed_at ?? null,
            seq: d.seq ?? null,
          } satisfies MailboxRow
        }),
      'drain mailbox one',
    )
  },

  hasPendingInterrupt(
    db: Db,
    sessionName: string,
  ): ResultAsync<boolean, string> {
    const pgSQL = `SELECT EXISTS(
         SELECT 1 FROM mailbox
         WHERE session_name = $1 AND msg_type = 'interrupt' AND status = 'pending'
       ) AS ok`
    const sqliteSQL = `SELECT EXISTS(
         SELECT 1 FROM mailbox
         WHERE session_name = ? AND msg_type = 'interrupt' AND status = 'pending'
       ) AS ok`
    const isPg = dbBackend(db) === 'pg'
    return q(
      () =>
        (isPg
          ? rawAll(db, pgSQL, [sessionName])
          : rawAll(db, sqliteSQL, [sessionName])
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
