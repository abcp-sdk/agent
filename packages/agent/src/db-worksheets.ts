import type { WorksheetRow } from '@easylab-agent/schema'
import { eq, and } from 'drizzle-orm'
import { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { nowStr, q } from './db-client.js'
import { worksheets } from './db-schema.js'

const toRow = (r: typeof worksheets.$inferSelect): WorksheetRow => ({
  id: r.id,
  session_name: r.sessionName,
  ext_id: r.extId,
  action: r.action,
  args: r.args,
  title: r.title,
  origin_call_id: r.originCallId,
  status: r.status,
  created_at: r.createdAt,
  decided_at: r.decidedAt,
})

/**
 * Worksheets: durable approval artifacts published by extensions. Never
 * expire, never auto-approve — a row lives until a human decides it. The
 * session FK cascades: deleting a session withdraws its pending worksheets.
 */
export const Worksheets = {
  /** Idempotent insert (worksheet_id is minted by the publishing extension). */
  insert(
    db: Db,
    w: {
      id: string
      sessionName: string
      extId: string
      action: string
      args: string
      title: string
      originCallId?: string | null
    },
  ): ResultAsync<WorksheetRow | null, string> {
    return q(
      () =>
        db
          .insert(worksheets)
          .values({
            id: w.id,
            sessionName: w.sessionName,
            extId: w.extId,
            action: w.action,
            args: w.args,
            title: w.title,
            originCallId: w.originCallId ?? null,
            status: 'pending',
            createdAt: nowStr(),
          })
          .onConflictDoNothing({ target: worksheets.id })
          .returning()
          .then(rows => (rows[0] === undefined ? null : toRow(rows[0]))),
      'insert worksheet',
    )
  },

  get(db: Db, id: string): ResultAsync<WorksheetRow | null, string> {
    return q(
      () =>
        db
          .select()
          .from(worksheets)
          .where(eq(worksheets.id, id))
          .limit(1)
          .then(rows => (rows[0] === undefined ? null : toRow(rows[0]))),
      'get worksheet',
    )
  },

  listBySession(
    db: Db,
    sessionName: string,
    status?: string,
  ): ResultAsync<WorksheetRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(worksheets)
          .where(
            status === undefined
              ? eq(worksheets.sessionName, sessionName)
              : and(
                  eq(worksheets.sessionName, sessionName),
                  eq(worksheets.status, status),
                ),
          )
          .orderBy(worksheets.createdAt)
          .then(rows => rows.map(toRow)),
      'list worksheets by session',
    )
  },

  listByStatus(db: Db, status: string): ResultAsync<WorksheetRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(worksheets)
          .where(eq(worksheets.status, status))
          .orderBy(worksheets.createdAt)
          .then(rows => rows.map(toRow)),
      'list worksheets by status',
    )
  },

  /**
   * Atomic claim for dispatch: pending → dispatching. Returns the row when
   * this caller won the claim, null when another path already took it (or
   * the row is not pending). This is the compare-and-swap that makes
   * double-clicks and concurrent replicas safe.
   */
  claimForDispatch(
    db: Db,
    id: string,
  ): ResultAsync<WorksheetRow | null, string> {
    return q(
      () =>
        db
          .update(worksheets)
          .set({ status: 'dispatching', decidedAt: nowStr() })
          .where(and(eq(worksheets.id, id), eq(worksheets.status, 'pending')))
          .returning()
          .then(rows => (rows[0] === undefined ? null : toRow(rows[0]))),
      'claim worksheet for dispatch',
    )
  },

  markTerminal(
    db: Db,
    id: string,
    status: 'dispatched' | 'rejected',
  ): ResultAsync<void, string> {
    return q(
      () =>
        db
          .update(worksheets)
          .set({ status, decidedAt: nowStr() })
          .where(eq(worksheets.id, id)),
      'mark worksheet terminal',
    ).map(() => undefined)
  },

  /** Roll back a failed dispatch so the user can approve again. */
  rollbackToPending(db: Db, id: string): ResultAsync<void, string> {
    return q(
      () =>
        db
          .update(worksheets)
          .set({ status: 'pending', decidedAt: null })
          .where(and(eq(worksheets.id, id), eq(worksheets.status, 'dispatching'))),
      'rollback worksheet to pending',
    ).map(() => undefined)
  },
}
