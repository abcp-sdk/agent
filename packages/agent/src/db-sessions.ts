import type { SessionRow } from '@zergx-agent/schema'
import { sql as dsql, eq } from 'drizzle-orm'
import type { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { dbBackend, nowStr, q, rawAll } from './db-client.js'
import { mailbox, sessions } from './db-schema.js'

/**
 * Default preset applied to every session when none is specified. `plan`
 * is the read-only role (repo inspect + session analysis, no side effects);
 * the sandbox-only `explore` and full-capability `build` presets are opt-in.
 */
export const DEFAULT_PRESET = 'plan'

type Row = typeof sessions.$inferSelect

const toRow = (r: Row): SessionRow => ({
  name: r.name,
  model: r.model,
  preset: r.preset,
  tip_id: r.tipId,
  max_turns: r.maxTurns,
  system_prompt: r.systemPrompt,
  input_tokens: Number(r.inputTokens),
  output_tokens: Number(r.outputTokens),
  total_tokens: Number(r.totalTokens),
  last_input_tokens: Number(r.lastInputTokens),
  last_output_tokens: Number(r.lastOutputTokens),
  created_at: r.createdAt,
  updated_at: r.updatedAt,
  last_used_at: r.lastUsedAt,
  locale: r.locale,
})

export interface SessionPatch {
  // `| undefined` on every optional: callers pass zod-inferred bodies whose
  // absent fields arrive as explicit `undefined` (exactOptionalPropertyTypes).
  model?: string | undefined
  preset?: string | undefined
  maxTurns?: number | undefined
  systemPrompt?: string | undefined
  locale?: string | undefined
}

export const Sessions = {
  list(db: Db): ResultAsync<SessionRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(sessions)
          .orderBy(dsql`${sessions.updatedAt} DESC`)
          .then(rows => rows.map(toRow)),
      'list sessions',
    )
  },

  get(db: Db, name: string): ResultAsync<SessionRow | null, string> {
    return q(
      () =>
        db
          .select()
          .from(sessions)
          .where(eq(sessions.name, name))
          .limit(1)
          .then(rows => {
            const r = rows[0]
            return r === undefined ? null : toRow(r)
          }),
      'get session',
    )
  },

  create(
    db: Db,
    input: Pick<SessionRow, 'name'> & {
      // `| undefined`: explicit-undefined keys from zod-inferred bodies are
      // accepted and fall through to the defaults below.
      model?: string | undefined
      preset?: string | undefined
      tipId?: string | null | undefined
      systemPrompt?: string | undefined
      maxTurns?: number | undefined
      locale?: string | undefined
    },
  ): ResultAsync<string, string> {
    return q(
      () =>
        db.insert(sessions).values({
          name: input.name,
          model: input.model ?? '',
          preset: input.preset ?? DEFAULT_PRESET,
          tipId: input.tipId ?? null,
          systemPrompt: input.systemPrompt ?? '',
          maxTurns: input.maxTurns ?? 0,
          locale: input.locale ?? '',
          createdAt: nowStr(),
          updatedAt: nowStr(),
        }),
      'create session',
    ).map(() => input.name)
  },

  delete(db: Db, name: string): ResultAsync<void, string> {
    return q(async () => {
      // COW-safe delete. Messages are append-only and session-agnostic; a
      // session only points at its chain head via `tip_id`. Delete removes the
      // private mailbox queue and the session row, never message history.
      await db.delete(mailbox).where(eq(mailbox.sessionName, name))
      await db.delete(sessions).where(eq(sessions.name, name))
    }, 'delete session')
  },

  setModel(db: Db, name: string, model: string): ResultAsync<void, string> {
    return q(
      () =>
        db
          .update(sessions)
          .set({ model, updatedAt: nowStr() })
          .where(eq(sessions.name, name)),
      'set session model',
    ).map(() => undefined)
  },

  updateSettings(
    db: Db,
    name: string,
    patch: SessionPatch,
  ): ResultAsync<void, string> {
    return q(
      () =>
        db
          .update(sessions)
          .set(patchToDrizzle(patch, nowStr()))
          .where(eq(sessions.name, name)),
      'update session settings',
    ).map(() => undefined)
  },

  tip(db: Db, name: string): ResultAsync<string | null, string> {
    return q(
      () =>
        db
          .select({ tipId: sessions.tipId })
          .from(sessions)
          .where(eq(sessions.name, name))
          .limit(1)
          .then(rows => rows[0]?.tipId ?? null),
      'get session tip',
    )
  },

  setTip(
    db: Db,
    name: string,
    tipId: string | null,
  ): ResultAsync<void, string> {
    return q(
      () =>
        db
          .update(sessions)
          .set({ tipId, updatedAt: nowStr() })
          .where(eq(sessions.name, name)),
      'set session tip',
    ).map(() => undefined)
  },

  addUsage(
    db: Db,
    name: string,
    input: number,
    output: number,
  ): ResultAsync<void, string> {
    const sql =
      dbBackend(db) === 'pg'
        ? `UPDATE sessions SET
           input_tokens = input_tokens + $1,
           output_tokens = output_tokens + $2,
           total_tokens = total_tokens + $3,
           last_input_tokens = $1,
           last_output_tokens = $2,
           last_used_at = $4
         WHERE name = $5`
        : `UPDATE sessions SET
           input_tokens = input_tokens + ?,
           output_tokens = output_tokens + ?,
           total_tokens = total_tokens + ?,
           last_input_tokens = ?,
           last_output_tokens = ?,
           last_used_at = ?
         WHERE name = ?`
    const pgParams: unknown[] = [input, output, input + output, nowStr(), name]
    const sqliteParams: unknown[] = [
      input,
      output,
      input + output,
      input,
      output,
      nowStr(),
      name,
    ]
    return q(
      () => rawAll(db, sql, dbBackend(db) === 'pg' ? pgParams : sqliteParams),
      'add session usage',
    ).map(() => undefined)
  },

  exists(db: Db, name: string): ResultAsync<boolean, string> {
    return q(
      () =>
        db
          .select({ name: sessions.name })
          .from(sessions)
          .where(eq(sessions.name, name))
          .limit(1)
          .then(rows => rows.length > 0),
      'session exists',
    )
  },
}

function patchToDrizzle(
  patch: SessionPatch,
  updatedAt: string,
): Partial<typeof sessions.$inferInsert> {
  const set: Partial<typeof sessions.$inferInsert> = { updatedAt }
  if (patch.model !== undefined) set.model = patch.model
  if (patch.preset !== undefined) set.preset = patch.preset
  if (patch.maxTurns !== undefined) set.maxTurns = patch.maxTurns
  if (patch.systemPrompt !== undefined) set.systemPrompt = patch.systemPrompt
  if (patch.locale !== undefined) set.locale = patch.locale
  return set
}
