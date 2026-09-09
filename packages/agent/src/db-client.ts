import { DatabaseSync } from 'node:sqlite'
import { drizzle as drizzleSqlite } from 'drizzle-orm/node-sqlite'
import { drizzle, type PostgresJsDatabase } from 'drizzle-orm/postgres-js'
import { ResultAsync } from 'neverthrow'
import postgres, { type Sql } from 'postgres'
import { z } from 'zod'
import type { DbBackend } from './config.js'
import { parse } from './json.js'
import { logger } from './logger.js'

/**
 * The drizzle database handle. The query surface we use — insert/select/update/
 * delete, where/orderBy/limit, returning(), onConflictDoNothing/onConflictDoUpdate
 * — is implementable by BOTH drizzle drivers. We type `Db` as the node:sqlite
 * drizzle handle (it is the image default and exposes every builder we call),
 * and construct the equivalent postgres-js handle at runtime for `pg` mode,
 * casting across the shared shape. `$client` is the underlying raw driver.
 */
export type Db = ReturnType<typeof drizzleSqlite> & {
  $client: Sql | DatabaseSync
}

/** Raw query rows are returned as plain record arrays (drizzle and raw alike). */
export type DbRow = Record<string, unknown>

/** Shape of a provider row imported from the legacy config (Zod-inferred). */
export interface ProviderImport {
  provider_id: string
  base_url: string
  api_type?: string | undefined
  api_key?: string | undefined
  headers?: unknown
  models?: unknown
}

/** Resolve which backend is driving a Db handle (carried on the handle). */
export function dbBackend(db: Db): DbBackend {
  return (db as { __backend?: DbBackend }).__backend ?? 'sqlite'
}

export type {
  ContentPayload,
  Json,
  TextPartData,
  ToolPartData,
  ToolResultPartData,
  WakePayload,
} from './json.js'
// Re-export the shared JSON/parse surface for one-stop imports elsewhere in
// the agent package (single source of truth in ./json.ts).
export {
  ContentPayloadSchema,
  parse,
  stringify,
  TextPartDataSchema,
  ToolPartDataSchema,
  ToolResultPartDataSchema,
  WakePayloadSchema,
} from './json.js'

const DDL = `
CREATE TABLE IF NOT EXISTS sessions (
    name TEXT PRIMARY KEY,
    model TEXT NOT NULL DEFAULT '',
    preset TEXT NOT NULL DEFAULT '',
    tip_id TEXT,
    max_turns INTEGER NOT NULL DEFAULT 0,
    system_prompt TEXT NOT NULL DEFAULT '',
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    total_tokens BIGINT NOT NULL DEFAULT 0,
    last_input_tokens BIGINT NOT NULL DEFAULT 0,
    last_output_tokens BIGINT NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (NOW()::text),
    updated_at TEXT NOT NULL DEFAULT (NOW()::text),
    last_used_at TEXT
);
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS locale TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    prev_id TEXT,
    created_at TEXT NOT NULL DEFAULT (NOW()::text)
);
CREATE INDEX IF NOT EXISTS idx_messages_prev ON messages (prev_id);

CREATE TABLE IF NOT EXISTS parts (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_parts_message ON parts (message_id, seq);

CREATE TABLE IF NOT EXISTS mailbox (
    id TEXT PRIMARY KEY,
    session_name TEXT NOT NULL REFERENCES sessions(name) ON DELETE CASCADE,
    msg_type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    effective_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (NOW()::text),
    consumed_at TEXT,
    seq INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mb_sess ON mailbox (session_name);

CREATE TABLE IF NOT EXISTS providers (
    provider_id TEXT PRIMARY KEY,
    api_type TEXT NOT NULL DEFAULT 'openai-compatible',
    base_url TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    headers TEXT NOT NULL DEFAULT 'null',
    models TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
);

-- presets / config / files-meta moved to NATS KV buckets (abc-presets,
-- abc-agent-config, abc-files-meta). The legacy PG tables are intentionally
-- NOT dropped: existing deployments keep them as the one-time backfill
-- source (see kv-backfill.ts); fresh installs never create them.
`

// SQLite has no `NOW()::text`, supports table creation with the full column
// set (so the additive ALTERs below are only needed when upgrading an
// already-created file), and rejects some unused PG column operators.
const SQLITE_DDL = `
CREATE TABLE IF NOT EXISTS sessions (
    name TEXT PRIMARY KEY,
    model TEXT NOT NULL DEFAULT '',
    preset TEXT NOT NULL DEFAULT '',
    tip_id TEXT,
    max_turns INTEGER NOT NULL DEFAULT 0,
    system_prompt TEXT NOT NULL DEFAULT '',
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    last_input_tokens INTEGER NOT NULL DEFAULT 0,
    last_output_tokens INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_used_at TEXT,
    locale TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    role TEXT NOT NULL,
    prev_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_messages_prev ON messages (prev_id);

CREATE TABLE IF NOT EXISTS parts (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_parts_message ON parts (message_id, seq);

CREATE TABLE IF NOT EXISTS mailbox (
    id TEXT PRIMARY KEY,
    session_name TEXT NOT NULL REFERENCES sessions(name) ON DELETE CASCADE,
    msg_type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    effective_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    consumed_at TEXT,
    seq INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mb_sess ON mailbox (session_name);

CREATE TABLE IF NOT EXISTS providers (
    provider_id TEXT PRIMARY KEY,
    api_type TEXT NOT NULL DEFAULT 'openai-compatible',
    base_url TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    headers TEXT NOT NULL DEFAULT 'null',
    models TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
);
`

// PG-specific additive migrations (safe to re-run; destructive-free). Kept as
// a separate statement list so sqlite can run the guarded equivalents.
const PG_MIGRATIONS = `
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS max_turns INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS system_prompt TEXT NOT NULL DEFAULT '';
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_input_tokens BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_output_tokens BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE messages DROP COLUMN IF EXISTS tool_name;
    ALTER TABLE messages DROP COLUMN IF EXISTS tool_call_id;
    ALTER TABLE sessions DROP COLUMN IF EXISTS last_read_at;
    UPDATE sessions SET preset = 'plan' WHERE preset = '';
    UPDATE sessions SET preset = 'plan' WHERE preset = 'orchestrator';
    UPDATE sessions SET preset = 'plan' WHERE preset = 'executor';
    UPDATE sessions SET preset = 'plan' WHERE preset = 'analyst';
    ALTER TABLE sessions ALTER COLUMN preset SET DEFAULT 'plan';
`

// SQLite: feature detection for columns we never create (they are all in the
// CREATE); the additive ALTERs are only needed for pre-existing files. Each is
// best-effort — sqlite throws on duplicate columns, so we swallow errcode 1.
const SQLITE_MIGRATIONS = [
  `ALTER TABLE sessions ADD COLUMN max_turns INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE sessions ADD COLUMN system_prompt TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE sessions ADD COLUMN last_input_tokens INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE sessions ADD COLUMN last_output_tokens INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE messages DROP COLUMN tool_name`,
  `ALTER TABLE messages DROP COLUMN tool_call_id`,
  `ALTER TABLE sessions DROP COLUMN last_read_at`,
  `UPDATE sessions SET preset = 'plan' WHERE preset IN ('', 'orchestrator', 'executor', 'analyst')`,
]

/**
 * Connection string → sqlite file path. Accepts `sqlite:///path`, `sqlite:path`,
 * `sqlite:/path`, or a bare absolute/relative path.
 */
function sqlitePath(url: string): string {
  let u = url
  if (u.startsWith('sqlite://')) u = u.slice('sqlite://'.length)
  else if (u.startsWith('sqlite:')) u = u.slice('sqlite:'.length)
  return u || ':memory:'
}

/** Reject URL-style sqlite that smuggles e.g. query params into a filename. */
function sanitizeSqlitePath(p: string): string {
  const q = p.indexOf('?')
  return q === -1 ? p : p.slice(0, q)
}

export function nowStr(): string {
  return new Date().toISOString()
}

export function uuid(): string {
  return crypto.randomUUID()
}

/** Normalize a raw drizzle/postgres.js execute() result into a row array. */
const RawRowsSchema = z.array(z.record(z.string(), z.unknown()))

const ResultWithRowsSchema = z
  .object({ rows: z.array(z.record(z.string(), z.unknown())) })
  .partial()

export function rowsOf(res: unknown): Record<string, unknown>[] {
  const direct = RawRowsSchema.safeParse(res)
  if (direct.success) return direct.data
  if (Array.isArray(res)) return res as Record<string, unknown>[]
  const wrapped = ResultWithRowsSchema.safeParse({ rows: res })
  if (wrapped.success && wrapped.data.rows !== undefined) {
    return wrapped.data.rows
  }
  return []
}

/**
 * Raw SQL with positional `?` params for sqlite and `$n` for pg. Each backend
 * writes its own statements; here we just dispatch to the underlying driver
 * and normalize the result to plain row records.
 */
export async function rawAll(
  db: Db,
  sql: string,
  params: unknown[] = [],
): Promise<DbRow[]> {
  const client = db.$client
  if (dbBackend(db) === 'sqlite') {
    const stmt = (client as DatabaseSync).prepare(sql)
    return stmt.all(...(params as never[])) as DbRow[]
  }
  const rows = await (client as Sql).unsafe(sql, params as never[])
  return rowsOf(rows)
}

/**
 * Raw SQL execution for writes (INSERT / UPDATE / DELETE / DDL) with the same
 * positional `?` (sqlite) / `$n` (pg) param convention as [rawAll].
 */
export async function rawRun(
  db: Db,
  sql: string,
  params: unknown[] = [],
): Promise<void> {
  const client = db.$client
  if (dbBackend(db) === 'sqlite') {
    (client as DatabaseSync).prepare(sql).run(...(params as never[]))
    return
  }
  await (client as Sql).unsafe(sql, params as never[])
}

/**
 * Connect (idempotent DDL) and import any legacy providers blob from the
 * config table into the providers table (single source of truth).
 */
export function connectDb(
  backend: DbBackend,
  url: string,
): ResultAsync<Db, string> {
  return ResultAsync.fromPromise(
    (async () => {
      const ddl = backend === 'sqlite' ? SQLITE_DDL : DDL
      if (backend === 'sqlite') {
        const raw = new DatabaseSync(sanitizeSqlitePath(sqlitePath(url)))
        raw.exec('PRAGMA journal_mode = WAL')
        raw.exec('PRAGMA foreign_keys = ON')
        // drizzleSqlite accepts a DatabaseSync via the object form; TS narrows
        // it to the string overload, so construct via the exported drizzle()
        // (same driver) and cast the result.
        const db = drizzleSqlite({ client: raw } as never) as unknown as Db
        ;(db as { __backend?: DbBackend }).__backend = 'sqlite'
        await migrateSchema(raw, ddl)
        await importProvidersBackend(db, raw)
        return db
      }
      const sql = postgres(url, { max: 10 })
      await migrateSchema(sql, DDL)
      const db = drizzle({ client: sql }) as unknown as Db
      ;(db as { __backend?: DbBackend }).__backend = 'pg'
      await importProvidersBackend(db, sql)
      return db
    })(),
    e => `db connect failed: ${String(e)}`,
  )
}

/**
 * Idempotent schema bootstrap. Tables are created with IF NOT EXISTS and are
 * never dropped: session history, mailbox, and message chains must survive
 * restarts, rollouts, and multi-replica boots (dropping them would destroy
 * every conversation on each deploy and break cross-replica durable mailbox
 * delivery). Column/table changes must be expressed as additive migrations,
 * not a drop-and-recreate reset.
 */
async function migrateSchema(
  driver: Sql | DatabaseSync,
  ddl: string,
): Promise<void> {
  if (driver instanceof DatabaseSync) {
    driver.exec(ddl)
    for (const stmt of SQLITE_MIGRATIONS) {
      try {
        driver.exec(stmt)
      } catch {
        // duplicate column on an already-migrated file: best-effort
      }
    }
  } else {
    await (driver as Sql).unsafe(ddl)
    await (driver as Sql).unsafe(PG_MIGRATIONS)
  }
}

async function importProvidersBackend(
  db: Db,
  driver: Sql | DatabaseSync,
): Promise<void> {
  try {
    let rows: { value?: string }[]
    if (driver instanceof DatabaseSync) {
      const stmt = (driver as DatabaseSync).prepare(
        `SELECT value FROM config WHERE key = 'providers'`,
      )
      rows = stmt.all() as unknown as { value?: string }[]
    } else {
      const res =
        await (driver as Sql)`SELECT value FROM config WHERE key = 'providers'`
      rows = res as { value?: string }[]
    }
    const raw = rows[0]?.value
    const rawParsed = z.string().safeParse(raw)
    if (
      !rawParsed.success ||
      rawParsed.data === '' ||
      rawParsed.data === '{}'
    ) {
      return
    }
    const parsed = parse(z.unknown(), rawParsed.data)
    if (parsed.isErr()) return

    const ProviderImportSchema = z.object({
      provider_id: z.string(),
      base_url: z.string(),
      api_type: z.string().optional(),
      api_key: z.string().optional(),
      headers: z.unknown(),
      models: z.unknown(),
    })
    const ProvidersMapSchema = z.record(z.string(), ProviderImportSchema)
    const providers = ProvidersMapSchema.safeParse(parsed.value)
    if (!providers.success) return

    let imported = 0
    const providerEntries = z
      .array(z.tuple([z.string(), ProviderImportSchema]))
      .safeParse(Object.entries(providers.data))
    if (!providerEntries.success) return
    for (const [, o] of providerEntries.data) {
      await rawInsertProvider(db, o)
      imported++
    }
    try {
      await rawSetConfigProviders(db)
    } catch {
      // legacy table absent on fresh installs
    }
    if (imported > 0) {
      logger.info({ imported }, 'imported providers from config table')
    }
  } catch {
    return
  }
}

async function rawInsertProvider(db: Db, o: ProviderImport): Promise<void> {
  const values: unknown[] = [
    o.provider_id,
    o.api_type ?? 'openai-compatible',
    o.base_url,
    o.api_key ?? '',
    JSON.stringify(o.headers ?? null),
    JSON.stringify(o.models ?? []),
    nowStr(),
    nowStr(),
  ]
  if (dbBackend(db) === 'sqlite') {
    ;(db.$client as DatabaseSync)
      .prepare(
        `INSERT INTO providers (provider_id, api_type, base_url, api_key, headers, models, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (provider_id) DO NOTHING`,
      )
      .run(...(values as never[]))
  } else {
    await (db.$client as Sql).unsafe(
      `INSERT INTO providers (provider_id, api_type, base_url, api_key, headers, models, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (provider_id) DO NOTHING`,
      values as never[],
    )
  }
}

async function rawSetConfigProviders(db: Db): Promise<void> {
  if (dbBackend(db) === 'sqlite') {
    ;(db.$client as DatabaseSync)
      .prepare(`UPDATE config SET value = '{}' WHERE key = 'providers'`)
      .run()
  } else {
    await (db.$client as Sql)`UPDATE config SET value = '{}' WHERE key = 'providers'`
  }
}

/** Wrap a throwing async query into a ResultAsync with context.
 * The error chain (Drizzle wraps the pg cause) is flattened into the
 * message so downstream classifiers (e.g. foreign-key detection) can see
 * the underlying pg error code/detail. */
export function q<T>(
  op: () => Promise<T>,
  context: string,
): ResultAsync<T, string> {
  return ResultAsync.fromPromise(op(), e => {
    const causes: string[] = []
    let cur: unknown = e
    for (let depth = 0; depth < 4 && cur instanceof Error; depth++) {
      causes.push(cur.message ?? String(cur))
      cur = (cur as Error & { cause?: unknown }).cause
    }
    return `${context}: ${causes.join(' | caused by: ')}`
  })
}
