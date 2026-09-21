import { DatabaseSync } from 'node:sqlite'
import { drizzle as drizzleSqlite } from 'drizzle-orm/node-sqlite'
import { drizzle } from 'drizzle-orm/postgres-js'
import { ResultAsync } from 'neverthrow'
import postgres, { type Sql } from 'postgres'
import { z } from 'zod'
import type { DbBackend } from './config.js'
import { DEFAULT_PRESET } from './config.js'

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
    tenant TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    variant TEXT NOT NULL DEFAULT '',
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
    last_used_at TEXT,
    locale TEXT NOT NULL DEFAULT '',
    "group" TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, name)
);
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS locale TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS variant TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    role TEXT NOT NULL,
    prev_id TEXT,
    created_at TEXT NOT NULL DEFAULT (NOW()::text)
);
CREATE INDEX IF NOT EXISTS idx_messages_prev ON messages (prev_id);

CREATE TABLE IF NOT EXISTS parts (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_parts_message ON parts (message_id, seq);

CREATE TABLE IF NOT EXISTS mailbox (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    session_name TEXT NOT NULL,
    msg_type TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL DEFAULT '{}',
    effective_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (NOW()::text),
    consumed_at TEXT,
    seq INTEGER
);
CREATE TABLE IF NOT EXISTS providers (
    tenant TEXT NOT NULL DEFAULT 'default',
    provider_id TEXT NOT NULL,
    capability TEXT NOT NULL DEFAULT 'text',
    api_type TEXT NOT NULL DEFAULT 'openai-compatible',
    base_url TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    headers TEXT NOT NULL DEFAULT 'null',
    models TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, provider_id)
);
ALTER TABLE providers ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
ALTER TABLE providers ADD COLUMN IF NOT EXISTS capability TEXT NOT NULL DEFAULT 'text';

-- Multi-tenant identity: tenants + their bearer tokens. Done in a single
-- statement list so sqlite/pg share the shape.
CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    disabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS tenant_tokens (
    token_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT '',
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_tenant_tokens_tenant ON tenant_tokens (tenant_id);

-- presets / config / files-meta moved to NATS KV buckets (abc-presets,
-- abcp-agent-config, abc-files-meta). The legacy PG tables are intentionally
-- NOT dropped: existing deployments keep them as the one-time backfill
-- source (see kv-backfill.ts); fresh installs never create them.

-- File metadata + sha dedup live HERE (not NATS KV) so durable file state can
-- leave NATS for an external object store. Tenant-SCOPED: the same bytes under
-- two tenants resolve to two independent codes (dedup is per (tenant, sha256)),
-- so a cross-tenant upload can never resolve to another tenant's bytes.
CREATE TABLE IF NOT EXISTS agent_files (
    code TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    sha256 TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    mime TEXT NOT NULL DEFAULT '',
    size BIGINT NOT NULL DEFAULT 0,
    uploader_session TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (NOW()::text),
    width INTEGER,
    height INTEGER,
    duration_ms BIGINT,
    thumb_code TEXT,
    thumbhash TEXT
);
CREATE INDEX IF NOT EXISTS idx_agent_files_sha ON agent_files (tenant, sha256);
`

// SQLite has no `NOW()::text`, supports table creation with the full column
// set (so the additive ALTERs below are only needed when upgrading an
// already-created file), and rejects some unused PG column operators.
const SQLITE_DDL = `
CREATE TABLE IF NOT EXISTS sessions (
    tenant TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    variant TEXT NOT NULL DEFAULT '',
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
    locale TEXT NOT NULL DEFAULT '',
    "group" TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, name)
);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    role TEXT NOT NULL,
    prev_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_messages_prev ON messages (prev_id);

CREATE TABLE IF NOT EXISTS parts (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_parts_message ON parts (message_id, seq);

CREATE TABLE IF NOT EXISTS mailbox (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    session_name TEXT NOT NULL,
    msg_type TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL DEFAULT '{}',
    effective_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    consumed_at TEXT,
    seq INTEGER
);
-- Provider registry. THREAT MODEL for api_key: keys are stored in PLAINTEXT
-- (both sqlite and PG). Acceptable because: the table is only readable via the
-- DB handle inside the agent process; the RPC surface never returns a raw key
-- (providers.ts masks it via maskSecret, and a client saving the MASKED value
-- round-trips the stored key untouched); tenant tokens (the DB's other secret)
-- are hashed sha256, so a DB dump yields provider keys but NOT credentials.
-- NOT acceptable without changes when: the DB is hosted off-box, shared, or
-- backed up unencrypted. Then encrypt at rest (e.g. XChaCha20 with a KMS-held
-- key) BEFORE this column leaves the node. See README "Secret handling".
CREATE TABLE IF NOT EXISTS providers (
    tenant TEXT NOT NULL DEFAULT 'default',
    provider_id TEXT NOT NULL,
    capability TEXT NOT NULL DEFAULT 'text',
    api_type TEXT NOT NULL DEFAULT 'openai-compatible',
    base_url TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    headers TEXT NOT NULL DEFAULT 'null',
    models TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, provider_id)
);

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    disabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS tenant_tokens (
    token_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    token_sha256 TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT '',
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_tenant_tokens_tenant ON tenant_tokens (tenant_id);

-- File metadata + sha dedup (see DDL above): tenant-scoped content addressing.
CREATE TABLE IF NOT EXISTS agent_files (
    code TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    sha256 TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    mime TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL DEFAULT 0,
    uploader_session TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    width INTEGER,
    height INTEGER,
    duration_ms INTEGER,
    thumb_code TEXT,
    thumbhash TEXT,
    UNIQUE (tenant, sha256)
);
CREATE INDEX IF NOT EXISTS idx_agent_files_sha ON agent_files (tenant, sha256);
`

// PG-specific additive migrations (safe to re-run; destructive-free). Kept as
// a separate statement list so sqlite can run the guarded equivalents.
const PG_MIGRATIONS = `
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS max_turns INTEGER NOT NULL DEFAULT 0;
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS system_prompt TEXT NOT NULL DEFAULT '';
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS variant TEXT NOT NULL DEFAULT '';
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_input_tokens BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS last_output_tokens BIGINT NOT NULL DEFAULT 0;
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE sessions ADD COLUMN IF NOT EXISTS "group" TEXT NOT NULL DEFAULT '';
    ALTER TABLE messages ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE parts ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE mailbox ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE mailbox ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT '';
    ALTER TABLE providers ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
    ALTER TABLE providers ADD COLUMN IF NOT EXISTS capability TEXT NOT NULL DEFAULT 'text';
    DROP TABLE IF EXISTS worksheets;
    ALTER TABLE agent_files ADD COLUMN IF NOT EXISTS width INTEGER;
    ALTER TABLE agent_files ADD COLUMN IF NOT EXISTS height INTEGER;
    ALTER TABLE agent_files ADD COLUMN IF NOT EXISTS duration_ms BIGINT;
    ALTER TABLE agent_files ADD COLUMN IF NOT EXISTS thumb_code TEXT;
    ALTER TABLE agent_files ADD COLUMN IF NOT EXISTS thumbhash TEXT;
    ALTER TABLE messages DROP COLUMN IF EXISTS tool_name;
    ALTER TABLE messages DROP COLUMN IF EXISTS tool_call_id;
    ALTER TABLE sessions DROP COLUMN IF EXISTS last_read_at;
    UPDATE sessions SET preset = '${DEFAULT_PRESET}' WHERE preset = '';
    UPDATE sessions SET preset = '${DEFAULT_PRESET}' WHERE preset = 'orchestrator';
    UPDATE sessions SET preset = '${DEFAULT_PRESET}' WHERE preset = 'executor';
    UPDATE sessions SET preset = '${DEFAULT_PRESET}' WHERE preset = 'analyst';
    ALTER TABLE sessions ALTER COLUMN preset SET DEFAULT '${DEFAULT_PRESET}';
    CREATE INDEX IF NOT EXISTS idx_messages_tenant ON messages (tenant, id);
    CREATE INDEX IF NOT EXISTS idx_parts_tenant ON parts (tenant, message_id);
    DROP INDEX IF EXISTS idx_mb_sess;
    CREATE INDEX IF NOT EXISTS idx_mb_sess ON mailbox (tenant, session_name);
`

// Composite-primary-key swap for a pre-v2 (tenant-less) PG database. Guarded
// on the tenant column so it only runs during the v1 -> v2 upgrade.
const PG_TENANT_PK_MIGRATIONS = `
    ALTER TABLE sessions DROP CONSTRAINT IF EXISTS sessions_pkey;
    ALTER TABLE sessions ADD PRIMARY KEY (tenant, name);
    ALTER TABLE providers DROP CONSTRAINT IF EXISTS providers_pkey;
    ALTER TABLE providers ADD PRIMARY KEY (tenant, provider_id);
    ALTER TABLE mailbox DROP CONSTRAINT IF EXISTS mailbox_session_name_fkey;
`

// agent_files: global (sha256 UNIQUE) -> tenant-scoped (tenant, sha256). Adds
// the tenant column (backfilling 'default'), drops the old global unique
// constraint, and installs the composite one. Guarded so it is a no-op once
// applied (the DROP CONSTRAINT IF EXISTS / ADD COLUMN IF NOT EXISTS are
// idempotent; the composite UNIQUE is created by the v2 DDL for fresh installs
// and by this statement for upgrades — the DROP/ADD pair below tolerates both).
const PG_AGENT_FILES_TENANT_MIGRATION = `
    ALTER TABLE agent_files ADD COLUMN IF NOT EXISTS tenant TEXT NOT NULL DEFAULT 'default';
    UPDATE agent_files SET tenant = 'default' WHERE tenant = '';
    ALTER TABLE agent_files DROP CONSTRAINT IF EXISTS agent_files_sha256_key;
    DROP INDEX IF EXISTS idx_agent_files_sha;
    CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_files_tenant_sha ON agent_files (tenant, sha256);
    CREATE INDEX IF NOT EXISTS idx_agent_files_sha ON agent_files (tenant, sha256);
`

// SQLite: feature detection for columns we never create (they are all in the
// CREATE); the additive ALTERs are only needed for pre-existing files. Each is
// best-effort — sqlite throws on duplicate columns, so we swallow errcode 1.
const SQLITE_MIGRATIONS = [
  `ALTER TABLE sessions ADD COLUMN variant TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE sessions ADD COLUMN max_turns INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE sessions ADD COLUMN system_prompt TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE sessions ADD COLUMN last_input_tokens INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE sessions ADD COLUMN last_output_tokens INTEGER NOT NULL DEFAULT 0`,
  `ALTER TABLE sessions ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'`,
  `ALTER TABLE sessions ADD COLUMN "group" TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE messages ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'`,
  `ALTER TABLE parts ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'`,
  `ALTER TABLE mailbox ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'`,
  `ALTER TABLE mailbox ADD COLUMN source TEXT NOT NULL DEFAULT ''`,
  `ALTER TABLE providers ADD COLUMN tenant TEXT NOT NULL DEFAULT 'default'`,
  `ALTER TABLE providers ADD COLUMN capability TEXT NOT NULL DEFAULT 'text'`,
  `ALTER TABLE agent_files ADD COLUMN width INTEGER`,
  `ALTER TABLE agent_files ADD COLUMN height INTEGER`,
  `ALTER TABLE agent_files ADD COLUMN duration_ms INTEGER`,
  `ALTER TABLE agent_files ADD COLUMN thumb_code TEXT`,
  `ALTER TABLE agent_files ADD COLUMN thumbhash TEXT`,
  `ALTER TABLE messages DROP COLUMN tool_name`,
  `ALTER TABLE messages DROP COLUMN tool_call_id`,
  `ALTER TABLE sessions DROP COLUMN last_read_at`,
  `UPDATE sessions SET preset = '${DEFAULT_PRESET}' WHERE preset IN ('', 'orchestrator', 'executor', 'analyst')`,
  `CREATE INDEX IF NOT EXISTS idx_messages_tenant ON messages (tenant, id)`,
  `CREATE INDEX IF NOT EXISTS idx_parts_tenant ON parts (tenant, message_id)`,
  `CREATE INDEX IF NOT EXISTS idx_mb_sess ON mailbox (tenant, session_name)`,
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
    ;(client as DatabaseSync).prepare(sql).run(...(params as never[]))
    return
  }
  await (client as Sql).unsafe(sql, params as never[])
}

/**
 * Connect (idempotent DDL). The providers table is the single source of truth
 * and is NEVER wiped at boot: registered providers (and their models) must
 * survive restarts, rollouts and redeploys. Schema evolution is expressed as
 * additive migrations (see [migrateSchema]), not a drop-and-recreate reset.
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
        return db
      }
      const sql = postgres(url, { max: 10 })
      await migrateSchema(sql, DDL)
      const db = drizzle({ client: sql }) as unknown as Db
      ;(db as { __backend?: DbBackend }).__backend = 'pg'
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
 *
 * v1 -> v2 multi-tenant upgrade: `sessions` and `providers` need a COMPOSITE
 * primary key (tenant, …). SQLite cannot alter a PK in place, so those two
 * tables are rebuilt (copying rows, backfilling tenant='default') when the
 * existing table's PK lacks `tenant`. `messages`/`parts`/`mailbox` keep their
 * PKs and only gain a `tenant` column.
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
    // Drop the dead `worksheets` table FIRST: its now-invalid FK
    // (`session_name REFERENCES sessions(name)` vs the composite sessions PK)
    // makes SQLite reject sessions rebuilds, forks, deletes and PK-renames at
    // prepare time. See [dropLegacyWorksheets].
    dropLegacyWorksheets(driver)
    rebuildIfPkLacksTenant(driver, 'sessions', [
      'tenant',
      'name',
      'model',
      'variant',
      'preset',
      'tip_id',
      'max_turns',
      'system_prompt',
      'input_tokens',
      'output_tokens',
      'total_tokens',
      'last_input_tokens',
      'last_output_tokens',
      'created_at',
      'updated_at',
      'last_used_at',
      'locale',
      'group',
    ])
    rebuildIfPkLacksTenant(driver, 'providers', [
      'tenant',
      'provider_id',
      'api_type',
      'base_url',
      'api_key',
      'headers',
      'models',
      'created_at',
      'updated_at',
    ])
    // v1 mailbox carried `session_name ... REFERENCES sessions(name)`. After the
    // sessions PK becomes (tenant, name) that parent key is no longer unique, so
    // any FK action on mailbox fails with "foreign key mismatch". SQLite cannot
    // DROP a constraint, so rebuild the table with the FK-free v2 DDL.
    rebuildMailboxIfLegacyFk(driver)
    // agent_files moved from a global (sha256 UNIQUE) to a tenant-scoped
    // (tenant, sha256) uniqueness. SQLite cannot change a constraint in place,
    // so rebuild when the tenant column is absent; pre-existing rows are
    // backfilled with tenant='default'.
    rebuildAgentFilesIfLegacy(driver)
  } else {
    await (driver as Sql).unsafe(ddl)
    await (driver as Sql).unsafe(PG_MIGRATIONS)
    await (driver as Sql).unsafe(PG_TENANT_PK_MIGRATIONS)
    await (driver as Sql).unsafe(PG_AGENT_FILES_TENANT_MIGRATION)
  }
}

/** Primary-key columns of a sqlite table (in ordinal order). */
function sqlitePkColumns(raw: DatabaseSync, table: string): string[] {
  const rows = raw.prepare(`PRAGMA table_info(${table})`).all() as Array<{
    name: string
    pk: number
  }>
  return rows
    .filter(r => Number(r.pk) > 0)
    .sort((a, b) => Number(a.pk) - Number(b.pk))
    .map(r => String(r.name))
}

/**
 * Rebuild a sqlite table whose PRIMARY KEY must gain `tenant`. No-op when the
 * PK already includes tenant. Copies every column by name (tenant defaults to
 * 'default' for pre-v2 rows).
 */
function rebuildIfPkLacksTenant(
  raw: DatabaseSync,
  table: string,
  columns: string[],
): void {
  const pk = sqlitePkColumns(raw, table)
  if (pk.length === 0 || pk.includes('tenant')) return
  const cols = columns.join(', ')
  // legacy_alter_table keeps other tables' FK references pointing at the
  // ORIGINAL name across the RENAME, so (e.g.) mailbox's FK to sessions is not
  // silently rewritten to the temp table we immediately drop.
  raw.exec('PRAGMA foreign_keys = OFF')
  raw.exec('PRAGMA legacy_alter_table = ON')
  try {
    raw.exec(`ALTER TABLE ${table} RENAME TO _${table}_v1`)
    // Recreate the v2 table by re-running the v2 CREATE (the name is now free).
    raw.exec(SQLITE_TABLE_DDL[table] ?? '')
    raw.exec(`INSERT INTO ${table} (${cols}) SELECT ${cols} FROM _${table}_v1`)
    raw.exec(`DROP TABLE _${table}_v1`)
  } finally {
    raw.exec('PRAGMA legacy_alter_table = OFF')
    raw.exec('PRAGMA foreign_keys = ON')
  }
}

/**
 * Rebuild the sqlite `mailbox` table when it still carries the v1 foreign key
 * `session_name REFERENCES sessions(name)`. Once `sessions` gains the (tenant,
 * name) PK that parent key is no longer unique, and every mailbox INSERT/UPDATE
 * fails with "foreign key mismatch". The v2 DDL drops the FK entirely (mailbox
 * rows are scoped by tenant + session_name, no referential enforcement), so a
 * rename+recreate+copy+drop clears it. No-op on fresh (FK-free) databases.
 */
function rebuildMailboxIfLegacyFk(raw: DatabaseSync): void {
  const fks = raw.prepare('PRAGMA foreign_key_list(mailbox)').all() as Array<{
    table: string
  }>
  if (!fks.some(fk => String(fk.table) === 'sessions')) return
  const cols =
    'id, tenant, session_name, msg_type, payload, effective_at, status, created_at, consumed_at, seq'
  raw.exec('PRAGMA foreign_keys = OFF')
  raw.exec('PRAGMA legacy_alter_table = ON')
  try {
    raw.exec('ALTER TABLE mailbox RENAME TO _mailbox_v1')
    raw.exec(SQLITE_TABLE_DDL['mailbox'] ?? '')
    raw.exec(`INSERT INTO mailbox (${cols}) SELECT ${cols} FROM _mailbox_v1`)
    raw.exec('DROP TABLE _mailbox_v1')
  } finally {
    raw.exec('PRAGMA legacy_alter_table = OFF')
    raw.exec('PRAGMA foreign_keys = ON')
  }
}

/**
 * Rebuild the sqlite `agent_files` table when it still lacks the `tenant`
 * column (pre-multi-tenant S3 deployment). The old table had a global
 * `sha256 UNIQUE`; the v2 table is tenant-scoped with `UNIQUE (tenant, sha256)`.
 * SQLite cannot alter a constraint in place, so rename+recreate+copy+drop;
 * existing rows are backfilled with tenant='default' (they predate tenancy).
 * No-op on fresh (already tenant-scoped) databases.
 */
function rebuildAgentFilesIfLegacy(raw: DatabaseSync): void {
  const cols = raw.prepare('PRAGMA table_info(agent_files)').all() as Array<{
    name: string
  }>
  if (cols.length === 0) return // table absent (nats mode / fresh)
  if (cols.some(c => String(c.name) === 'tenant')) return
  raw.exec('PRAGMA foreign_keys = OFF')
  raw.exec('PRAGMA legacy_alter_table = ON')
  try {
    raw.exec('ALTER TABLE agent_files RENAME TO _agent_files_v1')
    raw.exec(SQLITE_TABLE_DDL['agent_files'] ?? '')
    raw.exec(`INSERT INTO agent_files
      (code, tenant, sha256, name, mime, size, uploader_session, created_at)
      SELECT code, 'default', sha256, name, mime, size, uploader_session, created_at
      FROM _agent_files_v1`)
    raw.exec('DROP TABLE _agent_files_v1')
  } finally {
    raw.exec('PRAGMA legacy_alter_table = OFF')
    raw.exec('PRAGMA foreign_keys = ON')
  }
}

/**
 * Drop the legacy `worksheets` table from a pre-removal database.
 *
 * The worksheet feature was deleted from the schema (it moved to easylab), but
 * the table survives on databases created before that. Its
 * `session_name REFERENCES sessions(name)` FK now points at a NON-unique parent
 * key (the sessions PK is `(tenant, name)`), which makes SQLite reject — at
 * PREPARE time — every statement that touches that FK: session fork
 * (`INSERT … SELECT`), `DELETE FROM sessions`, and primary-key renames all fail
 * with `foreign key mismatch - "worksheets" referencing "sessions"`. The table
 * is dead (no code reads it), so dropping it is the fix. No-op on fresh DBs.
 */
function dropLegacyWorksheets(raw: DatabaseSync): void {
  const cols = raw.prepare('PRAGMA table_info(worksheets)').all() as Array<{
    name: string
  }>
  if (cols.length === 0) return // table absent (fresh / already migrated)
  raw.exec('PRAGMA foreign_keys = OFF')
  try {
    raw.exec('DROP TABLE worksheets')
  } finally {
    raw.exec('PRAGMA foreign_keys = ON')
  }
}

/** The v2 CREATE TABLE statement for a table that may require a PK rebuild. */
const SQLITE_TABLE_DDL: Record<string, string> = {
  sessions: `CREATE TABLE IF NOT EXISTS sessions (
    tenant TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    variant TEXT NOT NULL DEFAULT '',
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
    locale TEXT NOT NULL DEFAULT '',
    "group" TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, name)
  )`,
  providers: `CREATE TABLE IF NOT EXISTS providers (
    tenant TEXT NOT NULL DEFAULT 'default',
    provider_id TEXT NOT NULL,
    capability TEXT NOT NULL DEFAULT 'text',
    api_type TEXT NOT NULL DEFAULT 'openai-compatible',
    base_url TEXT NOT NULL DEFAULT '',
    api_key TEXT NOT NULL DEFAULT '',
    headers TEXT NOT NULL DEFAULT 'null',
    models TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, provider_id)
  )`,
  mailbox: `CREATE TABLE IF NOT EXISTS mailbox (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    session_name TEXT NOT NULL,
    msg_type TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL DEFAULT '{}',
    effective_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    consumed_at TEXT,
    seq INTEGER
  )`,
  agent_files: `CREATE TABLE IF NOT EXISTS agent_files (
    code TEXT PRIMARY KEY,
    tenant TEXT NOT NULL DEFAULT 'default',
    sha256 TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    mime TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL DEFAULT 0,
    uploader_session TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    width INTEGER,
    height INTEGER,
    duration_ms INTEGER,
    thumb_code TEXT,
    thumbhash TEXT,
    UNIQUE (tenant, sha256)
  )`,
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
