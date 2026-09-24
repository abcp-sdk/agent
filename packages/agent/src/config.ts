import { logger } from './logger.js'

export interface ServerConfig {
  port: number
  /** HTTP server transport: "auto" (h1 + h2c) | "h1" | "h2c". */
  httpProtocol: 'auto' | 'h1' | 'h2c'
  /** Storage backend: "pg" or "sqlite". */
  backend: DbBackend
  /** The resolved connection string for the selected backend. */
  dbUrl: string
  natsUrl: string
  /** Tool-call timeout (ms). Fixed; not user-configurable. */
  toolTimeoutMs: number
  /** Max agent steps per turn when neither the session nor its preset sets one. */
  defaultMaxTurns: number
  /**
   * Retries for a provider failure while STARTING a model call (429/5xx/network,
   * before any output). The AI SDK honors `isRetryable` + `Retry-After`/
   * `retry-after-ms`. Set via env `LLM_MAX_RETRIES` (default 3).
   */
  llmMaxRetries: number
  /**
   * Retries for a provider error AFTER streaming has begun (a mid-stream
   * disconnect / 429 / 5xx). The AI SDK re-runs only the current step and
   * DISCARDS tool parts from the failed attempt, so tools never re-execute.
   * Set via env `LLM_STREAM_RETRIES` (default 3).
   */
  llmStreamRetries: number
  /**
   * CORS allow-origin for browser (Flutter Web) clients. `*` allows any
   * origin; set an explicit origin to lock it down.
   */
  corsOrigin: string
  /** Request authentication mode. `required` (default) verifies bearer tokens;
   *  `none` disables auth and pins every request to {@link defaultTenant}. */
  authMode: 'required' | 'none'
  /** Static admin bearer token (unused when authMode='none'). */
  adminToken: string
  /** Tenant a request is pinned to when authMode='none' (and the migration
   *  target for pre-v2 data). */
  defaultTenant: string
  /** Optional first-boot bootstrap: a tenant id to create (and mint a token
   *  for) when the tenant table is empty. Both must be set to take effect. */
  bootstrapTenant: string
  bootstrapToken: string
  /**
   * Tools the host hard-disables for EVERY session, regardless of the
   * session's preset whitelist (empty or not). Every entry MUST be the
   * extension-qualified name `<extId>.<name>` (e.g. `bundled.mail-send`), so a
   * name collision across extensions is addressed unambiguously. Bare entries
   * are dropped at parse time with a warning. This is the enforcement backstop
   * a host needs when it cannot rely on preset whitelists alone (a user may
   * create/blank a preset). Set via env `DISABLED_TOOLS` (comma-separated).
   */
  disabledTools: string[]
  /**
   * Blob backend for DURABLE file bytes (and, when `s3`, their metadata):
   *   - `nats` (default): JetStream object store + KV metadata.
   *   - `s3`: S3-compatible object store for the bytes, `agent_files` table
   *     for metadata. Transient objects (tool payloads, catalog caches) stay
   *     on NATS. No read fallback between the two.
   */
  blobBackend: 'nats' | 's3'
  /** S3 settings (used only when blobBackend='s3'). */
  s3: S3Settings
  /**
   * One-time, idempotent seed of EXTENSION config values (the `cfg` KV bucket,
   * key `t.<tenant>.<extId>.<name>`), applied at boot ONLY when a value is
   * absent. This is how a deployment pins a default for a required extension
   * knob (e.g. `workspace.worker-url`) without a UI round-trip. Set via env
   * `AGENT_EXT_CONFIG_SEED` as a JSON array of
   * `{ "tenant": "...", "extId": "...", "name": "...", "value": "..." }`.
   * A later user set (or an existing value) always wins — seeding never
   * overwrites.
   */
  extConfigSeed: Array<{
    tenant: string
    extId: string
    name: string
    value: string
  }>
}

export interface S3Settings {
  bucket: string
  region: string
  endpoint: string
  accessKeyId: string
  secretAccessKey: string
  forcePathStyle: boolean
  prefix: string
}

export type DbBackend = 'pg' | 'sqlite'

/**
 * The one built-in preset. There is no configurable default: the generic
 * agent always seeds `default` (a host may inject additional system presets
 * via SYSTEM_PRESETS_FILE, but never renames which one is the fallback).
 */
export const DEFAULT_PRESET = 'default'

/**
 * Tenant-scoped Config KV keys holding the DEFAULTS applied when a request
 * omits or blanks a session's model / preset. Both are per tenant; an unset
 * key falls back to the built-in `default` preset (and, for model, to no
 * model at all — a session may exist without one until a turn needs it).
 */
export const CONFIG_DEFAULT_MODEL = 'default_model'
export const CONFIG_DEFAULT_PRESET = 'default_preset'

/** Fallback max turns when neither the session nor its preset sets a value. */
const DEFAULT_MAX_TURNS = 25

/** Fixed tool-call timeout (10 minutes). */
const TOOL_TIMEOUT_MS = 600_000

/**
 * Default provider retry budgets.
 *
 * `LLM_MAX_RETRIES` covers request-start failures (the SDK's own retry loop,
 * which is what throws `AI_RetryError: Failed after N attempts`). 11 retries =
 * 12 total attempts, with the SDK's UNCAPPED exponential backoff
 * (2,4,8,…,2048s; the last single wait is ~34 min, ~68 min cumulative). That
 * long tail is deliberate: a persistently rate-limited upstream is better
 * waited out than failed, and the user can interrupt the turn to stop it.
 *
 * `LLM_STREAM_RETRIES` covers mid-stream provider error events; those use OUR
 * backoff (capped at 30s) and stay at 3.
 */
const DEFAULT_LLM_MAX_RETRIES = 11
const DEFAULT_LLM_STREAM_RETRIES = 3

/** Resolve the storage backend from DATABASE_URL scheme + explicit override. */
function resolveBackend(env: NodeJS.ProcessEnv): DbBackend {
  const explicit = env['DB_BACKEND']
  if (explicit) {
    const v = explicit.toLowerCase()
    if (v === 'pg' || v === 'postgres' || v === 'postgresql') return 'pg'
    if (v === 'sqlite' || v === 'sqlite3' || v === 'better-sqlite3')
      return 'sqlite'
  }
  const url = env['DATABASE_URL'] ?? env['DATABASE_SCHEME']
  if (url?.startsWith('sqlite')) return 'sqlite'
  return 'pg'
}

/** Validate HTTP_PROTOCOL → 'auto' | 'h1' | 'h2c' (default 'auto'). */
function normalizeHttpProtocol(v: string): ServerConfig['httpProtocol'] {
  const lower = v.toLowerCase()
  if (lower === 'h1' || lower === 'h2c' || lower === 'auto') return lower
  return 'auto'
}

/** Parse a non-negative integer retry count; a bad value falls back to [def]. */
function parseCount(raw: string, def: number): number {
  const n = Number.parseInt(raw, 10)
  if (!Number.isInteger(n) || n < 0) {
    logger.warn({ raw }, 'invalid retry count; using default')
    return def
  }
  return n
}

/**
 * Load the (small) set of deployment-level settings. Application behaviour
 * (max turns, temperature, tool timeout, locale, compaction budgets) is NOT
 * configurable via environment: it comes from sessions/presets/provider model
 * config, or fixed constants.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): ServerConfig {
  const or = (k: string, d: string) => {
    const v = env[k]
    return v !== undefined && v !== '' ? v : d
  }

  const pgUrl = `postgres://${or('POSTGRES_USER', 'root')}:${or('POSTGRES_PASSWORD', 'devpassword')}@${or('POSTGRES_HOST', 'postgres.abcp.svc.cluster.local')}:${or('POSTGRES_PORT', '5432')}/${or('POSTGRES_DB_AGENT', 'abcp_agent')}`

  const backend = resolveBackend(env)

  return {
    port: Number.parseInt(or('PORT', '8080'), 10),
    httpProtocol: normalizeHttpProtocol(or('HTTP_PROTOCOL', 'auto')),
    backend,
    dbUrl:
      backend === 'sqlite'
        ? or('DATABASE_URL', 'sqlite:///data/abcp-agent.db')
        : pgUrl,
    natsUrl: or('NATS_URL', 'nats://nats.abcp.svc.cluster.local:4222'),
    toolTimeoutMs: TOOL_TIMEOUT_MS,
    defaultMaxTurns: DEFAULT_MAX_TURNS,
    llmMaxRetries: parseCount(
      or('LLM_MAX_RETRIES', String(DEFAULT_LLM_MAX_RETRIES)),
      DEFAULT_LLM_MAX_RETRIES,
    ),
    llmStreamRetries: parseCount(
      or('LLM_STREAM_RETRIES', String(DEFAULT_LLM_STREAM_RETRIES)),
      DEFAULT_LLM_STREAM_RETRIES,
    ),
    corsOrigin: or('AGENT_CORS_ORIGIN', '*'),
    authMode:
      or('AGENT_AUTH_MODE', 'required') === 'none' ? 'none' : 'required',
    adminToken: or('AGENT_ADMIN_TOKEN', ''),
    defaultTenant: or('AGENT_DEFAULT_TENANT', 'default'),
    bootstrapTenant: or('AGENT_BOOTSTRAP_TENANT', ''),
    bootstrapToken: or('AGENT_BOOTSTRAP_TOKEN', ''),
    disabledTools: parseDisabledTools(or('DISABLED_TOOLS', '')),
    blobBackend:
      or('AGENT_BLOB_BACKEND', 'nats').toLowerCase() === 's3' ? 's3' : 'nats',
    s3: {
      bucket: or('S3_BUCKET', ''),
      region: or('S3_REGION', 'us-east-1'),
      endpoint: or('S3_ENDPOINT', ''),
      accessKeyId: or('S3_ACCESS_KEY', ''),
      secretAccessKey: or('S3_SECRET_KEY', ''),
      forcePathStyle: or('S3_PATH_STYLE', 'true') !== 'false',
      prefix: or('S3_PREFIX', 'abcp'),
    },
    extConfigSeed: parseExtConfigSeed(or('AGENT_EXT_CONFIG_SEED', '')),
  }
}

/**
 * Parse `AGENT_EXT_CONFIG_SEED` (a JSON array of `{tenant,extId,name,value}`)
 * into validated entries. A malformed value is DROPPED with a warning rather
 * than crashing boot — a bad seed must never take the agent down.
 */
export function parseExtConfigSeed(raw: string): ServerConfig['extConfigSeed'] {
  if (raw.trim() === '') return []
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch (e) {
    logger.warn(
      { err: String(e) },
      'AGENT_EXT_CONFIG_SEED: invalid JSON; ignored',
    )
    return []
  }
  if (!Array.isArray(parsed)) {
    logger.warn('AGENT_EXT_CONFIG_SEED: expected a JSON array; ignored')
    return []
  }
  const out: ServerConfig['extConfigSeed'] = []
  for (const entry of parsed) {
    if (typeof entry !== 'object' || entry === null) continue
    const e = entry as Record<string, unknown>
    const tenant = String(e['tenant'] ?? '').trim()
    const extId = String(e['extId'] ?? '').trim()
    const name = String(e['name'] ?? '').trim()
    const value = e['value']
    if (tenant === '' || extId === '' || name === '' || value === undefined) {
      logger.warn(
        { entry },
        'AGENT_EXT_CONFIG_SEED: entry missing fields; dropped',
      )
      continue
    }
    out.push({ tenant, extId, name, value: String(value) })
  }
  return out
}

/**
 * Parse the `DISABLED_TOOLS` value into extension-qualified tool names. Every
 * entry MUST be `<extId>.<name>` (e.g. `bundled.mail-send`); a bare name is
 * ambiguous when two extensions expose the same tool, so it is DROPPED with a
 * warning rather than silently disabling a whole class of tools.
 */
function parseDisabledTools(raw: string): string[] {
  const out: string[] = []
  for (const part of raw.split(',')) {
    const entry = part.trim()
    if (entry === '') continue
    if (!entry.includes('.')) {
      logger.warn(
        { entry },
        'DISABLED_TOOLS entry must be "<extId>.<name>" — dropped',
      )
      continue
    }
    out.push(entry)
  }
  return out
}
