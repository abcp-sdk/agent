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

  const pgUrl = `postgres://${or('POSTGRES_USER', 'root')}:${or('POSTGRES_PASSWORD', 'devpassword')}@${or('POSTGRES_HOST', 'postgres.easylab.svc.cluster.local')}:${or('POSTGRES_PORT', '5432')}/${or('POSTGRES_DB_AGENT', 'easylab_agent')}`

  const backend = resolveBackend(env)

  return {
    port: Number.parseInt(or('PORT', '8080'), 10),
    httpProtocol: normalizeHttpProtocol(or('HTTP_PROTOCOL', 'auto')),
    backend,
    dbUrl:
      backend === 'sqlite'
        ? or('DATABASE_URL', 'sqlite:///data/easylab-agent.db')
        : pgUrl,
    natsUrl: or('NATS_URL', 'nats://nats.easylab.svc.cluster.local:4222'),
    toolTimeoutMs: TOOL_TIMEOUT_MS,
    defaultMaxTurns: DEFAULT_MAX_TURNS,
    corsOrigin: or('AGENT_CORS_ORIGIN', '*'),
    authMode:
      or('AGENT_AUTH_MODE', 'required') === 'none' ? 'none' : 'required',
    adminToken: or('AGENT_ADMIN_TOKEN', ''),
    defaultTenant: or('AGENT_DEFAULT_TENANT', 'default'),
    bootstrapTenant: or('AGENT_BOOTSTRAP_TENANT', ''),
    bootstrapToken: or('AGENT_BOOTSTRAP_TOKEN', ''),
  }
}
