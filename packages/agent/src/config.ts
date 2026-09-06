export function envOr(key: string, fallback: string): string {
  const v = process.env[key]
  return v !== undefined && v !== '' ? v : fallback
}

export interface ServerConfig {
  port: number
  /** Storage backend: "pg" or "sqlite". */
  backend: DbBackend
  postgresUrl: string
  /** The resolved connection string for the selected backend. */
  dbUrl: string
  natsUrl: string
  /** Discovery timeout for extension/tool NATS broadcasts (ms). */
  extensionDiscoverMs: number
  toolTimeoutMs: number
  /** LLM fallback when a session model is unset AND the operator configured an env default. Empty when none is set; sessions then resolve from registered providers only. */
  llmApiType: string
  llmBaseUrl: string
  llmApiKey: string
  llmModel: string
  defaultMaxTurns: number
  defaultTemperature: number
  defaultMaxTokens: number
  /** Model context window (estimated tokens). Compaction budgets are fractions of it. */
  compactionContextTokens: number
  /** File storage backend: "nats" (JetStream object store). */
  filesStorage: string
  /** VLM model ref for image-read ("provider_id/model_id"). */
  imageReadModel: string
}

export type DbBackend = 'pg' | 'sqlite'

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

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ServerConfig {
  const or = (k: string, d: string) => {
    const v = env[k]
    return v !== undefined && v !== '' ? v : d
  }

  const pgUrl = `postgres://${or('POSTGRES_USER', 'root')}:${or('POSTGRES_PASSWORD', 'devpassword')}@${or('POSTGRES_HOST', 'postgres.zergx.svc.cluster.local')}:${or('POSTGRES_PORT', '5432')}/${or('POSTGRES_DB_AGENT', 'zergx_agent')}`

  const backend = resolveBackend(env)

  return {
    port: Number.parseInt(or('ZERGX_PORT', '8080'), 10),
    backend,
    postgresUrl: pgUrl,
    dbUrl:
      backend === 'sqlite'
        ? or('DATABASE_URL', 'sqlite:///data/zergx-agent.db')
        : pgUrl,
    natsUrl: or('NATS_URL', 'nats://nats.zergx.svc.cluster.local:4222'),
    extensionDiscoverMs: Number.parseInt(
      or('ZERGX_EXTENSION_DISCOVER_MS', '500'),
      10,
    ),
    toolTimeoutMs:
      Number.parseInt(or('ZERGX_TOOL_TIMEOUT_SECS', '600'), 10) * 1000,
    llmApiType: or('ZERGX_LLM_API_TYPE', ''),
    llmBaseUrl: or('ZERGX_LLM_BASE_URL', ''),
    llmApiKey: or('ZERGX_LLM_API_KEY', ''),
    llmModel: or('ZERGX_LLM_MODEL', ''),
    defaultMaxTurns: Number.parseInt(or('ZERGX_DEFAULT_MAX_TURNS', '25'), 10),
    defaultTemperature: Number.parseFloat(or('ZERGX_LLM_TEMPERATURE', '0')),
    defaultMaxTokens: Number.parseInt(or('ZERGX_LLM_MAX_TOKENS', '32768'), 10),
    compactionContextTokens: Number.parseInt(
      or('ZERGX_COMPACTION_CONTEXT_TOKENS', '200000'),
      10,
    ),
    filesStorage: or('FILE_STORAGE', 'nats'),
    imageReadModel: or('IMAGE_READ_MODEL', ''),
  }
}
