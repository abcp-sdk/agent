import { buildConnectRoutes } from './connect.js'
import {
  type AgentDeps,
  type Bus,
  backfillKvFromPg,
  calibrateMessageFacts,
  connectBus,
  connectDb,
  type Db,
  LlmRegistry,
  loadConfig,
  logger,
  Mailbox,
  makeBlobStore,
  Presets,
  rawAll,
  rawRun,
  refreshModelsDev,
  runSessionTurn,
  watchMailboxWake,
} from '@easylab-agent/agent'
import { serveBundled } from '@abc-protocol/bundled-extension'
import {
  connectNodeAdapter,
  type ConnectNodeAdapterOptions,
} from '@connectrpc/connect-node'
import { createServer } from 'node:http'
import { buildApp } from './app.js'
import { handleRest, isRecord, serveStatic } from './http.js'

/** Runtime type guard for an unknown function value. */
function isFn(v: unknown): v is () => unknown {
  return typeof v === 'function'
}

/**
 * The `node:sea` module surface this server depends on. The API only exists
 * inside a single-executable application; we probe it structurally so no
 * assumptions are made about the resolved module.
 */
interface SeaModule {
  getRawAsset: (key: string) => ArrayBuffer | undefined
}

/** Structural type guard for the optional `node:sea` module. */
function isSeaModule(value: unknown): value is SeaModule {
  return (
    typeof value === 'object' &&
    value !== null &&
    'getRawAsset' in value &&
    typeof value.getRawAsset === 'function'
  )
}

/**
 * `node:sea`'s `getRawAsset` is only available inside a single-executable
 * application. We load it lazily via the CJS `require` (the whole server is
 * bundled to CJS for SEA) so a normal `node` run does not crash at import.
 * The try/catch is the sanctioned CJS-interop boundary.
 */
function getSeaAsset(key: string): ArrayBuffer | null {
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const sea: unknown = require('node:sea')
    if (!isSeaModule(sea)) return null
    return sea.getRawAsset(key) ?? null
  } catch {
    return null
  }
}

async function main(): Promise<void> {
  // Process-level safety nets: log instead of crash. Node >=15 terminates the
  // process on unhandled rejections by default, so one stray floating promise
  // (e.g. a NATS blip during a fire-and-forget bookkeeping call) must never
  // take the whole agent down.
  process.on('unhandledRejection', reason => {
    logger.error({ reason: String(reason) }, 'unhandledRejection')
  })
  process.on('uncaughtException', err => {
    logger.error({ err: String(err) }, 'uncaughtException — exiting')
    setTimeout(() => process.exit(1), 1000).unref()
  })

  const config = loadConfig()

  const dbRes = await connectDb(config.backend, config.dbUrl)
  if (dbRes.isErr()) {
    logger.error({ err: dbRes.error }, 'db connect failed')
    process.exit(1)
  }
  const db: Db = dbRes.value

  const busRes = await connectBus(config.natsUrl)
  if (busRes.isErr()) {
    logger.error({ err: busRes.error }, 'event bus connect failed (required)')
    process.exit(1)
  }
  const bus: Bus = busRes.value

  // One-time message-fact calibration: refresh the abc-session-state KV
  // projection from PG so chat-list previews are correct even for sessions
  // that predate the projection or whose KV writes were missed.
  void calibrateMessageFacts(bus, db)

  // One-time PG → KV migration for presets / config / files-meta (marker-
  // guarded per domain; a failure retries on the next boot).
  void backfillKvFromPg(db, bus)

  // Seed the immutable system presets (create-if-absent). Idempotent across
  // replicas and restarts; never overwrites what a restore/user already set.
  void Presets.seedDefaults(bus)

  // Mailbox retention: consumed rows are audit-only, prune past the window.
  const retentionDays = Number.parseInt(
    process.env.MAILBOX_RETENTION_DAYS ?? '7',
    10,
  )
  if (Number.isFinite(retentionDays) && retentionDays > 0) {
    const sweep = async () => {
      const r = await Mailbox.purgeConsumed(db, retentionDays)
      if (r.isErr()) logger.warn({ err: r.error }, 'mailbox purge failed')
      else if (r.value > 0)
        logger.info({ n: r.value }, 'purged consumed mailbox rows')
    }
    void sweep()
    const timer = setInterval(() => void sweep(), 60 * 60 * 1000)
    timer.unref()
  }

  const llm = new LlmRegistry(config)
  const files = makeBlobStore(bus)
  const deps: AgentDeps = {
    db,
    bus,
    config,
    llm,
    files,
  }

  // ---- bundled extension (in-process) ----
  // Serve the bundled extension over the same bus so its tools are discover-
  // able. It is an optional lib: if it fails to register we log and continue
  // (the agent still works standalone without the bundled toolset).
  const stopBundled = serveBundled({
    bus,
    resolveModel: (db, modelId) => llm.resolve(db as Db, modelId),
    blobGet: (code) => files.get(code).then(r => ({ meta: { ...r.meta } as Record<string, unknown>, data: r.data })),
    rawAll: (sql, params) => rawAll(db, sql, params),
    rawRun: (sql, params) => rawRun(db, sql, params),
    // Config lives in the `cfg` KV bucket (source of truth for extensions).
    // Session-scoped overrides are applied by the Extension itself; here we
    // resolve the effective global value (envelope-aware {r,v} format).
    resolveConfig: async (name, sessionName) => {
      const raw = await bus.kvGet('cfg', `bundled.${name}`)
      if (raw === null || raw === undefined) return undefined
      try {
        const z = JSON.parse(raw) as { v?: unknown }
        if (z && typeof z === 'object' && 'v' in z) return z.v
        return z
      } catch {
        return raw
      }
    },
  })

  // ---- serving surface: HTTP/1.1 + HTTP/2 (cleartext), no framework ----
  // 1. RPC: the Connect AgentService (/agent.v1.AgentService/*) is served by
  //    @connectrpc/connect-node on connect + gRPC protocols. The adapter is
  //    compatible with BOTH node:http (http/1.1) and node:http2 listeners, so
  //    revers-proxies that speak http/1.1 (e.g. the public ingress) can reach
  //    it without an h2-clear upgrade.
  // 2. REST facade (/api/v1) + the SEA-served SPA ride the adapter fallback
  //    through a tiny strongly-typed native dispatcher.
  // Clients may speak HTTP/1.1 (public ingress) or HTTP/2 prior knowledge.

  const restRouter = buildApp()

  const fallback: NonNullable<ConnectNodeAdapterOptions['fallback']> = async (
    req,
    res,
  ) => {
    const url = new URL(
      req.url ?? '/',
      `http://${req.headers.host ?? 'localhost'}`,
    )
    const pathname = url.pathname
    if (pathname === '/api/v1' || pathname.startsWith('/api/v1/')) {
      const rel = pathname.slice('/api/v1'.length) || '/'
      await handleRest(restRouter, deps, rel, req, res)
      return
    }
    serveStatic(res, getSeaAsset, pathname)
  }

  const handler = connectNodeAdapter({
    routes: buildConnectRoutes(deps),
    grpc: true,
    grpcWeb: true,
    connect: true,
    fallback,
  })

  const server = createServer(handler)
  server.listen(config.port, () => {
    logger.info({ port: config.port, pid: process.pid }, 'listening (http1)')
  })

  // Watch every session's mailbox wake wildcard so this replica can claim and
  // run work for any session — the horizontal scale-out trigger.
  const stopWake = watchMailboxWake(deps)

  // Populate the models.dev catalog cache once at startup (30min TTL); a
  // failing fetch is non-fatal — the catalog is a prefill convenience.
  void refreshModelsDev(bus).then(
    () => {},
    e => logger.warn({ err: String(e) }, 'models.dev refresh failed'),
  )

  /** Structural access to the sqlite/pg client's close hook (no casts). */
  const closeDb = (): Promise<unknown> => {
    if (!isRecord(db)) return Promise.resolve()
    const client = db['$client']
    if (!isRecord(client)) return Promise.resolve()
    const endFn = client['end']
    if (isFn(endFn)) return Promise.resolve(endFn.call(client))
    const closeFn = client['close']
    if (isFn(closeFn)) return Promise.resolve(closeFn.call(client))
    return Promise.resolve()
  }

  const shutdown = () => {
    logger.info('shutting down')
    stopWake()
    void stopBundled()
    server.close(() => {
      bus.close()
      void closeDb().then(
        () => process.exit(0),
        () => process.exit(0),
      )
      setTimeout(() => process.exit(0), 3000).unref()
    })
  }
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  // Recovery: re-run turns for sessions whose prompts were persisted into PG
  // but whose turn never ran (e.g. replica died after ack before
  // runSessionTurn). The claim mechanism keeps this safe across replicas
  // (exactly one wins the per-session lease).
  const pending = await Mailbox.pendingSessions(db)
  if (pending.isOk()) {
    for (const sid of pending.value) {
      void runSessionTurn(deps, sid).then(
        () => {},
        e => logger.error({ sid, err: String(e) }, 'recovery turn crashed'),
      )
    }
  }
}

void main()
