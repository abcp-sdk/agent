import { buildConnectRoutes } from './connect.js'
import {
  type AgentDeps,
  type Bus,
  backfillKvFromPg,
  backfillModelRefs,
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
import { createConnectRouter } from '@connectrpc/connect'
import { createFetchHandler } from '@connectrpc/connect/protocol'
import { Hono } from 'hono'
import { getRequestListener } from '@hono/node-server'
import { createServer as createHttpServer } from 'node:http'
import * as http2Module from 'node:http2'

/** Structural type guard for an unknown value (used for db close hooks). */
function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null
}

/** Runtime type guard for an unknown function value. */
function isFn(v: unknown): v is () => unknown {
  return typeof v === 'function'
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

  // One-time migration: rewrite legacy bare model ids to canonical
  // `provider_id/model_id` references (flat model lookup is gone). Blocks boot
  // because turns must not run against un-migrated refs.
  await backfillModelRefs(db)

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

  const llm = new LlmRegistry()
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
    resolveConfig: async (name, _sessionName) => {
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

  // ---- serving surface: Connect RPC (hono + createFetchHandler) ----
  // Build the Connect router (grpc + grpc-web + connect protocols), then wrap
  // EACH per-RPC universal handler into a Web Request=>Response fetch handler
  // and register it on Hono by its request path. The HTTP server is selected
  // by `config.httpProtocol`:
  //   "auto" — both HTTP/1.1 and cleartext HTTP/2 (h2c) on one port
  //   "h1"   — HTTP/1.1 only
  //   "h2c"  — cleartext HTTP/2 only
  const connectRouter = createConnectRouter({
    grpc: true,
    grpcWeb: true,
    connect: true,
  })
  buildConnectRoutes(deps)(connectRouter)

  const app = new Hono()
  // createFetchHandler goes from a UniversalHandlerFn to (req: Request) =>
  // Response. Each ConnectRouter handler is one RPC; regнster it by path.
  for (const uHandler of connectRouter.handlers) {
    const fetchHandler = createFetchHandler(uHandler)
    app.all(uHandler.requestPath, c => fetchHandler(c.req.raw))
  }
  app.all('*', () =>
    new Response(JSON.stringify({ code: 'unimplemented', message: 'not found' }), {
      status: 404,
      headers: { 'content-type': 'application/json' },
    }),
  )

  // A single node-compatible request listener that adapts http/http2
  // (IncomingMessage|Http2ServerRequest) into Hono's Web Request.
  const requestListener = getRequestListener(app.fetch)

  type ServerLike = {
    close?: (cb?: () => void) => void
    listen: (port: number, cb?: () => void) => unknown
  }
  // Single listener on ONE port; the protocol is chosen by the
  // HTTP_PROTOCOL env (`config.httpProtocol`):
  //   "auto" (default) & "h1" → HTTP/1.1 (ingress terminates TLS and forwards
  //                              h1; browsers / public ingress speak this)
  //   "h2c"                   → cleartext HTTP/2 prior-knowledge (native
  //                              client transports that peer-initiate h2c)
  // Node cannot mux HTTP/1.1 and cleartext h2c on the same socket, so these
  // are mutually exclusive — pick one via the container env var.
  let server: ServerLike | null = null
  const protocol = config.httpProtocol
  if (protocol === 'h2c') {
    const s = http2Module.createServer(
      { allowHTTP1: false } as never,
      requestListener as never,
    )
    s.listen(config.port, () =>
      logger.info({ port: config.port, pid: process.pid }, 'listening (h2c)'),
    )
    server = s
  } else {
    // "auto" defaults to HTTP/1.1: the public ingress / reverse proxy speaks
    // HTTP/1.1 to the backend after terminating TLS (ALPN h2) upstream.
    const s = createHttpServer(requestListener as never)
    s.listen(config.port, () =>
      logger.info({ port: config.port, pid: process.pid }, 'listening (http1)'),
    )
    server = s
  }
  const closeServer = (): Promise<unknown> =>
    new Promise(resolve =>
      server?.close?.(() => resolve(undefined)),
    )

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
    void closeServer().then(() => {
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
