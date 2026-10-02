import { createServer as createHttpServer } from 'node:http'
import * as http2Module from 'node:http2'
import { serveBundled } from '@abc-protocol/bundled-extension'
import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  type AgentDeps,
  BUCKET_CONFIG,
  type Bus,
  backfillKvFromPg,
  backfillMessageSeqFromKv,
  backfillModelRefs,
  calibrateMessageFacts,
  configureFileMetaStore,
  connectBus,
  connectDb,
  type Db,
  ensureLockBuckets,
  IdleWatchdog,
  initReadSeq,
  knownTenants,
  LlmRegistry,
  loadConfig,
  logger,
  Mailbox,
  Messages,
  makeBlobStore,
  natsToken,
  Presets,
  Providers,
  rawAll,
  rawRun,
  reconcileOwnLeases,
  refreshModelsDev,
  runSessionTurn,
  S3ObjectStore,
  type ServerConfig,
  serveFileRpc,
  startHeartbeat,
  storeFile,
  Tenants,
  tenantKVKey,
  WAKE_MAILBOX_TYPES,
  watchMailboxWake,
} from '@abcp-agent/agent'
import { createConnectRouter } from '@connectrpc/connect'
import { createFetchHandler } from '@connectrpc/connect/protocol'
import { getRequestListener } from '@hono/node-server'
import { Hono } from 'hono'
import { buildAdminRoutes } from './admin.js'
import { makeAuth } from './auth.js'
import { buildConnectRoutes } from './connect.js'
import { defaultTenant } from './tenant.js'

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

  // The tenant a request falls back to when none is supplied (pre-auth /
  // single-tenant deployments).
  const fallbackTenant = defaultTenant()

  // First-boot bootstrap: when the tenant table is empty and a bootstrap
  // tenant+token are configured, create the tenant and mint that exact token.
  // This is how a fresh standalone deployment gets its first credential.
  // MUST run before the tenant enumeration below: everything keyed by the
  // known-tenant set (preset seeding, message-fact calibration) has to
  // already see the bootstrap tenant on its very first boot.
  await bootstrapTenantIfEmpty(db, config)

  // The tenants this deployment knows about. There is no external tenant
  // registry: the set is derived from the DB plus the configured fallback.
  const tenants = await knownTenants(db, fallbackTenant).then(
    r => (r.isOk() ? r.value : [fallbackTenant]),
    () => [fallbackTenant],
  )

  // One-time migration: rewrite legacy bare model ids to canonical
  // `provider_id/model_id` references (flat model lookup is gone). Blocks boot
  // because turns must not run against un-migrated refs.
  await backfillModelRefs(db, tenants)

  // One S3 store instance when the S3 blob backend is selected; it both
  // supplies durable objects to the bus and (via its native GetObject stream)
  // powers true streaming file reads.
  const s3Store =
    config.blobBackend === 's3'
      ? new S3ObjectStore({
          ...config.s3,
          forcePathStyle: config.s3.forcePathStyle,
        })
      : undefined
  const busRes = await connectBus(config.natsUrl, {
    ...(s3Store !== undefined ? { durableObjects: s3Store } : {}),
    // Ride out a broker restart / slow-consumer disconnect forever, and wait
    // for the broker on first boot. Without this nats.js gives up after 10
    // attempts and the process lives on as a zombie that answers HTTP but can
    // never publish again.
    maxReconnectAttempts: -1,
    waitOnFirstConnect: true,
  })
  if (busRes.isErr()) {
    logger.error({ err: busRes.error }, 'event bus connect failed (required)')
    process.exit(1)
  }
  const bus: Bus = busRes.value

  // Supervise the bus: if NATS closes PERMANENTLY (reconnect budget exhausted,
  // auth violation, or an explicit close) the process is unusable — a zombie
  // that still serves HTTP while every publish/subscribe fails. Exit non-zero
  // so Kubernetes restarts the pod and rebuilds a fresh connection. `closing`
  // guards the graceful-shutdown path (where bus.close() is intentional).
  let shuttingDown = false
  void bus.closed().then(() => {
    if (shuttingDown) return
    logger.error('event bus closed permanently — exiting for restart')
    process.exit(1)
  })

  // Pin the run-lease + owner-heartbeat buckets' TTLs BEFORE anything can
  // create them with the wrong one. `WatchSessions` (and any `bus.kvWatch`)
  // creates a missing bucket persistent (ttl=0); if that wins the race, lease
  // keys never expire and a mid-turn crash leaves a stale `running` lease that
  // reports the session busy forever. Awaited so it precedes the watchers.
  await ensureLockBuckets(bus)

  // Boot reconciliation: a fresh process owns no live turns, so any lease
  // stamped with THIS instance id (or a leftover heartbeat) is a crash
  // remnant. Clear them BEFORE serving so a crashed-mid-turn session reads
  // idle immediately instead of after the lease TTL. Awaited for the same
  // reason (must precede the watchers/snapshot).
  await reconcileOwnLeases(bus, tenants)

  // Instance heartbeat: the run lock treats a session as busy only while its
  // owner's heartbeat is alive, so a crashed replica's sessions read idle
  // without waiting for the lease TTL.
  const stopHeartbeat = startHeartbeat(bus)

  // File metadata backend follows the blob backend: `nats` keeps it in the
  // abc-files-meta KV; `s3` puts it in the `agent_files` DB table so file
  // state (bytes + metadata) leaves NATS together.
  configureFileMetaStore(config.blobBackend === 's3' ? 'db' : 'nats', db)

  // One-time seed of the authoritative `sessions.message_seq` from the legacy
  // KV fact counter. AWAITED and BEFORE calibration (which rewrites facts
  // WITHOUT the legacy counter, so it must not run first). A turn must not start
  // from 0 and reset clients' persisted read watermarks. Idempotent
  // (marker-guarded, never regresses an advanced value).
  await backfillMessageSeqFromKv(db, bus, tenants)

  // One-time read-watermark init: seed `read_seq = message_seq` so pre-existing
  // history is not flagged as unread the moment server-side unread lands.
  // AWAITED before serving; idempotent (marker-guarded).
  await initReadSeq(db, bus, tenants)

  // One-time message-fact calibration: refresh the abc-session-state KV
  // projection from PG so chat-list previews are correct even for sessions
  // that predate the projection or whose KV writes were missed.
  for (const t of tenants) void calibrateMessageFacts(bus, t, db)

  // One-time PG → KV migration for presets / config / files-meta (marker-
  // guarded per domain; a failure retries on the next boot).
  void backfillKvFromPg(db, bus)

  // Seed the immutable system presets per tenant (create-if-absent).
  // Idempotent across replicas and restarts; never overwrites what a
  // restore/user already set.
  for (const t of tenants) void Presets.seedDefaults(bus, t)

  // Seed deployment-pinned EXTENSION config (e.g. the default workspace
  // worker-url) into the `cfg` KV bucket. Create-if-absent: an existing value
  // (a user set) always wins, so this is safe to run on every boot. A seed
  // entry with tenant "*" applies to EVERY known tenant.
  seedExtConfig(bus, config, tenants)

  // Seed deployment-pinned PROVIDERS for tenants that have none yet. This is
  // how a fresh standalone deployment gets a working model registry without a
  // UI round-trip. Never overwrites a tenant that already has providers.
  void seedProviders(db, config, tenants)

  // Seed deployment-pinned TENANT config (e.g. `default_model`, so a fresh
  // session has a working model on its first turn without a UI round-trip).
  // Create-if-absent: a value a user later set always wins.
  seedTenantConfig(bus, config, tenants)

  // Mailbox retention: consumed rows are audit-only, prune past a fixed window.
  const retentionDays = 7
  if (retentionDays > 0) {
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
  const files = makeBlobStore(bus, s3Store)
  // Agent-served file RPCs (`abc.<tenant>.file.ingest`/`.get`): let a DB-less,
  // store-less extension (e.g. the playwright extension) persist bytes + mint a
  // canonical file:<code> through the agent, regardless of blob backend.
  const _stopFileRpc = serveFileRpc({ bus, files })
  // ONE long-lived abc agent role: it owns the extension-manifest cache and
  // the config authority. `serveConfig()` must run before any config write,
  // otherwise SetExtensionConfig fails with an opaque internal error (the
  // authority would be created per call and immediately discarded).
  const abcAgent = new AbcAgent(bus)
  await abcAgent.serveConfig()
  const deps: AgentDeps = {
    db,
    bus,
    config,
    llm,
    files,
    agent: abcAgent,
  }

  // ---- bundled extension (in-process) ----
  // Serve the bundled extension over the same bus so its tools are discover-
  // able. It is an optional lib: if it fails to register we log and continue
  // (the agent still works standalone without the bundled toolset).
  const stopBundled = serveBundled({
    bus,
    // `db` is structurally passed by the extension but is always null — the
    // resolver owns the real handle (`deps.db` from the closure).
    resolveModel: (_db, modelId, tenant) =>
      llm.resolve(db as Db, tenant ?? fallbackTenant, modelId),
    // Generation models (image/video/speech) resolve from the SAME provider
    // registry, capability-tagged; the config knobs hold provider_id/model_id.
    resolveGenerative: (capability, ref, tenant) =>
      llm.resolveGenerative(
        db as Db,
        tenant ?? fallbackTenant,
        ref,
        capability,
      ),
    blobGet: (code, tenant) =>
      files.get(tenant ?? fallbackTenant, code).then(r => ({
        meta: { ...r.meta } as Record<string, unknown>,
        data: r.data,
      })),
    // Generated media (images/videos/audio) land in the same blob store as
    // uploaded files so they can be referenced as file:<code> afterwards. The
    // content type is DERIVED from the bytes (the generator's mediaType is not
    // trusted); the derived mime is returned to the caller.
    ingestBlob: async ({ bytes, name, session, tenant }) => {
      const t = tenant ?? fallbackTenant
      const data = new Uint8Array(Buffer.from(bytes, 'base64'))
      const record = await storeFile(
        { bus, files },
        { tenant: t, data, name, uploaderSession: session },
      )
      return { code: record.code, mime: record.mime, name: record.name }
    },
    // ---- Narrow data access for the bundled extension ----
    // The extension never speaks SQL: the HOST owns the schema and serves
    // these named operations (see BundledDeps in bundled-extension/deps.ts).
    sessionGroup: async (tenant, sid) => {
      const rows = await rawAll(
        db,
        'SELECT "group" AS g FROM sessions WHERE tenant = ? AND name = ?',
        [tenant, sid],
      )
      return rows.length > 0 ? String(rows[0]!['g'] ?? '') : ''
    },
    sessionExists: async (tenant, sid) => {
      const rows = await rawAll(
        db,
        'SELECT 1 AS x FROM sessions WHERE tenant = ? AND name = ? LIMIT 1',
        [tenant, sid],
      )
      return rows.length > 0
    },
    sessionTip: async (tenant, sid) => {
      const rows = await rawAll(
        db,
        'SELECT tip_id AS t FROM sessions WHERE tenant = ? AND name = ?',
        [tenant, sid],
      )
      return rows.length > 0 ? String(rows[0]!['t'] ?? '') : ''
    },
    sessionsInGroup: async (tenant, group) => {
      const rows = await rawAll(
        db,
        'SELECT name AS n FROM sessions WHERE tenant = ? AND "group" = ?',
        [tenant, group],
      )
      return rows.map(r => String(r['n'] ?? '')).filter(n => n !== '')
    },
    forkSession: async (tenant, parent, child) => {
      // Anchor the child at the message BEFORE the parent's current-turn prompt.
      // The assistant step that is invoking this tool is not persisted yet, so
      // the parent tip IS the prompt that started this turn (e.g. "start 10
      // subsessions"). Inheriting it made every child believe it owned that
      // instruction. forkBase() steps back over that prompt; the child then
      // receives its own task as the handoff trigger.
      const parentTip = await rawAll(
        db,
        'SELECT tip_id AS t FROM sessions WHERE tenant = ? AND name = ?',
        [tenant, parent],
      )
      const tip = parentTip.length > 0 ? String(parentTip[0]!['t'] ?? '') : ''
      const baseRes = await Messages.forkBase(
        db,
        tenant,
        tip === '' ? null : tip,
      )
      if (baseRes.isErr()) throw new Error(baseRes.error)
      const forkTip = baseRes.value
      await rawRun(
        db,
        `INSERT INTO sessions
           (tenant, name, model, variant, preset, tip_id, max_turns,
            system_prompt, locale, "group", created_at, updated_at)
         SELECT tenant, ?, model, variant, preset, ?, max_turns,
                system_prompt, locale, ?, datetime('now'), datetime('now')
         FROM sessions WHERE tenant = ? AND name = ?`,
        [child, forkTip, parent, tenant, parent],
      )
    },
    deleteSessionRow: (tenant, sid) =>
      rawRun(db, 'DELETE FROM sessions WHERE tenant = ? AND name = ?', [
        tenant,
        sid,
      ]),
    deleteSessionMailbox: (tenant, sid) =>
      rawRun(db, 'DELETE FROM mailbox WHERE tenant = ? AND session_name = ?', [
        tenant,
        sid,
      ]),
    messageChain: async (tenant, tip, limit) => {
      const rows = await rawAll(
        db,
        `WITH RECURSIVE chain AS (
           SELECT m.id, m.role, m.prev_id, m.created_at, 0 AS depth
           FROM messages m WHERE m.id = ? AND m.tenant = ?
           UNION ALL
           SELECT m.id, m.role, m.prev_id, m.created_at, c.depth + 1
           FROM messages m JOIN chain c ON m.id = c.prev_id
           WHERE m.tenant = ?
         )
         SELECT id, role, created_at, depth FROM chain WHERE depth < ? ORDER BY depth ASC`,
        [tip, tenant, tenant, limit],
      )
      return rows.map(r => ({
        id: String(r['id'] ?? ''),
        role: String(r['role'] ?? ''),
        createdAt: String(r['created_at'] ?? ''),
        depth: Number(r['depth'] ?? 0),
      }))
    },
    messageParts: async (tenant, ids) => {
      if (ids.length === 0) return []
      const placeholders = ids.map(() => '?').join(',')
      const rows = await rawAll(
        db,
        `SELECT message_id, type, seq, data FROM parts
         WHERE tenant = ? AND message_id IN (${placeholders})
         ORDER BY message_id, seq`,
        [tenant, ...ids],
      )
      return rows.map(r => ({
        messageId: String(r['message_id'] ?? ''),
        type: String(r['type'] ?? ''),
        seq: Number(r['seq'] ?? 0),
        data: String(r['data'] ?? ''),
      }))
    },
    // The `bundled_todos` table is now OWNED by the agent's schema bootstrap
    // (db-client.ts DDL + additive migrations), which is the single migration
    // authority. A lazy CREATE TABLE IF NOT EXISTS here could never add the
    // `tenant` column to a legacy table (and rawRun would silently drop the
    // trailing CREATE INDEX on sqlite). Nothing to do at tool-set time.
    todosEnsure: async () => {},
    todosReplace: async (tenant, sid, rows) => {
      await rawRun(
        db,
        'DELETE FROM bundled_todos WHERE tenant = ? AND session_id = ?',
        [tenant, sid],
      )
      for (const r of rows) {
        await rawRun(
          db,
          'INSERT INTO bundled_todos (tenant, session_id, content, status, priority) VALUES (?,?,?,?,?)',
          [tenant, sid, r.content, r.status, r.priority],
        )
      }
    },
    // Cross-session messaging (subsession handoff + direct session-send):
    // the bundled tools deliver to any session's mailbox over the same bus.
    publishMailbox: (tenant, sessionName, type, payload, source) =>
      new AbcAgent(bus).publishMailbox(
        tenant,
        sessionName,
        type,
        payload,
        source ?? '',
      ),
    // Config lives in the `cfg` KV bucket (source of truth for extensions).
    // Session-scoped overrides are applied by the Extension itself; here we
    // resolve the effective global value (envelope-aware {r,v} format).
    // Session-scoped variables the agent projects (vars bucket, provider
    // "agent"). Used by bundled tools that need the session's locale etc.
    getSessionVariable: async (
      tenant: string,
      provider: string,
      sessionName: string,
      name: string,
    ) => {
      const raw = await bus
        .kvGet(
          'vars',
          tenantKVKey(tenant, `${provider}.${natsToken(sessionName)}.${name}`),
        )
        .catch(() => null)
      return raw ?? undefined
    },
    resolveConfig: async (name, sessionName, tenant) => {
      const t = tenant ?? fallbackTenant
      // Extension config lives in the `cfg` KV bucket (the SDK's
      // ConfigAuthority writes there), keyed tenant-first:
      //   global  -> t.<tenant>.<extId>.<name>
      //   session -> t.<tenant>.<extId>.<escapedSession>.<name>
      // Read the effective value envelope-aware ({r,v} with a bare fallback),
      // trying the session override first.
      const readCfg = async (key: string): Promise<unknown> => {
        const raw = await bus.kvGet('cfg', tenantKVKey(t, key))
        if (raw === null || raw === undefined || raw === '') return undefined
        try {
          const z = JSON.parse(raw) as unknown
          if (z !== null && typeof z === 'object' && 'v' in z) {
            return (z as { v: unknown }).v
          }
          return z
        } catch {
          return raw
        }
      }
      if (sessionName !== undefined && sessionName !== '') {
        const scoped = await readCfg(`bundled.${sessionName}.${name}`)
        if (scoped !== undefined) return scoped
      }
      return readCfg(`bundled.${name}`)
    },
  })

  // Populate the models.dev catalog cache BEFORE serving. `ListModels` and
  // `TestProvider` derive reasoning variants from it, so a request racing this
  // (async) fetch would otherwise observe zero variants on a cold start. The
  // fetch falls back to the bundled snapshot, so it never blocks boot for long.
  await refreshModelsDev(bus).then(
    () => {},
    e => logger.warn({ err: String(e) }, 'models.dev refresh failed'),
  )
  // Keep the catalog fresh: the read side caches for 30 minutes, so refresh
  // on the same cadence — without this the catalog went stale until restart
  // (the snapshot fallback only ever applied at boot).
  const catalogRefreshMs = 30 * 60 * 1000
  const catalogTimer = setInterval(
    () =>
      void refreshModelsDev(bus).then(
        () => {},
        e => logger.warn({ err: String(e) }, 'models.dev refresh failed'),
      ),
    catalogRefreshMs,
  )
  catalogTimer.unref()

  // Idle-turn watchdog: re-trigger a session that stopped mid-task after a tool
  // call. Opt-in via `IDLEWATCH_ENABLED` (default off). Ported from the
  // workspace-gateway so the behaviour lives with the agent; the gateway copy
  // can then be disabled.
  const idlewatch = new IdleWatchdog({ db, bus, config })
  idlewatch.start()

  // ---- serving surface: Connect RPC (hono + createFetchHandler) ----
  // Build the Connect router (grpc + grpc-web + connect protocols), then wrap
  // EACH per-RPC universal handler into a Web Request=>Response fetch handler
  // and register it on Hono by its request path. The HTTP server is selected
  // by `config.httpProtocol`:
  //   "auto" — both HTTP/1.1 and cleartext HTTP/2 (h2c) on one port
  //   "h1"   — HTTP/1.1 only
  //   "h2c"  — cleartext HTTP/2 only
  // ---- authentication (token -> tenant) ----
  // A single interceptor gates BOTH services: Health is public; AdminService
  // requires the static admin token; AgentService requires a tenant token.
  const auth = makeAuth(deps, {
    mode: config.authMode,
    adminToken: config.adminToken,
    defaultTenant: config.defaultTenant,
  })
  const connectRouter = createConnectRouter({
    grpc: true,
    grpcWeb: true,
    connect: true,
    interceptors: [auth.interceptor],
  })
  buildConnectRoutes(deps)(connectRouter)
  buildAdminRoutes(deps, auth.invalidate)(connectRouter)

  const app = new Hono()
  // ---- CORS (browser / Flutter Web clients) ----
  // Connect RPC uses POST with a non-simple content-type, so the browser sends
  // an OPTIONS preflight. Answer it and attach the CORS headers to every
  // response. `AGENT_CORS_ORIGIN` (default `*`) sets the allowed origin.
  const corsOrigin = config.corsOrigin
  const corsHeaders: Record<string, string> = {
    'Access-Control-Allow-Origin': corsOrigin,
    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
    'Access-Control-Allow-Headers':
      'content-type, connect-protocol-version, connect-timeout-ms, ' +
      'grpc-timeout, x-grpc-web, x-user-agent, authorization',
    'Access-Control-Expose-Headers':
      'grpc-status, grpc-message, connect-protocol-version',
    'Access-Control-Max-Age': '86400',
    Vary: 'Origin',
  }
  app.options(
    '*',
    _c => new Response(null, { status: 204, headers: corsHeaders }),
  )
  app.use('*', async (c, next) => {
    await next()
    for (const [k, v] of Object.entries(corsHeaders)) c.res.headers.set(k, v)
  })
  // createFetchHandler goes from a UniversalHandlerFn to (req: Request) =>
  // Response. Each ConnectRouter handler is one RPC; regнster it by path.
  for (const uHandler of connectRouter.handlers) {
    const fetchHandler = createFetchHandler(uHandler)
    app.all(uHandler.requestPath, c => fetchHandler(c.req.raw))
  }
  app.all(
    '*',
    () =>
      new Response(
        JSON.stringify({ code: 'unimplemented', message: 'not found' }),
        {
          status: 404,
          headers: { 'content-type': 'application/json' },
        },
      ),
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
    new Promise(resolve => server?.close?.(() => resolve(undefined)))

  // ---- liveness / readiness endpoint (plain HTTP/1.1, separate port) ----
  // The RPC surface may be h2c-only, which a kubelet `httpGet` probe (HTTP/1.1)
  // cannot speak; and readiness must reflect the BUS, not just "the port
  // accepts connections" — a zombie whose NATS connection died would otherwise
  // stay Ready and black-hole every session. Healthy ⇒ 200; not Ready (bus
  // permanently closed, or still starting) ⇒ 503. Shutdown ⇒ 503 so the pod is
  // pulled from the Service before it drains.
  let ready = false
  let healthServer: ReturnType<typeof createHttpServer> | null = null
  if (config.healthPort > 0) {
    healthServer = createHttpServer((req, res) => {
      const path = (req.url ?? '/').split('?')[0]
      if (path !== '/health' && path !== '/healthz' && path !== '/ready') {
        res.writeHead(404).end()
        return
      }
      if (path === '/ready' && !ready) {
        res.writeHead(503, { 'content-type': 'text/plain' }).end('not ready')
        return
      }
      res.writeHead(200, { 'content-type': 'text/plain' }).end('ok')
    })
    healthServer.listen(config.healthPort, () =>
      logger.info({ port: config.healthPort }, 'health listening (http1)'),
    )
  }
  const closeHealth = (): Promise<unknown> =>
    new Promise(resolve => healthServer?.close?.(() => resolve(undefined)))

  // Watch every session's mailbox wake wildcard so this replica can claim and
  // run work for any session — the horizontal scale-out trigger.
  const stopWake = watchMailboxWake(deps)
  // Everything the server needs is now wired; mark Ready.
  ready = true

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
    shuttingDown = true
    ready = false
    stopWake()
    idlewatch.stop()
    stopHeartbeat()
    void stopBundled()
    void closeHealth()
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
  const recoverPending = async (reason: 'boot' | 'sweep'): Promise<void> => {
    // Only sessions with a WAKE-type row (trigger/compact) are recovered; a
    // session holding only context-only rows (e.g. a lone `event`) folds lazily
    // and must not be re-woken (that would emit a spurious idle).
    const pending = await Mailbox.pendingSessions(db, WAKE_MAILBOX_TYPES)
    if (pending.isErr()) {
      logger.warn({ reason, err: pending.error }, 'pending scan failed')
      return
    }
    if (pending.value.length > 0) {
      // A busy session legitimately has a pending row mid-turn, so the periodic
      // sweep logs at debug to avoid noise; boot is one-shot and worth a warn.
      const fields = { reason, sessions: pending.value.length }
      const msg = 'recovering sessions with pending mailbox rows'
      if (reason === 'boot') logger.warn(fields, msg)
      else logger.debug(fields, msg)
    }
    for (const item of pending.value) {
      void runSessionTurn(deps, item.tenant, item.session_name).then(
        () => {},
        e =>
          logger.error(
            { tenant: item.tenant, sid: item.session_name, err: String(e) },
            'recovery turn crashed',
          ),
      )
    }
  }
  await recoverPending('boot')

  // Boot recovery of turns KILLED MID-FLIGHT (the previous process was
  // SIGTERM'd / OOM'd / restarted): recoverPending only re-runs sessions with
  // a pending mailbox row, but a killed turn had already consumed its wake —
  // the durable trace is the chain having advanced PAST the tip the last turn
  // recorded as its end (`fact.last_turn_tip`), which the idlewatch sweep
  // resumes. Delayed past the owner-heartbeat/lease TTL (30s) so a crashed
  // replica's sessions read idle by then; the sweep is idempotent and the
  // per-session lease arbitrates against any turn that already restarted.
  if (config.resumeDanglingOnBoot) {
    const bootResume = setTimeout(() => {
      void idlewatch.sweepOnce().catch(err => {
        logger.warn({ err: String(err) }, 'boot dangling-resume sweep failed')
      })
    }, 45_000)
    bootResume.unref()
    logger.info('resume-dangling-on-boot: sweep scheduled in 45s')
  }

  // Periodic backstop: a wake that was lost (transient claim failure, replica
  // restart mid-ack, …) leaves a row pending with no further trigger. Re-scan
  // and re-run those sessions. The per-session lease arbitrates across
  // replicas, so concurrent sweeps are safe and a genuinely-busy session is a
  // no-op. This closes the "message enqueued but turn never ran" hole without
  // relying solely on boot recovery.
  const pendingSweepMs = 15_000
  const sweepTimer = setInterval(
    () => void recoverPending('sweep'),
    pendingSweepMs,
  )
  sweepTimer.unref()
}

void main()

/**
 * Seed deployment-pinned extension config into the `cfg` KV bucket.
 *
 * The SDK's ConfigAuthority reads/writes `cfg` keys `t.<tenant>.<extId>.<name>`
 * with the envelope `{r,v}`. We seed ONLY when the key is absent (kvCreate
 * fails on an existing key), so a value a user later sets via the UI — or a
 * value already present from a prior boot — is never clobbered. This gives a
 * fresh deployment a working default (e.g. the in-cluster easyworker URL)
 * without any UI round-trip, while keeping the UI authoritative afterwards.
 */
function seedExtConfig(
  bus: Bus,
  config: ServerConfig,
  tenants: readonly string[],
): void {
  for (const s of config.extConfigSeed) {
    // `tenant: "*"` fans the entry out to every known tenant.
    const targets = s.tenant === '*' ? tenants : [s.tenant]
    for (const tenant of targets) {
      void (async () => {
        try {
          const key = tenantKVKey(tenant, `${s.extId}.${s.name}`)
          const created = await bus.kvCreate(
            'cfg',
            key,
            JSON.stringify({ r: 1, v: s.value }),
            0,
          )
          if (created !== null) {
            logger.info(
              { tenant, extId: s.extId, name: s.name },
              'seeded extension config default',
            )
          }
        } catch (e) {
          logger.warn(
            { tenant, extId: s.extId, name: s.name, err: String(e) },
            'extension config seed failed',
          )
        }
      })()
    }
  }
}

/**
 * Seed deployment-pinned TENANT config into the `abcp-agent-config` KV bucket
 * (the same bucket/key `Config.set` uses — a RAW string value, unlike the
 * extension `cfg` bucket which stores a `{r,v}` envelope). Applied ONLY when the
 * key is absent (kvCreate fails on an existing key), so a value a user later set
 * via the UI is never clobbered. This is how a fresh deployment gets a working
 * `default_model` (the first turn otherwise errors with "no model selected").
 */
function seedTenantConfig(
  bus: Bus,
  config: ServerConfig,
  tenants: readonly string[],
): void {
  for (const s of config.configSeed) {
    // `tenant: "*"` fans the entry out to every known tenant.
    const targets = s.tenant === '*' ? tenants : [s.tenant]
    for (const tenant of targets) {
      void (async () => {
        try {
          const key = tenantKVKey(tenant, s.key)
          const created = await bus.kvCreate(BUCKET_CONFIG, key, s.value, 0)
          if (created !== null) {
            logger.info({ tenant, key: s.key }, 'seeded tenant config default')
          }
        } catch (e) {
          logger.warn(
            { tenant, key: s.key, err: String(e) },
            'tenant config seed failed',
          )
        }
      })()
    }
  }
}

/**
 * Seed the configured providers into every tenant that has NONE yet. This is
 * the standalone analogue of the workspace gateway's provisioning: a fresh
 * deployment gets its model registry (the platform gateway providers) without
 * a UI round-trip. Idempotent and non-destructive: a tenant with any provider
 * already registered is left untouched, so a UI edit always wins.
 */
async function seedProviders(
  db: Db,
  config: ServerConfig,
  tenants: readonly string[],
): Promise<void> {
  if (config.providerSeed.length === 0) return
  for (const tenant of tenants) {
    const existing = await Providers.listTenants(db, tenant)
    if (existing.isErr()) {
      logger.warn({ tenant, err: existing.error }, 'provider seed: list failed')
      continue
    }
    if (existing.value.length > 0) continue
    for (const p of config.providerSeed) {
      if (p.tenant !== '*' && p.tenant !== tenant) continue
      const r = await Providers.upsert(db, tenant, {
        providerId: p.providerId,
        capability: p.capability,
        apiType: p.apiType,
        baseUrl: p.baseUrl,
        apiKey: p.apiKey,
        headers: null,
        models: p.models.map(m => ({
          id: m.id,
          name: m.name,
          context_limit: m.contextLimit,
          model_type: p.capability,
        })),
      })
      if (r.isErr()) {
        logger.warn(
          { tenant, providerId: p.providerId, err: r.error },
          'provider seed: upsert failed',
        )
      } else {
        logger.info(
          { tenant, providerId: p.providerId },
          'seeded provider default',
        )
      }
    }
  }
}

/**
 * First-boot bootstrap: when the tenant table is empty and both
 * AGENT_BOOTSTRAP_TENANT and AGENT_BOOTSTRAP_TOKEN are set, create that tenant
 * and store the given token (hashed). Idempotent: a non-empty table is a no-op,
 * and a lost race is swallowed.
 */
async function bootstrapTenantIfEmpty(
  db: Db,
  config: ServerConfig,
): Promise<void> {
  if (config.bootstrapTenant === '' || config.bootstrapToken === '') return
  const existing = await Tenants.list(db)
  if (existing.isErr()) {
    logger.warn({ err: existing.error }, 'bootstrap: list tenants failed')
    return
  }
  if (existing.value.length > 0) return
  const created = await Tenants.create(db, config.bootstrapTenant, 'bootstrap')
  if (created.isErr()) {
    logger.warn({ err: created.error }, 'bootstrap: create tenant failed')
    return
  }
  const issued = await Tenants.issueToken(
    db,
    config.bootstrapTenant,
    'bootstrap',
    config.bootstrapToken,
  )
  if (issued.isErr()) {
    logger.warn({ err: issued.error }, 'bootstrap: issue token failed')
    return
  }
  logger.info(
    { tenant: config.bootstrapTenant },
    'bootstrap: created initial tenant + token',
  )
}
