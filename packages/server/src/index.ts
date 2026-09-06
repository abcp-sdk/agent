import { serve } from '@hono/node-server'
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
  refreshModelsDev,
  runSessionTurn,
  watchMailboxWake,
  watchWorksheetReconciler,
} from '@zergx-agent/agent'
import { Hono } from 'hono'
import { HTTPException } from 'hono/http-exception'
import { app } from './app.js'
import type { AppEnv } from './context.js'

export type { AppType } from './app.js'

const MIME: Record<string, string> = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
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

/** Serve the SPA from embedded SEA assets (single-executable deployment). */
function seaStatic() {
  return async (c: import('hono').Context<AppEnv>) => {
    const path = new URL(c.req.url).pathname
    // Route non-asset paths to the SPA entry.
    const asset =
      path === '/' || !path.includes('.') ? 'index.html' : path.slice(1)
    const data = getSeaAsset(asset)
    if (data === null) {
      // Fall back to index.html for client-side routing.
      const index = getSeaAsset('index.html')
      if (index === null) return c.json({ ok: false, error: 'not found' }, 404)
      return new Response(index, {
        headers: { 'content-type': 'text/html' },
      })
    }
    const ext = asset.slice(asset.lastIndexOf('.'))
    return new Response(data, {
      headers: {
        'content-type': MIME[ext] ?? 'application/octet-stream',
      },
    })
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
    process.env.ZERGX_MAILBOX_RETENTION_DAYS ?? '7',
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

  // Build the full outer app: deps-injection + error handling + sea-static are
  // registered BEFORE mounting the routes, mirroring the E1 framework's order
  // (contextMiddleware → onError → route), so mounted handlers always see
  // `c.get('deps')`.
  const servingApp = new Hono<AppEnv>()

  servingApp.use('*', async (c, next) => {
    c.set('deps', deps)
    await next()
  })

  servingApp.onError((err, c) => {
    if (err instanceof HTTPException) {
      if (err.res) return c.newResponse(err.res.body, err.res)
      return c.json({ ok: false, error: err.message }, err.status)
    }
    logger.error({ err }, 'unhandled error')
    return c.json({ ok: false, error: 'Internal Server Error' }, 500)
  })

  servingApp.route('/api/v1', app)

  // Serve the SPA from embedded SEA assets (single-executable deployment).
  servingApp.use('*', seaStatic())

  // Watch every session's mailbox wake wildcard so this replica can claim and
  // run work for any session — the horizontal scale-out trigger.
  const stopWake = watchMailboxWake(deps)
  const stopWorksheetReconciler = watchWorksheetReconciler(deps)

  // Populate the models.dev catalog cache once at startup (30min TTL); a
  // failing fetch is non-fatal — the catalog is a prefill convenience.
  void refreshModelsDev(bus).then(
    () => {},
    e => logger.warn({ err: String(e) }, 'models.dev refresh failed'),
  )

  const server = serve({ fetch: servingApp.fetch, port: config.port }, info => {
    logger.info({ port: info.port, pid: process.pid }, 'listening')
  })

  const shutdown = () => {
    logger.info('shutting down')
    stopWake()
    server.close(() => {
      bus.close()
      const end = (
        db as { $client: { end?: () => Promise<unknown>; close?: () => void } }
      ).$client
      void Promise.resolve(
        typeof end.end === 'function' ? end.end() : end.close?.(),
      ).then(
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
