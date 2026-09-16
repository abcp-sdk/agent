import { Agent as AbcAgent, isSessionRunning } from '@abc-protocol/sdk'
import type { JsonObject, JsonValue } from '@bufbuild/protobuf'
import { create, fromJson, toJson } from '@bufbuild/protobuf'
import { StructSchema, type Value, ValueSchema } from '@bufbuild/protobuf/wkt'
import {
  Code,
  ConnectError,
  type ConnectRouter,
  type HandlerContext,
  type ServiceImpl,
} from '@connectrpc/connect'
import {
  type AgentDeps,
  appendSessionId,
  BUCKET_SESSION_STATE,
  type ChainMessage,
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  Config,
  catalogModel,
  clearActiveRun,
  compactSession,
  DEFAULT_PRESET,
  deleteSessionIds,
  discoverGatewayModels,
  discoverTools,
  factFromPersist,
  fileByCode,
  findVariant,
  fireAndForget,
  GATEWAY_API_TYPE,
  GATEWAY_PROVIDER_ID,
  getModelsDev,
  interruptRun,
  isGatewayApiType,
  localizeSchema,
  Mailbox,
  Messages,
  mailboxSubject,
  maskSecret,
  Parts,
  Presets,
  Providers,
  parse,
  parseCapability,
  parseProviderModelRef,
  pickDescription,
  pickLocalized,
  projectMessageFact,
  publishLifecycle,
  publishSessionChanged,
  pushChainChanged,
  readActiveRun,
  readMessageFacts,
  renderTemplate,
  resolveLocale,
  Sessions,
  TextPartDataSchema,
  toModelVariant,
  toolConfigMap,
  validateApiType,
  variantsForApiType,
  writeMessageFact,
} from '@easylab-agent/agent'
import {
  type AgentService,
  GetAgentConfigResponseSchema,
  GetFileResponseSchema,
  IngestFileResponseSchema,
  ListToolsResponseSchema,
  type WatchSessionResponse,
  WatchSessionResponseSchema,
  type WatchSessionsResponse,
  WatchSessionsResponseSchema,
} from '@easylab-agent/schema'
import { EidDedup } from '../context.js'
import {
  fieldString,
  isRecord,
  toJsonObject,
  toJsonValue,
  toStructFields,
  toStructValue,
  toValue,
  valueToRaw,
} from '../proto-json.js'
import { runProviderTest } from '../provider-test.js'
import { resolveSessionDefaults } from '../session-defaults.js'
import { tenantOf } from '../tenant.js'
import {
  parseProviderModels,
  presetToMsg,
  providerToMsg,
  sessionToMsg,
} from '../views.js'
import { refreshMessageFactFromTip, storeBytes } from './helpers.js'

/**
 * Server-streaming watch handlers: watchSession and watchSessions.
 */

export function watchHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async *watchSession(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { id, since } = req
      const agent = new AbcAgent(deps.bus)
      // Single ordered subscription: retained history from the live turn's
      // start (or live-from-now when idle), then live events — no separate
      // replay + subscribe handover, no polling.
      const activeRun = await readActiveRun(deps.bus, tenant, id)
      // Incremental replay anchor: `since` is a message id the client already
      // has. Replay from that message's timestamp so turns that completed
      // while the client was offline are delivered. If the anchor is missing
      // (withdrawn) fall back to live-from-now — the client's ListMessages
      // resync handles the chain.
      let startTimeMs: number | undefined =
        activeRun !== null ? activeRun.startedAtMs : undefined
      if (since !== undefined && since !== '') {
        const anchor = await Messages.get(deps.db, tenant, since)
        if (anchor.isOk() && anchor.value !== null) {
          const t = Date.parse(anchor.value.created_at ?? '')
          if (!Number.isNaN(t)) {
            startTimeMs =
              startTimeMs === undefined ? t : Math.min(startTimeMs, t)
          }
        }
      }
      const dedup = new EidDedup()
      for await (const raw of agent.streamEvents(
        tenant,
        id,
        startTimeMs !== undefined ? { startTimeMs } : undefined,
      )) {
        // Only the live run's events (a prior turn's terminal marker may fall
        // inside the same time window); never resurface finished/revoked runs.
        // EXEMPT chain-changed: it is an out-of-band chain notification with
        // no run_id, and MUST reach viewers even while a turn is running.
        const isChainChanged = raw?.event === 'chain-changed'
        if (
          !isChainChanged &&
          activeRun !== null &&
          fieldString(raw?.params, 'run_id') !== activeRun.runId
        ) {
          continue
        }
        const eid = fieldString(raw, 'eid')
        if (dedup.duplicate(eid)) continue
        yield toWatchEvent(raw)
      }
    },

    /**
     * Real-time session-list stream. Emits an initial full snapshot, then a
     * stream of per-session upserts and removals. Driven by three sources:
     * the message-fact KV (`abc-session-meta`) for previews/seq, the
     * lifecycle subject for created/forked/renamed/deleted, and the
     * `abc.session.changed` pub for settings edits. Watchers are best-effort
     * nudged and refetch the affected session from the DB + KV, so a missed
     * nudge only delays that session's row until the next event.
     */
    async *watchSessions(_req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      // Build one Session snapshot (facts + row) for a name; null if gone.
      const snapshotOf = async (name: string) => {
        const r = await Sessions.get(deps.db, tenant, name)
        if (r.isErr() || r.value === null) return null
        const facts = await readMessageFacts(deps.bus, tenant, [name])
        return sessionToMsg(r.value, facts.get(name))
      }

      // A queue serializes the three independent watchers into one stream.
      const queue: WatchSessionsResponse[] = []
      let wake: (() => void) | null = null
      const push = (msg: WatchSessionsResponse) => {
        queue.push(msg)
        wake?.()
        wake = null
      }
      const pushUpsert = async (name: string) => {
        const s = await snapshotOf(name)
        if (s !== null) {
          push(create(WatchSessionsResponseSchema, { upserts: [s] }))
        }
      }

      // Start all three watchers BEFORE emitting the snapshot so no change
      // that lands during the (async) snapshot query is lost; anything they
      // observe is queued and flushed after the snapshot.
      // 1) fact changes (message landed / preview advanced).
      const factWatch = await deps.bus
        .kvWatch(BUCKET_SESSION_STATE, `t.${tenant}.>`)
        .catch(() => null)
      const factTask = (async () => {
        if (factWatch === null) return
        for await (const ev of factWatch.stream) {
          if (ev.deleted) continue
          let sid = ''
          try {
            sid = String(
              (JSON.parse(ev.value) as { session_name?: string })
                .session_name ?? '',
            )
          } catch {
            continue
          }
          if (sid !== '') await pushUpsert(sid)
        }
      })()

      // 2) structural lifecycle changes.
      const lcSub = await deps.bus
        .subscribe(`abc.${tenant}.session.lifecycle.>`)
        .catch(() => null)
      const lcTask = (async () => {
        if (lcSub === null) return
        for await (const env of lcSub) {
          const kind = String((env.payload as { kind?: string })?.kind ?? '')
          const p = env.payload as {
            session_name?: string
            from?: string
            to?: string
          }
          if (kind === 'deleted' && p.session_name) {
            push(
              create(WatchSessionsResponseSchema, {
                removed: [p.session_name],
              }),
            )
          } else if (kind === 'renamed' && p.from && p.to) {
            push(create(WatchSessionsResponseSchema, { removed: [p.from] }))
            await pushUpsert(p.to)
          } else if (p.session_name) {
            await pushUpsert(p.session_name)
          }
        }
      })()

      // 3) settings-change nudges (setModel / updateSettings).
      const chSub = await deps.bus
        .subscribe(`abc.${tenant}.session.changed`)
        .catch(() => null)
      const chTask = (async () => {
        if (chSub === null) return
        for await (const env of chSub) {
          const sid = String(
            (env.payload as { session_name?: string })?.session_name ?? '',
          )
          if (sid !== '') await pushUpsert(sid)
        }
      })()

      try {
        // Initial full snapshot (the client replaces its whole list).
        const all = await Sessions.list(deps.db, tenant)
        if (all.isErr()) throw new Error(all.error)
        const facts = await readMessageFacts(
          deps.bus,
          tenant,
          all.value.map(s => s.name),
        )
        yield create(WatchSessionsResponseSchema, {
          snapshot: true,
          removed: [],
          upserts: all.value.map(s => sessionToMsg(s, facts.get(s.name))),
        })
        for (;;) {
          while (queue.length > 0) {
            const msg = queue.shift()
            if (msg !== undefined) yield msg
          }
          await new Promise<void>(resolve => {
            wake = resolve
          })
        }
      } finally {
        await factWatch?.stop().catch(() => {})
        void factTask.catch(() => {})
        await lcSub?.close().catch(() => {})
        await chSub?.close().catch(() => {})
        void lcTask.catch(() => {})
        void chTask.catch(() => {})
      }
    },
  }
}

// toWatchEvent converts a bus envelope into a WatchSessionResponse message.
function toWatchEvent(raw: unknown): WatchSessionResponse {
  const env = raw as {
    event?: string
    params?: Record<string, unknown>
    eid?: string
  }
  return create(WatchSessionResponseSchema, {
    event: env.event ?? 'message',
    params: toJsonObject(isRecord(env.params) ? env.params : {}),
    eid: env.eid ?? '',
  })
}

// ---- Struct helpers (google.protobuf.Value wrapping) ----
