import { Agent as AbcAgent, isSessionRunning } from '@abc-protocol/sdk'
import {
  appendSessionId,
  BUCKET_SESSION_STATE,
  catalogModel,
  GATEWAY_API_TYPE,
  GATEWAY_PROVIDER_ID,
  isGatewayApiType,
  parseCapability,
  validateApiType,
  discoverGatewayModels,
  Config,
  compactSession,
  deleteSessionIds,
  discoverTools,
  fileByCode,
  findVariant,
  fireAndForget,
  getModelsDev,
  clearActiveRun,
  interruptRun,
  localizeSchema,
  Mailbox,
  mailboxSubject,
  Messages,
  parseProviderModelRef,
  Parts,
  pickDescription,
  pickLocalized,
  Presets,
  Providers,
  publishLifecycle,
  publishSessionChanged,
  factFromPersist,
  projectMessageFact,
  writeMessageFact,
  parse,
  TextPartDataSchema,
  readActiveRun,
  readMessageFacts,
  renderTemplate,
  resolveLocale,
  Sessions,
  toModelVariant,
  toolConfigMap,
  variantsForApiType,
  DEFAULT_PRESET,
  type ChainMessage,
} from '@easylab-agent/agent'
import { type AgentDeps } from '@easylab-agent/agent'

import { EidDedup } from './context.js'
import { tenantOf } from './tenant.js'
import { runProviderTest } from './provider-test.js'
import {
  type ConnectRouter,
  type HandlerContext,
  type ServiceImpl,
  ConnectError,
  Code,
} from '@connectrpc/connect'
import { create, fromJson, toJson } from '@bufbuild/protobuf'
import type { JsonObject, JsonValue } from '@bufbuild/protobuf'
import { StructSchema, ValueSchema, type Value } from '@bufbuild/protobuf/wkt'
import {
  AgentService,
  ListToolsResponseSchema,
  GetAgentConfigResponseSchema,
  GetFileResponseSchema,
  IngestFileResponseSchema,
  WatchSessionResponseSchema,
  WatchSessionsResponseSchema,
  type WatchSessionResponse,
  type WatchSessionsResponse,
} from '@easylab-agent/schema'

/**
 * Structural row views for the Connect mappers. Call sites pass concrete DB
 * rows (which carry at least these fields), so the mappers stay strongly
 * typed without importing row classes or casting.
 */
interface SessionRowView {
  name: string
  model?: string | null | undefined
  variant?: string | null | undefined
  preset?: string | null | undefined
  tip_id?: string | null | undefined
  max_turns?: number | null | undefined
  system_prompt?: string | null | undefined
  input_tokens?: number | null | undefined
  output_tokens?: number | null | undefined
  total_tokens?: number | null | undefined
  last_input_tokens?: number | null | undefined
  last_output_tokens?: number | null | undefined
  created_at?: string | null | undefined
  updated_at?: string | null | undefined
  last_used_at?: string | null | undefined
  locale?: string | null | undefined
  org?: string | null | undefined
  repo?: string | null | undefined
  branch?: string | null | undefined
  unread_count?: number | null | undefined
  last_message_at?: string | null | undefined
  last_message_preview?: string | null | undefined
}

interface ProviderRowView {
  provider_id: string
  api_type?: string | null | undefined
  base_url?: string | null | undefined
  api_key?: string | null | undefined
  headers?: string | null | undefined
  models?: string | null | undefined
  updated_at?: string | null | undefined
}

interface PresetRowView {
  id: string
  system_prompt?: string | null | undefined
  system_prompt_i18n?: string | null | undefined
  tools?: string | null | undefined
  max_turns?: number | null | undefined
  is_system?: boolean | null | undefined
}

/** Runtime type guard: a plain string-keyed object (not an array). */
function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null
}

/** Read an optional string field off an opaque object (no casts). */
function fieldString(o: unknown, key: string): string | undefined {
  if (!isRecord(o)) return undefined
  const v: unknown = o[key]
  return typeof v === 'string' ? v : undefined
}

/** Wrap a raw JSON value into a google.protobuf.Value message. */
function toValue(v: unknown): Value {
  return fromJson(ValueSchema, v as JsonValue)
}

/** Unwrap a google.protobuf.Value message into a raw JSON value. */
function valueToRaw(value: Value | undefined | null): unknown {
  if (value === null || value === undefined) return null
  return toJson(ValueSchema, value)
}

/** Convert an unknown JSON-ish value into a typed JsonValue (no casts). */
function toJsonValue(v: unknown): JsonValue {
  if (
    v === null ||
    typeof v === 'string' ||
    typeof v === 'number' ||
    typeof v === 'boolean'
  ) {
    return v
  }
  if (Array.isArray(v)) return v.map(toJsonValue)
  if (isRecord(v)) return toJsonObject(v)
  return String(v)
}

/** Convert a record of unknowns into a typed JsonObject field by field. */
function toJsonObject(v: Record<string, unknown>): JsonObject {
  const out: JsonObject = {}
  for (const [key, value] of Object.entries(v)) {
    out[key] = toJsonValue(value)
  }
  return out
}

/**
 * Map a session row to the wire shape. `fact` is the newest-message fact read
 * from the `abc-session-meta` KV (preview + timestamp); it is the ONLY source
 * for `lastMessageAt`/`lastMessagePreview` (the sessions table does not store
 * them). `unreadCount` is always 0 — read state is client-local.
 */
function sessionToMsg(
  s: SessionRowView,
  fact?: {
    last_message_at: string
    last_message_preview: string
    message_seq: number
  },
) {
  return {
    name: s.name,
    model: s.model ?? '',
    variant: s.variant ?? '',
    preset: s.preset ?? '',
    tipId: s.tip_id ?? '',
    maxTurns: s.max_turns ?? 0,
    systemPrompt: s.system_prompt ?? '',
    inputTokens: s.input_tokens ?? 0,
    outputTokens: s.output_tokens ?? 0,
    totalTokens: s.total_tokens ?? 0,
    lastInputTokens: s.last_input_tokens ?? 0,
    lastOutputTokens: s.last_output_tokens ?? 0,
    createdAt: s.created_at ?? '',
    updatedAt: s.updated_at ?? '',
    lastUsedAt: s.last_used_at ?? '',
    locale: s.locale ?? '',
    org: s.org ?? '',
    repo: s.repo ?? '',
    branch: s.branch ?? '',
    unreadCount: 0,
    lastMessageAt: fact?.last_message_at ?? '',
    lastMessagePreview: fact?.last_message_preview ?? '',
    messageSeq: fact?.message_seq ?? 0,
  }
}

/**
 * Parse the stored provider `models` JSON (an array of
 * `{ id, name?, context_limit, model_type? }`) into the proto ProviderModel
 * shape. A bare string entry is also tolerated (name=id, context_limit=0) so a
 * malformed row never breaks listing. `model_type` is display-only kind
 * metadata from the gateway `/config` (empty when unknown / a text provider).
 */
function parseProviderModels(raw: string | null | undefined): {
  id: string
  name: string
  contextLimit: bigint
  modelType: string
}[] {
  let arr: unknown = []
  try {
    arr = JSON.parse(raw ?? '[]') ?? []
  } catch {
    return []
  }
  if (!Array.isArray(arr)) return []
  const out: { id: string; name: string; contextLimit: bigint; modelType: string }[] =
    []
  for (const item of arr) {
    if (typeof item === 'string') {
      if (item !== '')
        out.push({ id: item, name: item, contextLimit: 0n, modelType: '' })
      continue
    }
    if (item === null || typeof item !== 'object') continue
    const v = item as Record<string, unknown>
    const id = String(v['id'] ?? '')
    if (id === '') continue
    out.push({
      id,
      name: String(v['name'] ?? id),
      contextLimit: BigInt(Math.trunc(Number(v['context_limit'] ?? 0)) || 0),
      modelType: String(v['model_type'] ?? ''),
    })
  }
  return out
}

function providerToMsg(p: ProviderRowView) {
  let headers: Record<string, string> = {}
  try {
    headers = JSON.parse(p.headers ?? '{}') ?? {}
  } catch {}
  return {
    providerId: p.provider_id ?? '',
    apiType: p.api_type ?? '',
    baseUrl: p.base_url ?? '',
    apiKey: p.api_key ?? '',
    headers,
    models: parseProviderModels(p.models),
    updatedAt: p.updated_at ?? '',
  }
}

function presetToMsg(p: PresetRowView, locale?: string) {
  let tools: string[] = []
  try {
    tools = JSON.parse(p.tools ?? '[]') ?? []
  } catch {}
  return {
    id: p.id ?? '',
    systemPrompt: presetPromptFor(p, locale ?? ''),
    tools,
    maxTurns: p.max_turns ?? 0,
    isSystem: p.is_system ?? false,
  }
}

/** Resolve a preset's system prompt for [locale]: parse `system_prompt_i18n`
 * as a { locale: template } map and pick the entry (exact → primary language
 * → fallback to the default `system_prompt`). */
function presetPromptFor(p: PresetRowView, locale: string): string {
  const i18n = p.system_prompt_i18n
  if (i18n !== undefined && i18n !== null && i18n !== '{}' && i18n !== '') {
    try {
      const map = JSON.parse(i18n) as Record<string, unknown>
      const picked = pickLocalized(map as Record<string, string>, locale)
      if (picked !== null) return picked
    } catch {
      /* fall through to the default prompt */
    }
  }
  return p.system_prompt ?? ''
}

/**
 * Build the Connect v2 AgentService routes for the fetch handler. The RPC
 * surface (procedure paths /agent.v1.AgentService/*, all three protocols)
 * is served by @connectrpc/connect via a Web Request=>Response fetch handler
 * mounted on Hono.
 */
export function buildConnectRoutes(
  deps: AgentDeps,
): (router: ConnectRouter) => void {
  return router => {
    const impl: ServiceImpl<typeof AgentService> = {
      async health(_req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        return { ok: true, name: 'easylab-agent' }
      },
      async listSessions(_req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const r = await Sessions.list(deps.db, tenant)
        if (r.isErr()) throw new Error(r.error)
        const facts = await readMessageFacts(
          deps.bus,
          tenant,
          r.value.map(s => s.name),
        )
        return { sessions: r.value.map(s => sessionToMsg(s, facts.get(s.name))) }
      },
      async createSession(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const body = req
        const exists = await Sessions.exists(deps.db, tenant, body.name)
        if (exists.isErr()) throw new Error(exists.error)
        if (exists.value) throw new Error('Session already exists')
        const name = await Sessions.create(deps.db, tenant, {
          name: body.name,
          model: body.model,
          variant: body.variant,
          preset: body.preset,
        })
        if (name.isErr()) throw new Error(name.error)
        publishLifecycle(deps.bus, tenant, 'created', { session_name: body.name })
        return { ok: true, sessionName: name.value }
      },
      async getSession(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const r = await Sessions.get(deps.db, tenant, req.id)
        if (r.isErr()) throw new Error(r.error)
        if (r.value === null) throw new Error('session not found')
        const facts = await readMessageFacts(deps.bus, tenant, [req.id])
        return { session: sessionToMsg(r.value, facts.get(req.id)) }
      },
      async deleteSession(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        interruptRun(id)
        const r = await Sessions.delete(deps.db, tenant, id)
        if (r.isErr()) throw new Error(r.error)
        publishLifecycle(deps.bus, tenant, 'deleted', { session_name: id })
        return { ok: true }
      },
      async listMessages(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, limit: l, before, after } = req
        const limit = l && l > 0 ? l : 50
        const tipRes = await Sessions.tip(deps.db, tenant, id)
        const tipId = tipRes.isErr() ? null : tipRes.value
        const tip = tipId ?? ''
        // proto3 string defaults to "" (not null): treat empty as "no cursor"
        // so the chain walks from the tip instead of looking up prev_id of "".
        const beforeId = before !== undefined && before !== '' ? before : null
        const afterId = after !== undefined && after !== '' ? after : null

        // Hydrate a list of chain messages into the proto shape.
        const toMsgs = async (chain: ChainMessage[]) => {
          const msgIds = chain.map(m => m.id)
          const partsRes = await Parts.listByMessages(deps.db, tenant, msgIds)
          const partsByMsg = new Map<
            string,
            Array<{
              id: string
              message_id: string
              type: string
              seq: number
              data: string
            }>
          >()
          if (partsRes.isOk()) {
            for (const p of partsRes.value) {
              const list = partsByMsg.get(p.message_id) ?? []
              list.push(p)
              partsByMsg.set(p.message_id, list)
            }
          }
          return chain.map(m => ({
            id: m.id,
            role: m.role,
            prevId: m.prev_id ?? '',
            createdAt: m.created_at ?? '',
            parts: (partsByMsg.get(m.id) ?? []).map(p => ({
              id: p.id,
              messageId: p.message_id,
              type: p.type,
              seq: p.seq,
              data: p.data,
            })),
          }))
        }

        // Incremental mode: anchored on a client-known pin. The delta is the
        // segment appended since that anchor; if the anchor is gone (undo /
        // re-pointed chain) we return `resync` so the client drops its cache.
        if (afterId !== null) {
          const r = await Messages.deltaSince(deps.db, tenant, tipId, afterId, limit)
          if (r.isErr()) throw new Error(r.error)
          const resync = !r.value.anchorReached && r.value.reachedRoot
          let messages: Awaited<ReturnType<typeof toMsgs>> = []
          if (!resync) messages = await toMsgs(r.value.messages)
          return { ok: true, messages, resync, tipId: tip }
        }

        const r = await Messages.chain(deps.db, tenant, tipId, limit, beforeId)
        if (r.isErr()) throw new Error(r.error)
        const messages = await toMsgs(r.value)
        return { ok: true, messages, resync: false, tipId: tip }
      },
      async *prompt(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, prompt, attachments } = req
        const session = await Sessions.get(deps.db, tenant, id)
        if (session.isErr()) throw new Error(session.error)
        if (session.value === null) throw new Error('session not found')

        const tipRes = await Sessions.tip(deps.db, tenant, id)
        const tipId = tipRes.isErr() ? null : tipRes.value
        const insert = await Messages.insert(deps.db, tenant, 'user', tipId)
        if (insert.isErr()) throw new Error(insert.error)
        let seq = 0
        for (const att of attachments ?? []) {
          if (att.code === '') {
            throw new ConnectError(
              'attachment code is required',
              Code.InvalidArgument,
            )
          }
          // The client sends ONLY the code; name/mime/size are resolved from
          // the stored blob. proto3 gives ''/0 (not undefined) for omitted
          // fields, so treat empty as missing. There is NO silent fallback: a
          // file part must carry a real name, mime and size, otherwise the
          // prompt is refused (a blank metadata file part can't be rendered).
          let name = att.name
          let mime = att.mime
          let size = att.size
          const missing =
            (name ?? '') === '' || (mime ?? '') === '' || (size ?? 0) <= 0
          if (missing) {
            const rec = await fileByCode(deps.bus, tenant, att.code)
            if (rec.isErr()) throw new Error(rec.error)
            if (rec.value === null) {
              throw new ConnectError(
                `attachment not found: file:${att.code}`,
                Code.InvalidArgument,
              )
            }
            name = rec.value.name
            mime = rec.value.mime
            size = rec.value.size
          }
          if ((name ?? '') === '' || (mime ?? '') === '' || (size ?? 0) <= 0) {
            throw new ConnectError(
              `attachment file:${att.code} has incomplete metadata ` +
                `(name=${JSON.stringify(name)} mime=${JSON.stringify(mime)} size=${size})`,
              Code.FailedPrecondition,
            )
          }
          await Parts.insert(deps.db, tenant, insert.value, 'file', seq++, {
            code: att.code,
            name,
            mime,
            size,
          })
        }
        if (prompt !== '') {
          await Parts.insert(deps.db, tenant, insert.value, 'text', seq++, {
            text: prompt,
          })
        }
        await Sessions.setTip(deps.db, tenant, id, insert.value)
        fireAndForget(
          appendSessionId(deps.bus, tenant, id, insert.value),
          'appendSessionIds',
        )
        // Mirror the newest-message fact immediately so the chat-list preview
        // shows the user's message right away (assistant steps overwrite it as
        // the turn progresses).
        const previewText =
          prompt !== ''
            ? prompt
            : attachments.length > 0
              ? `[${attachments.length} attachment(s)]`
              : ''
        projectMessageFact(
          deps.bus,
          tenant,
          id,
          factFromPersist(new Date().toISOString(), 'user', previewText),
        )
        await new AbcAgent(deps.bus).publishMailbox(tenant, id, 'user_prompt', {
          text: prompt,
          attachments: attachments ?? [],
        })
        yield {
          event: 'accepted',
          params: { message_id: insert.value },
          eid: '',
        }
      },
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
          if (
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

      async fork(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, name, messageId, preset } = req
        const parent = await Sessions.get(deps.db, tenant, id)
        if (parent.isErr()) throw new Error(parent.error)
        if (parent.value === null) throw new Error('session not found')
        const p = parent.value
        const exists = await Sessions.exists(deps.db, tenant, name)
        if (exists.isErr()) throw new Error(exists.error)
        if (exists.value) throw new Error('Session already exists')
        let forkTip: string | null = p.tip_id
        if (messageId !== undefined && messageId !== '') {
          const target = await Messages.get(deps.db, tenant, messageId)
          if (target.isErr()) throw new Error(target.error)
          if (target.value === null) throw new Error('fork message not found')
          if (p.tip_id !== null && p.tip_id !== '') {
            const inChain = await Messages.isInChain(
              deps.db,
              tenant,
              p.tip_id,
              messageId,
            )
            if (inChain.isErr()) throw new Error(inChain.error)
            if (!inChain.value)
              throw new Error('fork message not in this session chain')
          }
          forkTip = messageId
        }
        const created = await Sessions.create(deps.db, tenant, {
          name,
          model: p.model,
          preset: preset ?? (p.preset !== '' ? p.preset : DEFAULT_PRESET),
          systemPrompt: p.system_prompt,
          maxTurns: p.max_turns,
          locale: p.locale,
          tipId: forkTip,
        })
        if (created.isErr()) throw new Error(created.error)
        publishLifecycle(deps.bus, tenant, 'forked', {
          session_name: name,
          parent: id,
        })
        return { session: sessionToMsg({ ...p, name }) }
      },
      async rename(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, name } = req
        if (name === id) return { session: sessionToMsg({ name }) }
        const parent = await Sessions.get(deps.db, tenant, id)
        if (parent.isErr()) throw new Error(parent.error)
        if (parent.value === null) throw new Error('session not found')
        const exists = await Sessions.exists(deps.db, tenant, name)
        if (exists.isErr()) throw new Error(exists.error)
        if (exists.value) throw new Error('Session already exists')
        const p = parent.value
        const created = await Sessions.create(deps.db, tenant, {
          name,
          model: p.model,
          preset: p.preset !== '' ? p.preset : DEFAULT_PRESET,
          systemPrompt: p.system_prompt,
          maxTurns: p.max_turns,
          locale: p.locale,
          tipId: p.tip_id,
        })
        if (created.isErr()) throw new Error(created.error)
        const removed = await Sessions.delete(deps.db, tenant, id)
        if (removed.isErr()) {
          void Sessions.delete(deps.db, tenant, name)
          throw new Error(removed.error)
        }
        publishLifecycle(deps.bus, tenant, 'renamed', { from: id, to: name })
        return { session: sessionToMsg({ ...p, name }) }
      },
      async setModel(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, model, variant } = req
        const r = await Sessions.setModel(deps.db, tenant, id, model, variant)
        if (r.isErr()) throw new Error(r.error)
        publishSessionChanged(deps.bus, tenant, id)
        const s = await Sessions.get(deps.db, tenant, id)
        return {
          session:
            s.isOk() && s.value
              ? sessionToMsg(s.value)
              : sessionToMsg({ name: id }),
        }
      },
      async undo(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, messageId } = req
        const session = await Sessions.get(deps.db, tenant, id)
        if (session.isErr()) throw new Error(session.error)
        const s = session.value
        if (s === null) throw new Error('session not found')
        const tip = s.tip_id
        if (tip === null || tip === '')
          return { session: s ? sessionToMsg(s) : sessionToMsg({ name: id }) }
        const targetId = messageId !== '' ? messageId : tip
        const target = await Messages.get(deps.db, tenant, targetId)
        if (target.isErr()) throw new Error(target.error)
        if (target.value === null) return { session: sessionToMsg(s) }
        const inChain = await Messages.isInChain(deps.db, tenant, tip, targetId)
        if (inChain.isErr()) throw new Error(inChain.error)
        if (!inChain.value) return { session: sessionToMsg(s) }
        // Abort any in-flight turn and invalidate its active-run marker BEFORE
        // moving the tip: a running turn must not keep emitting deltas (or a
        // late turn-complete) for content we are withdrawing.
        interruptRun(id)
        clearActiveRun(deps.bus, tenant, id)
        await Sessions.setTip(deps.db, tenant, id, target.value.prev_id)
        // AWAIT the context-cache invalidation: it must be complete before this
        // RPC returns, otherwise a prompt issued right after (retry/edit →
        // withdraw + resend) could re-read the STALE id list and feed the
        // withdrawn messages back to the model.
        await deleteSessionIds(deps.bus, tenant, id)
        // Repair the session-list projection immediately so the preview/time
        // reflect the new tip (not the withdrawn message). Also awaited for the
        // same ordering guarantee. `message_seq` is preserved (a withdraw is not
        // a new message).
        await refreshMessageFactFromTip(deps, tenant, id)
        return { session: sessionToMsg(s) }
      },
      async state(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        const running = await isSessionRunning(deps.bus, tenant, id)
        return { state: { status: running ? 'busy' : 'idle' } }
      },
      async mailbox(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        const r = await Mailbox.list(deps.db, tenant, id)
        if (r.isErr()) throw new Error(r.error)
        return {
          ok: true,
          mailbox: r.value.map(m => ({
            id: m.id,
            sessionName: m.session_name,
            msgType: m.msg_type,
            payload: m.payload,
            effectiveAt: m.effective_at ?? '',
            status: m.status,
            createdAt: m.created_at,
            consumedAt: m.consumed_at ?? '',
            seq: BigInt(m.seq ?? 0),
          })),
        }
      },
      async updateSettings(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { id, ...patch } = req
        // max_turns is optional: omitted = inherit (preset/default); an
        // explicit value must be > 0.
        if (patch.maxTurns !== undefined && patch.maxTurns <= 0) {
          throw new ConnectError(
            'max_turns must be > 0 (omit to inherit)',
            Code.InvalidArgument,
          )
        }
        const r = await Sessions.updateSettings(deps.db, tenant, id, patch)
        if (r.isErr()) throw new Error(r.error)
        publishSessionChanged(deps.bus, tenant, id)
        const s = await Sessions.get(deps.db, tenant, id)
        return {
          session:
            s.isOk() && s.value
              ? sessionToMsg(s.value)
              : sessionToMsg({ name: id }),
        }
      },
      async interrupt(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        interruptRun(id)
        void deps.bus
          .publish(mailboxSubject(tenant, id), { type: 'interrupt', session_name: id })
          .catch(() => undefined)
        return { ok: true, interrupted: true }
      },
      async compact(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        const r = await compactSession(deps, tenant, id)
        if (r.isErr()) throw new Error(r.error)
        return { ok: r.value }
      },
      async listProviders(_req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const r = await Providers.list(deps.db, tenant)
        if (r.isErr()) throw new Error(r.error)
        return { providers: r.value.map(providerToMsg) }
      },
      async listProvidersCatalog(_req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        return { providers: {} }
      },
      async registerProvider(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const p = req.provider
        if (!p) throw new Error('provider required')
        const apiType = p.apiType
        const gateway = isGatewayApiType(apiType)
        const validType = validateApiType(apiType)
        if (validType.isErr()) {
          throw new ConnectError(validType.error, Code.InvalidArgument)
        }
        // The gateway is a SUPERSET (text + multimodal) and is registered AT
        // MOST ONCE. Text providers carry only text models. A model's
        // capability is implied by the tool config knob that references it,
        // not stored here.
        if (gateway) {
          // The gateway is a SINGLETON with the fixed id `gateway`, so every
          // multimodal model ref is uniformly `gateway/<model-id>`.
          if (p.providerId !== GATEWAY_PROVIDER_ID) {
            throw new ConnectError(
              `the '${GATEWAY_API_TYPE}' provider id must be '${GATEWAY_PROVIDER_ID}'`,
              Code.InvalidArgument,
            )
          }
          const existing = await Providers.list(deps.db, tenant)
          if (existing.isErr()) throw new Error(existing.error)
          const other = existing.value.find(
            r =>
              isGatewayApiType(r.api_type) &&
              r.provider_id !== p.providerId,
          )
          if (other !== undefined) {
            throw new ConnectError(
              `a '${GATEWAY_API_TYPE}' gateway is already registered ('${other.provider_id}')`,
              Code.AlreadyExists,
            )
          }
        }
        const models: {
          id: string
          name: string
          context_limit: number
          model_type: string
        }[] = []
        for (const m of p.models ?? []) {
          if (m.id === '') {
            throw new ConnectError('model id is required', Code.InvalidArgument)
          }
          // A text provider's models are all text ⇒ context_limit required. The
          // gateway is a superset: context_limit > 0 marks its text models,
          // 0 marks its multimodal models (used by tools).
          if (!gateway && m.contextLimit <= 0n) {
            throw new ConnectError(
              `model '${m.id}': context_limit is required and must be > 0`,
              Code.InvalidArgument,
            )
          }
          models.push({
            id: m.id,
            name: m.name !== '' ? m.name : m.id,
            context_limit: Number(m.contextLimit),
            // Display-only kind (text|image|video|speech|transcription|…);
            // empty for a plain text provider.
            model_type: m.modelType ?? '',
          })
        }
        const r = await Providers.upsert(deps.db, tenant, {
          providerId: p.providerId,
          apiType,
          baseUrl: p.baseUrl,
          apiKey: p.apiKey ?? '',
          headers: p.headers ?? {},
          models,
        })
        if (r.isErr()) throw new Error(r.error)
        return { ok: true }
      },
      async discoverGatewayModels(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        // Ask the gateway's /config for its models and classify by modelType.
        // The gateway is the only multimodal provider, so a non-gateway type
        // is rejected here.
        const creds = {
          providerId: req.providerId !== '' ? req.providerId : GATEWAY_PROVIDER_ID,
          apiType: req.apiType,
          baseUrl: req.baseUrl,
          apiKey: req.apiKey ?? '',
          headers: req.headers ?? {},
        }
        const r = await discoverGatewayModels(creds)
        if (r.isErr()) return { ok: false, error: r.error, models: [] }
        return {
          ok: true,
          error: '',
          models: r.value.map(m => ({
            id: m.id,
            name: m.name,
            contextLimit: BigInt(m.contextLimit),
            modelType: m.modelType,
          })),
        }
      },
      async deleteProvider(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.providerId
        const r = await Providers.delete(deps.db, tenant, id)
        if (r.isErr()) throw new Error(r.error)
        return { ok: true }
      },
      async testProvider(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const r = req
        // ONLY a real (smallest-possible) generation proves a model is usable.
        // No /models fallback. An empty model is an error (nothing to test).
        if (r.model === undefined || r.model === '') {
          return { ok: false, result: 'model is required to test' }
        }
        const cap = parseCapability(r.capability)
        if (cap.isErr()) {
          return { ok: false, result: cap.error }
        }
        // `model` accepts a canonical provider/model ref or a bare id; use the
        // trailing model id for the model factory.
        const ref = parseProviderModelRef(r.model)
        const modelId = ref !== null ? ref.modelId : r.model
        const providerId = r.providerId !== '' ? r.providerId : (ref?.providerId ?? '')
        // Text models may carry a reasoning variant; resolve it to the
        // providerOptions the test generation should exercise.
        let textProviderOptions: Record<string, unknown> | undefined
        if (cap.value === 'text') {
          const meta = catalogModel(
            await getModelsDev(deps.bus),
            providerId,
            modelId,
          )
          const variantDef =
            meta === null ? null : findVariant(meta, r.apiType, r.variant ?? '')
          if (
            variantDef !== null &&
            Object.keys(variantDef.providerOptions).length > 0
          ) {
            textProviderOptions = variantDef.providerOptions
          }
        }
        try {
          return await runProviderTest(
            {
              apiType: r.apiType,
              baseUrl: r.baseUrl,
              apiKey: r.apiKey ?? '',
              modelId,
              capability: cap.value,
              providerId,
              variant: r.variant ?? '',
            },
            textProviderOptions,
          )
        } catch (e) {
          return { ok: false, result: `provider test failed: ${String(e)}` }
        }
      },
      async listModels(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const pid = req.providerId
        // provider_id is REQUIRED: a global (all-providers) model list is
        // rejected outright — it invites duplicate model ids across providers.
        if (pid === '') {
          throw new ConnectError(
            'provider_id is required',
            Code.InvalidArgument,
          )
        }
        const r = await Providers.list(deps.db, tenant)
        if (r.isErr()) throw new Error(r.error)
        const catalog = await getModelsDev(deps.bus)
        const provider = r.value.find(p => p?.provider_id === pid)
        const apiType = provider?.api_type ?? ''
        const parsed = provider === undefined ? [] : parseProviderModels(provider.models)
        // Session model listing surfaces TEXT models only. For a text provider
        // every model is text; for the gateway (superset) only models with a
        // positive context_limit are text (context_limit 0 = multimodal).
        const models = parsed
          .filter(m => m.contextLimit > 0n)
          .map(m => {
            const meta = catalogModel(catalog, pid, m.id)
            const variants =
              meta === null
                ? []
                : variantsForApiType(meta, apiType).map(toModelVariant)
            return {
              id: m.id,
              name: m.name,
              variants,
              contextLimit: m.contextLimit,
            }
          })
        return { models }
      },
      async listPresets(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const r = await Presets.list(deps.bus, tenant)
        if (r.isErr()) throw new Error(r.error)
        // Locale chain: request → config KV → "en".
        const configLocale = (await Config.get(deps.bus, tenant, 'locale')).unwrapOr(
          null,
        )
        const locale = resolveLocale(
          req.locale,
          configLocale,
          'en',
        )
        return { presets: r.value.map(p => presetToMsg(p, locale)) }
      },
      async upsertPreset(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const p = req.preset
        if (!p) throw new Error('preset required')
        const r = await Presets.upsert(deps.bus, tenant, {
          id: p.id,
          systemPrompt: p.systemPrompt ?? '',
          systemPromptI18n: p.systemPromptI18n ?? '{}',
          tools: JSON.stringify(p.tools ?? []),
          maxTurns: p.maxTurns ?? 0,
        })
        if (r.isErr()) throw new Error(r.error)
        return { ok: true }
      },
      async deletePreset(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        const r = await Presets.delete(deps.bus, tenant, id)
        if (r.isErr()) throw new Error(r.error)
        return { ok: true }
      },
      async previewPreset(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const id = req.id
        const r = await Presets.get(deps.bus, tenant, id)
        if (r.isErr()) throw new Error(r.error)
        if (r.value === null) throw new Error('preset not found')
        const i18n = r.value.system_prompt_i18n
        const template =
          i18n !== undefined && i18n !== '' && i18n !== '{}'
            ? i18n
            : r.value.system_prompt
        const rendered = await renderTemplate(template, deps.bus, tenant)
        return { template, rendered }
      },
      async getConfig(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const key = req.key
        const r = await Config.get(deps.bus, tenant, key)
        return { key, value: r.isOk() && r.value ? r.value : '' }
      },
      async setConfig(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { key, value } = req
        const r = await Config.set(deps.bus, tenant, key, value)
        if (r.isErr()) throw new Error(r.error)
        return { ok: true }
      },
      async listTools(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const tools = await discoverTools(deps.bus)
        const configLocale = (await Config.get(deps.bus, tenant, 'locale')).unwrapOr(
          null,
        )
        const locale = resolveLocale(
          req.locale,
          configLocale,
          'en',
        )
        return create(ListToolsResponseSchema, {
          tools: tools.map(t => ({
            name: t.name,
            description: pickDescription(t.description, t.descriptions, locale),
            category: t.extId,
            parameters: toJsonObject(
              localizeSchema(t.inputSchema, locale) ?? {},
            ),
            configFields: (t.extConfig ?? []).map(c => ({
              name: c.name,
              type: c.type,
              enumValues: c.enum_values ?? [],
              description: pickDescription(
                c.description ?? '',
                c.descriptions,
                locale,
              ),
              scope: c.scope ?? 'global',
            })),
            requiredConfig: t.requiredConfig ?? [],
          })),
        })
      },
      async getToolConfig(_req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const value = await toolConfigMap(deps.bus, tenant)
        // Response.config is a ToolConfig whose `values` is a
        // map<string, google.protobuf.Value>: each tool maps to a single
        // Value that is a Struct of its declared knobs. Wrap the whole per-tool
        // knob map into one Struct Value.
        const values: Record<string, Value> = {}
        for (const [toolName, cfg] of Object.entries(value)) {
          values[toolName] = toValue(cfg)
        }
        return { config: { values } }
      },
      async setToolConfig(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        // Request.config is a google.protobuf.Struct — already a plain JSON
        // object on the wire, so serialize it directly.
        const r = await Config.set(
          deps.bus,
          tenant,
          'tool_config',
          JSON.stringify(req.config ?? {}),
        )
        if (r.isErr()) throw new Error(r.error)
        return { ok: true }
      },
      async setExtensionConfig(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { extId, name, value } = req
        const agent = new AbcAgent(deps.bus)
        await agent.discover(500)
        // Request.value is a google.protobuf.Value message; unwrap it with the
        // canonical toJson() mapping into the raw value the config store needs.
        const v = valueToRaw(value)
        await agent.setConfig(tenant, extId, name, v)
        return { ok: true }
      },
      async uploadFile(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const file = req.file
        const data = req.data
        if (data === undefined) throw new Error('data required')
        // The SERVER mints the code (16-hex, deduped by sha): a client-supplied
        // code is ignored so every file code in the system is uniform. This
        // closes the only path that could mint non-canonical codes.
        const bytes = new Uint8Array(Buffer.from(data, 'base64'))
        const record = await storeBytes(
          deps,
          tenant,
          bytes,
          file?.name ?? 'artifact',
          file?.mime ?? 'application/octet-stream',
          '',
        )
        return { ok: true, code: record.code }
      },
      async ingestFile(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const { data: raw, name, mime } = req
        if (raw === undefined) throw new Error('data required')
        const bytes = new Uint8Array(raw)
        if (bytes.length === 0) {
          throw new ConnectError('file data is empty', Code.InvalidArgument)
        }
        // A stored blob MUST carry a usable name + a non-empty mime so history
        // can render it (image thumbnail / audio player). A concrete
        // `application/octet-stream` is allowed (unknown extension); an EMPTY
        // name/mime is not — reject the upload rather than store an
        // unrenderable record.
        const cleanName = (name ?? '').trim()
        const cleanMime = (mime ?? '').trim()
        if (cleanName === '') {
          throw new ConnectError('file name is required', Code.InvalidArgument)
        }
        if (cleanMime === '') {
          throw new ConnectError(
            'file mime is required',
            Code.InvalidArgument,
          )
        }
        const record = await storeBytes(
          deps,
          tenant,
          bytes,
          cleanName,
          cleanMime,
          '',
        )
        return create(IngestFileResponseSchema, { ok: true, code: record.code })
      },
      async getFile(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const code = req.code
        const row = await fileByCode(deps.bus, tenant, code)
        if (row.isErr()) throw new Error(row.error)
        if (row.value === null) throw new Error('file not found')
        const got = await deps.files.get(tenant, code)
        return create(GetFileResponseSchema, {
          data: new Uint8Array(got.data),
          name: row.value.name ?? '',
          mime: row.value.mime ?? 'application/octet-stream',
        })
      },
      async getFileMeta(req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const code = req.code
        const row = await fileByCode(deps.bus, tenant, code)
        if (row.isErr()) throw new Error(row.error)
        if (row.value === null) throw new Error('file not found')
        return {
          name: row.value.name ?? '',
          mime: row.value.mime ?? '',
          size: row.value.size ?? 0,
        }
      },
      async getAgentConfig(_req, ctx: HandlerContext) {
        const tenant = tenantOf(ctx)
        const r = await Providers.list(deps.db, tenant)
        if (r.isErr()) throw new Error(r.error)
        const providers: Record<string, string> = {}
        for (const p of r.value) {
          if (p) {
            providers[p.provider_id] = JSON.stringify(providerToMsg(p), (_k, v) =>
              typeof v === 'bigint' ? v.toString() : v,
            )
          }
        }
        return create(GetAgentConfigResponseSchema, {
          config: { providers },
        })
      },
    }

    router.service(AgentService, impl)
  }
}

/**
 * Rewrite the session-list message-fact (preview/time/role) from the CURRENT
 * tip, preserving `message_seq`. Called after an undo moves the tip backwards,
 * so the list reflects the withdrawn chain immediately. `last_message_*` are
 * cleared when the session has no tip left.
 */
async function refreshMessageFactFromTip(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<void> {
  const tipRes = await Sessions.tip(deps.db, tenant, sid)
  const tipId = tipRes.isErr() ? null : tipRes.value
  if (tipId === null || tipId === '') {
    await writeMessageFact(
      deps.bus,
      tenant,
      sid,
      factFromPersist('', '', ''),
    )
    return
  }
  const target = await Messages.get(deps.db, tenant, tipId)
  if (target.isErr() || target.value === null) {
    await writeMessageFact(deps.bus, tenant, sid, factFromPersist('', '', ''))
    return
  }
  const partsRes = await Parts.listByMessages(deps.db, tenant, [tipId])
  let text = ''
  if (partsRes.isOk()) {
    for (const p of partsRes.value) {
      if (p.type !== 'text') continue
      const d = parse(TextPartDataSchema, p.data)
      if (d.isOk()) {
        text += d.value.text
        break
      }
    }
  }
  await writeMessageFact(
    deps.bus,
    tenant,
    sid,
    factFromPersist(
      target.value.created_at ?? '',
      target.value.role ?? '',
      text,
    ),
  )
}

import {
  upsertFile,
  fileBySha,
  randomCode,
  type FileRecord,
} from '@easylab-agent/agent'
import { createHash } from 'node:crypto'

function getSha(data: Uint8Array): string {
  return createHash('sha256').update(data).digest('hex')
}

/** Dedup + store a single file. Shared by ingest/upload in the Connect surface. */
async function storeBytes(
  deps: AgentDeps,
  tenant: string,
  data: Uint8Array,
  name: string,
  mime: string,
  uploader: string,
): Promise<FileRecord> {
  const sha = getSha(data)
  const existing = await fileBySha(deps.bus, tenant, sha)
  if (existing.isOk() && existing.value !== null) {
    return existing.value
  }
  const code = randomCode()
  const record: FileRecord = {
    code,
    sha256: sha,
    name,
    mime,
    size: data.length,
    uploader_session: uploader,
    created_at: new Date().toISOString(),
  }
  await deps.files.put(tenant, code, record, data)
  await upsertFile(deps.bus, tenant, record)
  return record
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

function toStructValue(v: unknown): Record<string, unknown> {
  if (v === null || v === undefined) return { case: 'nullValue', value: 0 }
  if (typeof v === 'string') return { case: 'stringValue', value: v }
  if (typeof v === 'number') return { case: 'numberValue', value: v }
  if (typeof v === 'boolean') return { case: 'boolValue', value: v }
  if (Array.isArray(v))
    return { case: 'listValue', value: { values: v.map(toStructValue) } }
  if (isRecord(v))
    return {
      case: 'structValue',
      value: { fields: toStructFields(v) },
    }
  return { case: 'stringValue', value: String(v) }
}

function toStructFields(o: Record<string, unknown>): Record<string, unknown> {
  const fields: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(o)) fields[k] = toStructValue(v)
  return fields
}
