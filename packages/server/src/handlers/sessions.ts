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
  discoverTools,
  factFromPersist,
  fileByCode,
  findVariant,
  fireAndForget,
  GATEWAY_API_TYPE,
  getModelsDev,
  interruptRun,
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
} from '@abcp-agent/agent'
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
} from '@abcp-agent/schema'
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
 * Session lifecycle handlers: health, list/create/get/delete, fork, rename, setModel, undo, state, mailbox, updateSettings, interrupt, compact.
 */

export function sessionsHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async health() {
      return { ok: true, name: 'abcp-agent' }
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
      return {
        sessions: r.value.map(s => sessionToMsg(s, facts.get(s.name))),
      }
    },

    async createSession(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const body = req
      const exists = await Sessions.exists(deps.db, tenant, body.name)
      if (exists.isErr()) throw new Error(exists.error)
      if (exists.value) throw new Error('Session already exists')
      const { preset, model } = await resolveSessionDefaults(
        deps,
        tenant,
        body.preset,
        body.model,
      )
      const name = await Sessions.create(deps.db, tenant, {
        name: body.name,
        model,
        variant: body.variant,
        preset,
        group: body.group,
      })
      if (name.isErr()) throw new Error(name.error)
      publishLifecycle(deps.bus, tenant, 'created', {
        session_name: body.name,
      })
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
      interruptRun(tenant, id)
      const r = await Sessions.delete(deps.db, tenant, id)
      if (r.isErr()) throw new Error(r.error)
      publishLifecycle(deps.bus, tenant, 'deleted', { session_name: id })
      return { ok: true }
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
      if (model.trim() === '') {
        throw new ConnectError(
          'model is required (cannot be cleared)',
          Code.InvalidArgument,
        )
      }
      // A set model MUST be registered/resolvable.
      const resolved = await deps.llm.resolve(deps.db, tenant, model)
      if (resolved.isErr()) {
        throw new ConnectError(
          `unknown or unusable model: ${model} (${resolved.error})`,
          Code.InvalidArgument,
        )
      }
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
      interruptRun(tenant, id)
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
      // Announce the withdrawn chain on the session event stream so any
      // OTHER device viewing this session refetches (cross-device revert).
      pushChainChanged(deps.bus, tenant, id, target.value.prev_id, 'undo')
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
      const { id, limit, before } = req
      // Newest-first, paged backward (older) for infinite scroll: `before` is
      // the oldest entry the client already holds ('' = newest page).
      const r = await Mailbox.listPage(deps.db, tenant, id, limit, before ?? '')
      if (r.isErr()) throw new Error(r.error)
      return {
        ok: true,
        hasMore: r.value.hasMore,
        mailbox: r.value.rows.map(m => ({
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
      // Only model / preset / locale / variant are client-editable. Empty
      // model/preset are ignored (the update simply does not change them);
      // non-empty values MUST be registered/resolvable.
      const cleanPatch: {
        model?: string
        preset?: string
        variant?: string
        locale?: string
      } = {}
      if (patch.preset !== undefined && patch.preset !== '') {
        const p = await Presets.get(deps.bus, tenant, patch.preset)
        if (p.isErr()) throw new Error(p.error)
        if (p.value === null) {
          throw new ConnectError(
            `unknown preset: ${patch.preset}`,
            Code.InvalidArgument,
          )
        }
        cleanPatch.preset = patch.preset
      }
      if (patch.model !== undefined && patch.model !== '') {
        const resolved = await deps.llm.resolve(deps.db, tenant, patch.model)
        if (resolved.isErr()) {
          throw new ConnectError(
            `unknown or unusable model: ${patch.model} (${resolved.error})`,
            Code.InvalidArgument,
          )
        }
        cleanPatch.model = patch.model
      }
      if (patch.variant !== undefined) cleanPatch.variant = patch.variant
      if (patch.locale !== undefined) cleanPatch.locale = patch.locale
      const r = await Sessions.updateSettings(deps.db, tenant, id, cleanPatch)
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
      interruptRun(tenant, id)
      void deps.bus
        .publish(mailboxSubject(tenant, id), {
          type: 'interrupt',
          session_name: id,
        })
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
  }
}
