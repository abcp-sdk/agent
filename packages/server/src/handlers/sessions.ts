import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  type AgentDeps,
  clearActiveRun,
  DEFAULT_PRESET,
  deleteMessageFact,
  deleteSessionIds,
  interruptRun,
  logger,
  Mailbox,
  Messages,
  mailboxSubject,
  normalizeLocale,
  Presets,
  publishLifecycle,
  publishSessionChanged,
  pushChainChanged,
  readMessageFacts,
  readSessionStatus,
  readSessionStatuses,
  releaseLease,
  Sessions,
} from '@abcp-agent/agent'
import type { AgentService } from '@abcp-agent/schema'
import {
  Code,
  ConnectError,
  type HandlerContext,
  type ServiceImpl,
} from '@connectrpc/connect'
import { resolveSessionDefaults } from '../session-defaults.js'
import { tenantOf } from '../tenant.js'
import { sessionToMsg } from '../views.js'
import { refreshMessageFactFromTip } from './helpers.js'

/**
 * Session lifecycle handlers: health, list/create/get/delete, fork, rename, setModel, undo, state, mailbox, updateSettings, interrupt, compact.
 */

export function sessionsHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  /**
   * Remove every KV projection for a session name (message fact, context id
   * cache, run lock). Called on delete/rename so no stale projection survives
   * (the DB row is the source of truth; the KV is derived). Best-effort: a
   * transient KV error only delays convergence.
   */
  const purgeSessionProjections = async (
    tenant: string,
    name: string,
  ): Promise<void> => {
    await Promise.all([
      deleteMessageFact(deps.bus, tenant, name).catch(() => {}),
      deleteSessionIds(deps.bus, tenant, name).catch(() => {}),
      releaseLease(deps.bus, tenant, name).catch(() => {}),
    ])
  }

  /**
   * Log an ERROR when a session that HAS a message tip is missing its KV fact.
   * The KV fact is a derived projection of the DB chain; for a session with
   * messages a miss means the projection is stale/lost (a transient NATS error,
   * or a write that never landed). An EMPTY session (no tip) legitimately has no
   * fact, so it is not an error. We never fabricate `message_seq: 0` silently —
   * that would reset clients' unread watermarks.
   */
  const logMissingFact = (
    tenant: string,
    name: string,
    tipId: string,
  ): void => {
    if (tipId === '') return
    logger.error(
      { tenant, sid: name },
      'session message-fact projection missing (KV) — preview/seq unavailable',
    )
  }

  return {
    async health() {
      return { ok: true, name: 'abcp-agent' }
    },

    async listSessions(_req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const r = await Sessions.list(deps.db, tenant)
      if (r.isErr()) throw new Error(r.error)
      const names = r.value.map(s => s.name)
      // Batch-read the message facts AND the runtime status (run lock) so the
      // list rows carry busy/idle without a per-row State poll.
      const [facts, statuses] = await Promise.all([
        readMessageFacts(deps.bus, tenant, names),
        readSessionStatuses(deps.bus, tenant, names),
      ])
      // Surface a MISSING fact as an error (a derived projection that is stale
      // or lost) instead of silently emitting message_seq=0 / empty preview.
      for (const s of r.value) {
        if (!facts.has(s.name)) logMissingFact(tenant, s.name, s.tip_id ?? '')
      }
      return {
        sessions: r.value.map(s =>
          sessionToMsg(s, facts.get(s.name), statuses.get(s.name) ?? 'idle'),
        ),
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
        // Pin the session language at creation. An EMPTY request means "follow
        // the tenant default at turn time", so it must stay UNSET — do NOT run
        // it through normalizeLocale, whose `'' -> 'en'` fallback would pin the
        // session to English and make it win over a zh tenant config in
        // resolveLocale. A non-empty value is normalized so "ZH"/"zh_CN"
        // collapse to "zh". (updateSettings already treats '' as "unset".)
        locale: body.locale ? normalizeLocale(body.locale) : '',
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
      if (!facts.has(req.id)) {
        logMissingFact(tenant, req.id, r.value.tip_id ?? '')
      }
      return { session: sessionToMsg(r.value, facts.get(req.id)) }
    },

    async deleteSession(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const id = req.id
      interruptRun(tenant, id)
      const r = await Sessions.delete(deps.db, tenant, id)
      if (r.isErr()) throw new Error(r.error)
      // Drop the derived KV projections too: a deleted session must not leave a
      // fact (bucket is TTL 0), an id-list cache, or a run lock behind.
      await purgeSessionProjections(tenant, id)
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
      // Anchor the child at the message BEFORE the parent's current-turn
      // prompt when no explicit fork point is given. The assistant step that is
      // invoking the caller's tool is not persisted yet, so a parent tip that is
      // a `user` message IS the prompt that started this turn (e.g. "create
      // branch X"). Copying it onto the child would hand the child its parent's
      // own instruction — the same defect the subsession fork fixed in
      // `Messages.forkBase`. An explicit `messageId` always wins (fork-at-a-
      // message).
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
      } else {
        const base = await Messages.forkBase(deps.db, tenant, p.tip_id)
        if (base.isErr()) throw new Error(base.error)
        forkTip = base.value
      }
      // A fork that CHANGES the preset must not inherit the parent's
      // session-level system-prompt override: that text belongs to the parent's
      // role and would shadow the new preset's prompt (a maintainer's text
      // leaking into a developer branch). Carry it over only when the preset is
      // unchanged (a same-role continuation, e.g. a subsession fork).
      const parentPreset = p.preset !== '' ? p.preset : DEFAULT_PRESET
      const childPreset = preset ?? parentPreset
      const childSystemPrompt =
        childPreset === parentPreset ? p.system_prompt : ''
      const created = await Sessions.create(deps.db, tenant, {
        name,
        model: p.model,
        preset: childPreset,
        systemPrompt: childSystemPrompt,
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
        // A rename is the SAME conversation under a new name: carry the
        // authoritative counter so clients' read watermarks stay valid.
        messageSeq: p.message_seq,
      })
      if (created.isErr()) throw new Error(created.error)
      const removed = await Sessions.delete(deps.db, tenant, id)
      if (removed.isErr()) {
        void Sessions.delete(deps.db, tenant, name)
        throw new Error(removed.error)
      }
      // Move the KV projections from the OLD name to the NEW one: drop the old
      // fact/ids/lock, then rebuild the new name's fact from the (same) tip so
      // the list shows a preview immediately instead of after the next message.
      await purgeSessionProjections(tenant, id)
      await refreshMessageFactFromTip(deps, tenant, name)
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
      // Abort any in-flight turn and drop its active-run anchor BEFORE moving
      // the tip: a running turn must not keep emitting deltas (or a late
      // turn-complete) for content we are withdrawing, and a reconnecting
      // client must not replay that run.
      interruptRun(tenant, id)
      await clearActiveRun(deps.bus, tenant, id)
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
      // Owner-aware: a lease whose owner's heartbeat is gone reads idle, so a
      // crashed replica's session is not reported busy until the lease TTL.
      const status = await readSessionStatus(deps.bus, tenant, id)
      return { state: { status } }
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
          source: m.source,
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
      const session = await Sessions.get(deps.db, tenant, id)
      if (session.isErr()) throw new Error(session.error)
      if (session.value === null) throw new Error('session not found')
      // Enqueue the compaction on the MAILBOX rather than running it inline.
      // The mailbox is drained at a step boundary UNDER the session's run
      // lease, so a manual compact can never race a running turn's chain write
      // (the two would otherwise fork the chain). The result is reported via
      // the `compacted` event (ok true/false), not this response — `ok` here
      // means "accepted", exactly like Prompt's `accepted`.
      await new AbcAgent(deps.bus).publishMailbox(
        tenant,
        id,
        'compact',
        { reason: 'manual' },
        'user',
      )
      return { ok: true }
    },
  }
}
