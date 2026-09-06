import { Agent as AbcAgent, isSessionRunning } from '@abc-protocol/sdk'
import { $, createRoute, OpenAPIHono } from '@hono/zod-openapi'
import {
  appendSessionId,
  compactSession,
  DEFAULT_PRESET,
  deleteSessionIds,
  fileByCode,
  fireAndForget,
  interruptRun,
  Mailbox,
  Messages,
  mailboxSubject,
  Parts,
  publishLifecycle,
  Sessions,
  sseSubject,
} from '@zergx-agent/agent'
import {
  CreateSessionBodySchema,
  ForkBodySchema,
  ModelBodySchema,
  PromptBodySchema,
  RenameBodySchema,
  type SessionRow,
  SessionSettingsBodySchema,
  UndoBodySchema,
} from '@zergx-agent/schema'
import type { Context } from 'hono'
import { streamSSE } from 'hono/streaming'
import { z } from 'zod'
import { type AppEnv, EidDedup } from '../context.js'

/** Serialize a session row into the zergx UI contract. */
function sessionToJson(s: SessionRow): Record<string, unknown> {
  return { ...s }
}

function err500(c: Context, message: string) {
  return c.json({ ok: false, error: message }, 500)
}

// ---- SSE: durable JetStream replay + live, deduped by eid ----

const EidEventSchema = z.object({ eid: z.string().optional() }).passthrough()

async function sseHandler(c: Context<AppEnv>): Promise<Response> {
  const { bus } = c.get('deps')
  const sid = c.req.param('id')
  if (sid === undefined) {
    return c.json({ ok: false, error: 'session not found' }, 404)
  }
  const agent = new AbcAgent(bus)
  return streamSSE(c, async stream => {
    const subject = sseSubject(sid)

    // Subscribe live BEFORE replaying so the handover can overlap, not drop.
    let sub: Awaited<ReturnType<typeof bus.subscribe>>
    try {
      sub = await bus.subscribe(subject)
    } catch (e) {
      await stream.writeSSE({
        data: JSON.stringify({
          event: 'error',
          params: { message: String(e) },
        }),
      })
      return
    }

    let closed = false
    stream.onAbort(() => {
      closed = true
      void sub.close()
    })

    const dedup = new EidDedup()
    // Replay only the trailing in-flight turn, not the whole history. All
    // already-persisted messages are served by GET /messages (walked from the
    // current tip), so replaying their old text/tool events here would re-
    // materialize messages the user has since undone — the UI would show
    // withdrawn history. We split the retained events on the last turn
    // boundary (status busy / turn-complete) and emit only the events after
    // the most recent boundary, which is exactly the still-streaming turn (or
    // nothing when idle).
    //
    // BUT: an undone turn's events may still sit in the retained stream after
    // the last boundary (undo moves the tip back without a new boundary).
    // Replaying them resurrects a withdrawn message. So only replay when a
    // turn is genuinely running (lease held) — an idle session (turn done or
    // undone) has nothing to recover; GET /messages is authoritative.
    if (await isSessionRunning(bus, sid)) {
      const replay = await agent.replayEvents(sid)
      let tailStart = -1
      for (let i = replay.length - 1; i >= 0; i--) {
        const e = replay[i]?.event
        const p = (replay[i]?.params ?? {}) as { type?: string }
        if (e === 'turn-complete' || (e === 'status' && p.type === 'busy')) {
          tailStart = i
          break
        }
      }
      if (tailStart >= 0) {
        for (let i = tailStart; i < replay.length; i++) {
          const raw = replay[i]
          const v = EidEventSchema.safeParse(raw)
          if (!v.success) continue
          dedup.mark(v.data.eid)
          await stream.writeSSE({ data: JSON.stringify(raw) })
        }
      }
    }
    for await (const m of sub) {
      if (closed) break
      const parsed = EidEventSchema.safeParse(m.payload)
      if (!parsed.success) continue
      const v = parsed.data
      if (dedup.duplicate(v.eid)) continue
      await stream.writeSSE({ data: JSON.stringify(v) })
    }
  })
}

const SessionJsonSchema = z.record(z.string(), z.unknown())

const ErrorSchema = z.object({ ok: z.boolean(), error: z.string() })

// ---- route definitions ----

const listSessionsRoute = createRoute({
  method: 'get',
  path: '/sessions',
  summary: 'List sessions',
  responses: {
    200: {
      description: 'Sessions',
      content: {
        'application/json': {
          schema: z.object({ sessions: z.array(z.unknown()) }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const createSessionRoute = createRoute({
  method: 'post',
  path: '/sessions',
  summary: 'Create a session',
  request: {
    body: {
      content: { 'application/json': { schema: CreateSessionBodySchema } },
    },
  },
  responses: {
    200: {
      description: 'Created',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), session_name: z.string() }),
        },
      },
    },
    409: {
      description: 'Exists',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const getSessionRoute = createRoute({
  method: 'get',
  path: '/sessions/{id}',
  summary: 'Get a session',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Session',
      content: {
        'application/json': {
          schema: z.object({ session: SessionJsonSchema }),
        },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const deleteSessionRoute = createRoute({
  method: 'delete',
  path: '/sessions/{id}',
  summary: 'Delete a session',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Ok',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const listMessagesRoute = createRoute({
  method: 'get',
  path: '/sessions/{id}/messages',
  summary: 'List messages',
  request: {
    params: z.object({ id: z.string() }),
    query: z.object({
      limit: z.string().optional(),
      before: z.string().optional(),
    }),
  },
  responses: {
    200: {
      description: 'Messages',
      content: {
        'application/json': {
          schema: z.object({ messages: z.array(z.unknown()) }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const promptRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/prompt',
  summary: 'Send a user prompt',
  request: {
    params: z.object({ id: z.string() }),
    body: { content: { 'application/json': { schema: PromptBodySchema } } },
  },
  responses: {
    200: {
      description: 'Ok',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const forkRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/fork',
  summary: 'Fork a session',
  request: {
    params: z.object({ id: z.string() }),
    body: { content: { 'application/json': { schema: ForkBodySchema } } },
  },
  responses: {
    200: {
      description: 'Forked',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), session_name: z.string() }),
        },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    409: {
      description: 'Exists',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const renameRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/rename',
  summary: 'Rename a session',
  request: {
    params: z.object({ id: z.string() }),
    body: { content: { 'application/json': { schema: RenameBodySchema } } },
  },
  responses: {
    200: {
      description: 'Renamed',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), session_name: z.string() }),
        },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    409: {
      description: 'Exists',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const modelRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/model',
  summary: 'Set session model',
  request: {
    params: z.object({ id: z.string() }),
    body: { content: { 'application/json': { schema: ModelBodySchema } } },
  },
  responses: {
    200: {
      description: 'Model',
      content: {
        'application/json': { schema: z.object({ model: z.string() }) },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const undoRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/undo',
  summary: 'Undo (move tip pointer back)',
  request: {
    params: z.object({ id: z.string() }),
    body: { content: { 'application/json': { schema: UndoBodySchema } } },
  },
  responses: {
    200: {
      description: 'Undone',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), undone: z.boolean() }),
        },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const stateRoute = createRoute({
  method: 'get',
  path: '/sessions/{id}/state',
  summary: 'Session state',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'State',
      content: {
        'application/json': {
          schema: z.object({ status: z.string(), parts: z.array(z.unknown()) }),
        },
      },
    },
  },
})

const mailboxRoute = createRoute({
  method: 'get',
  path: '/sessions/{id}/mailbox',
  summary: 'List mailbox entries',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Entries',
      content: {
        'application/json': {
          schema: z.object({ entries: z.array(z.unknown()) }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const settingsRoute = createRoute({
  method: 'patch',
  path: '/sessions/{id}/settings',
  summary: 'Update session settings',
  request: {
    params: z.object({ id: z.string() }),
    body: {
      content: { 'application/json': { schema: SessionSettingsBodySchema } },
    },
  },
  responses: {
    200: {
      description: 'Updated session',
      content: {
        'application/json': {
          schema: z.object({ session: SessionJsonSchema }),
        },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const interruptRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/interrupt',
  summary: 'Interrupt an in-flight run',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Interrupted',
      content: {
        'application/json': { schema: z.object({ interrupted: z.boolean() }) },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const compactRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/compact',
  summary: 'Compact session history (rule-based, no LLM)',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Compacted',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const sessionOpenapi = new OpenAPIHono<AppEnv>()
  .openapi(listSessionsRoute, async c => {
    const { db } = c.get('deps')
    const r = await Sessions.list(db)
    return r.isErr()
      ? err500(c, r.error)
      : c.json({ sessions: r.value.map(sessionToJson) }, 200)
  })
  .openapi(createSessionRoute, async c => {
    const deps = c.get('deps')
    const b = c.req.valid('json')
    const exists = await Sessions.exists(deps.db, b.name)
    if (exists.isErr()) return err500(c, exists.error)
    if (exists.value) {
      return c.json({ ok: false, error: 'Session already exists' }, 409)
    }
    const name = await Sessions.create(deps.db, b)
    if (name.isErr()) return err500(c, name.error)
    publishLifecycle(deps.bus, 'created', { session_name: b.name })
    return c.json({ ok: true, session_name: name.value }, 200)
  })
  .openapi(getSessionRoute, async c => {
    const { db } = c.get('deps')
    const r = await Sessions.get(db, c.req.valid('param').id)
    if (r.isErr()) return err500(c, r.error)
    return r.value === null
      ? c.json({ ok: false, error: 'session not found' }, 404)
      : c.json({ session: sessionToJson(r.value) }, 200)
  })
  .openapi(deleteSessionRoute, async c => {
    const deps = c.get('deps')
    const id = c.req.valid('param').id
    interruptRun(id)
    const r = await Sessions.delete(deps.db, id)
    if (r.isErr()) return err500(c, r.error)
    publishLifecycle(deps.bus, 'deleted', { session_name: id })
    return c.json({ ok: true }, 200)
  })
  .openapi(listMessagesRoute, async c => {
    const { db } = c.get('deps')
    const { id } = c.req.valid('param')
    const q = c.req.valid('query')
    // Treat empty-string params as absent: `?limit=` (e.g. from a proxy that
    // always emits the key) must not coerce to NaN and silently return zero
    // messages.
    const limit = Number.parseInt(q.limit?.trim() || '50', 10) || 50
    const before = q.before?.trim() || null
    const tipRes = await Sessions.tip(db, id)
    const tipId = tipRes.isErr() ? null : tipRes.value
    const r = await Messages.chain(db, tipId, limit, before)
    return r.isErr() ? err500(c, r.error) : c.json({ messages: r.value }, 200)
  })
  .openapi(promptRoute, async c => {
    const deps = c.get('deps')
    const { id } = c.req.valid('param')
    const b = c.req.valid('json')

    const session = await Sessions.get(deps.db, id)
    if (session.isErr()) return err500(c, session.error)
    if (session.value === null) {
      return c.json({ ok: false, error: 'session not found' }, 404)
    }

    // Persist the user message first (chained onto the tip) so the turn loop
    // never runs against a history missing the prompt. Attachments are
    // persisted as their OWN `file` parts (structured), then the text — so
    // the reference is a distinct part, not padded into the prompt text.
    const tipRes = await Sessions.tip(deps.db, id)
    const tipId = tipRes.isErr() ? null : tipRes.value
    const insert = await Messages.insert(deps.db, 'user', tipId)
    if (insert.isErr()) return err500(c, insert.error)
    let seq = 0
    for (const att of b.attachments ?? []) {
      // Client only sends the code; backfill name/mime/size from the files KV
      // so the persisted `file` part carries full metadata (old + new).
      let name = att.name
      let mime = att.mime
      let size = att.size
      if (name === undefined && mime === undefined && size === undefined) {
        const rec = await fileByCode(deps.bus, att.code)
        if (rec.isOk() && rec.value !== null) {
          name ??= rec.value.name
          mime ??= rec.value.mime
          size ??= rec.value.size
        }
      }
      await Parts.insert(deps.db, insert.value, 'file', seq++, {
        code: att.code,
        name,
        mime,
        size,
      })
    }
    if (b.prompt !== '') {
      await Parts.insert(deps.db, insert.value, 'text', seq++, {
        text: b.prompt,
      })
    }
    await Sessions.setTip(deps.db, id, insert.value)

    // Keep the cached context id list in sync so the turn's loadHistory does
    // not serve a stale cache missing this just-persisted user message.
    fireAndForget(
      appendSessionId(deps.bus, id, insert.value),
      'appendSessionIds',
    )

    // Deliver the turn request over the durable mailbox queue. The agent's
    // consumer persists it into PG and runs the turn; the HTTP route never
    // writes the mailbox table directly.
    try {
      await new AbcAgent(deps.bus).publishMailbox(id, 'user_prompt', {
        text: b.prompt,
        attachments: b.attachments ?? [],
      })
    } catch (e) {
      // Roll the tip back so a failed delivery does not leave a dangling
      // user message with no turn to answer it.
      fireAndForget(
        Promise.resolve(Sessions.setTip(deps.db, id, tipId)),
        'setTip-rollback',
      )
      fireAndForget(deleteSessionIds(deps.bus, id), 'deleteSessionIds')
      return err500(c, `mailbox publish failed: ${String(e)}`)
    }

    return c.json({ ok: true }, 200)
  })
  .openapi(forkRoute, async c => {
    const deps = c.get('deps')
    const pid = c.req.valid('param').id
    const b = c.req.valid('json')
    const parent = await Sessions.get(deps.db, pid)
    if (parent.isErr()) return err500(c, parent.error)
    if (parent.value === null) {
      return c.json({ ok: false, error: 'session not found' }, 404)
    }
    const p = parent.value

    const exists = await Sessions.exists(deps.db, b.name)
    if (exists.isErr()) return err500(c, exists.error)
    if (exists.value) {
      return c.json({ ok: false, error: 'Session already exists' }, 409)
    }

    // Optional fork point: a message on the parent's chain. Absent forks
    // from the parent's current tip; present forks from that exact message
    // (callers pin the fork to the moment they captured the id). The
    // membership walk is the same hijack guard the undo route uses — a
    // message from another session's chain must not become our fork base.
    let forkTip: string | null = p.tip_id
    if (b.message_id !== undefined) {
      const target = await Messages.get(deps.db, b.message_id)
      if (target.isErr()) return err500(c, target.error)
      if (target.value === null) {
        return c.json({ ok: false, error: 'fork message not found' }, 404)
      }
      if (p.tip_id !== null && p.tip_id !== '') {
        const inChain = await Messages.isInChain(
          deps.db,
          p.tip_id,
          b.message_id,
        )
        if (inChain.isErr()) return err500(c, inChain.error)
        if (!inChain.value) {
          return c.json(
            { ok: false, error: 'fork message not in this session chain' },
            409,
          )
        }
      }
      forkTip = b.message_id
    }

    const name = await Sessions.create(deps.db, {
      name: b.name,
      model: p.model,
      // Manual fork/rename inherits the parent's full config (model, preset,
      // system_prompt, max_turns, locale) so it behaves identically. ONLY the
      // repo-extension `fork-branch` worksheet overrides preset to 'build'
      // (a work-branch sub-agent must edit/build); it passes explicit `preset`
      // in the fork body, which wins here.
      preset: b.preset ?? (p.preset !== '' ? p.preset : DEFAULT_PRESET),
      systemPrompt: p.system_prompt,
      maxTurns: p.max_turns,
      locale: p.locale,
      tipId: forkTip,
    })
    if (name.isErr()) return err500(c, name.error)
    publishLifecycle(deps.bus, 'forked', {
      session_name: b.name,
      parent: pid,
    })
    return c.json({ ok: true, session_name: name.value }, 200)
  })
  .openapi(renameRoute, async c => {
    const deps = c.get('deps')
    const oldName = c.req.valid('param').id
    const b = c.req.valid('json')

    if (b.name === oldName) {
      return c.json({ ok: true, session_name: oldName }, 200)
    }
    const parent = await Sessions.get(deps.db, oldName)
    if (parent.isErr()) return err500(c, parent.error)
    if (parent.value === null) {
      return c.json({ ok: false, error: 'session not found' }, 404)
    }
    const exists = await Sessions.exists(deps.db, b.name)
    if (exists.isErr()) return err500(c, exists.error)
    if (exists.value) {
      return c.json({ ok: false, error: 'Session already exists' }, 409)
    }
    const p = parent.value

    // rename = fork (copy tip) into the new name + delete the old name.
    // Messages are shared COW; deleting the old session leaves history intact.
    // Rename preserves the parent's full config, same as a manual fork.
    const created = await Sessions.create(deps.db, {
      name: b.name,
      model: p.model,
      preset: p.preset !== '' ? p.preset : DEFAULT_PRESET,
      systemPrompt: p.system_prompt,
      maxTurns: p.max_turns,
      locale: p.locale,
      tipId: p.tip_id,
    })
    if (created.isErr()) return err500(c, created.error)
    const removed = await Sessions.delete(deps.db, oldName)
    if (removed.isErr()) {
      // Roll the fork back so a failed delete never leaves two sessions
      // pointing at the same tip.
      void Sessions.delete(deps.db, b.name)
      return err500(c, removed.error)
    }
    publishLifecycle(deps.bus, 'renamed', { from: oldName, to: b.name })
    return c.json({ ok: true, session_name: b.name }, 200)
  })
  .openapi(modelRoute, async c => {
    const { db } = c.get('deps')
    const { id } = c.req.valid('param')
    const { model } = c.req.valid('json')
    const r = await Sessions.setModel(db, id, model)
    return r.isErr() ? err500(c, r.error) : c.json({ model }, 200)
  })
  .openapi(undoRoute, async c => {
    const deps = c.get('deps')
    const sid = c.req.valid('param').id
    const { message_id } = c.req.valid('json')

    const session = await Sessions.get(deps.db, sid)
    if (session.isErr()) return err500(c, session.error)
    const s = session.value
    if (s === null)
      return c.json({ ok: false, error: 'session not found' }, 404)

    const tip = s.tip_id
    if (tip === null || tip === '') {
      return c.json({ ok: false, undone: false }, 200)
    }
    const targetId = message_id ?? tip
    const target = await Messages.get(deps.db, targetId)
    if (target.isErr()) return err500(c, target.error)
    if (target.value === null) return c.json({ ok: false, undone: false }, 200)

    // The undo target must belong to this session's chain; a tip pointer onto
    // a foreign session's message would hijack the chain.
    const inChain = await Messages.isInChain(deps.db, tip, targetId)
    if (inChain.isErr()) return err500(c, inChain.error)
    if (!inChain.value) {
      return c.json({ ok: false, undone: false }, 200)
    }

    // undo == move the tip pointer back; messages are append-only and never
    // physically deleted (COW). At the chain head this becomes a no-op.
    await Sessions.setTip(deps.db, sid, target.value.prev_id)

    // The context id cache no longer matches the new tip; drop it so the next
    // load re-walks the chain.
    fireAndForget(deleteSessionIds(deps.bus, sid), 'deleteSessionIds')

    return c.json({ ok: true, undone: true }, 200)
  })
  .openapi(stateRoute, async c => {
    const deps = c.get('deps')
    const sid = c.req.valid('param').id
    const running = await isSessionRunning(deps.bus, sid)
    return c.json({ status: running ? 'busy' : 'idle', parts: [] }, 200)
  })
  .openapi(mailboxRoute, async c => {
    const { db } = c.get('deps')
    const r = await Mailbox.list(db, c.req.valid('param').id)
    return r.isErr() ? err500(c, r.error) : c.json({ entries: r.value }, 200)
  })
  .openapi(settingsRoute, async c => {
    const { db } = c.get('deps')
    const sid = c.req.valid('param').id
    const b = c.req.valid('json')
    const r = await Sessions.updateSettings(db, sid, {
      model: b.model,
      preset: b.preset,
      maxTurns: b.max_turns,
      systemPrompt: b.system_prompt,
      locale: b.locale,
    })
    if (r.isErr()) return err500(c, r.error)
    const s = await Sessions.get(db, sid)
    if (s.isErr()) return err500(c, s.error)
    return s.value === null
      ? c.json({ ok: false, error: 'session not found' }, 404)
      : c.json({ session: sessionToJson(s.value) }, 200)
  })
  .openapi(interruptRoute, async c => {
    const deps = c.get('deps')
    const sid = c.req.valid('param').id
    // 1. Local mid-stream abort (this replica).
    interruptRun(sid)
    // 2. Cross-replica abort: publish directly onto the mailbox wake subject.
    //    Never enqueued in the mailbox table — whichever replica is running
    //    the session watches this subject mid-stream and aborts immediately.
    void deps.bus
      .publish(mailboxSubject(sid), {
        type: 'interrupt',
        session_name: sid,
      })
      .catch(() => undefined)
    return c.json({ interrupted: true }, 200)
  })
  .openapi(compactRoute, async c => {
    const deps = c.get('deps')
    const sid = c.req.valid('param').id
    const r = await compactSession(deps, sid)
    return r.isErr() ? err500(c, r.error) : c.json({ ok: r.value }, 200)
  })

// SSE endpoints return a raw streaming Response, so they are registered as
// plain GET routes (the `.openapi()` path cannot express a stream response).
export const sessionRoutes = $(sessionOpenapi)
  .get('/sessions/:id/stream', sseHandler)
  .get('/sessions/:id/events', sseHandler)
