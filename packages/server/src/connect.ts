import { Agent as AbcAgent, isSessionRunning } from '@abc-protocol/sdk'
import {
  appendSessionId,
  Config,
  compactSession,
  deleteSessionIds,
  discoverTools,
  dispatchDecision,
  fileByCode,
  fireAndForget,
  interruptRun,
  localizeSchema,
  Mailbox,
  mailboxSubject,
  Messages,
  Parts,
  pickDescription,
  Presets,
  Providers,
  publishLifecycle,
  renderTemplate,
  resolveLocale,
  Sessions,
  toolConfigMap,
  DEFAULT_PRESET,
} from '@zergx-agent/agent'
import { type AgentDeps } from '@zergx-agent/agent'
import {
  createConnectRouter,
  type ServiceImpl,
} from '@connectrpc/connect'
import {
  createFetchHandler,
  universalServerRequestFromFetch,
  universalServerResponseToFetch,
} from '@connectrpc/connect/protocol'
import { create, fromJson } from '@bufbuild/protobuf'
import { StructSchema } from '@bufbuild/protobuf/wkt'
import {
  AgentService,
  ListToolsResponseSchema,
  GetZergxConfigResponseSchema,
} from '@zergx-agent/schema'

function sessionToMsg(s: AgentDeps['db'] extends never ? never : any) {
  const name = s.name as string
  return {
    name,
    model: s.model ?? '',
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
    unreadCount: s.unread_count ?? 0,
    lastMessageAt: s.last_message_at ?? '',
    lastMessagePreview: s.last_message_preview ?? '',
  }
}

function providerToMsg(p: any) {
  let headers: Record<string, string> = {}
  let models: string[] = []
  try {
    headers = JSON.parse(p.headers ?? '{}') ?? {}
  } catch {}
  try {
    models = JSON.parse(p.models ?? '[]') ?? []
  } catch {}
  return {
    providerId: p.provider_id ?? '',
    apiType: p.api_type ?? '',
    baseUrl: p.base_url ?? '',
    apiKey: p.api_key ?? '',
    headers,
    models,
    updatedAt: p.updated_at ?? '',
  }
}

function presetToMsg(p: any) {
  let tools: string[] = []
  try {
    tools = JSON.parse(p.tools ?? '[]') ?? []
  } catch {}
  return {
    id: p.id ?? '',
    systemPrompt: p.system_prompt ?? '',
    tools,
    maxTurns: p.max_turns ?? 0,
    isSystem: p.is_system ?? false,
  }
}

function worksheetToMsg(w: any) {
  return {
    id: w.id ?? '',
    sessionName: w.session_name ?? '',
    extId: w.ext_id ?? '',
    action: w.action ?? '',
    args: w.args ?? '',
    title: w.title ?? '',
    originCallId: w.origin_call_id ?? '',
    status: w.status ?? '',
    createdAt: w.created_at ?? '',
    decidedAt: w.decided_at ?? '',
  }
}

/** Build the Connect v2 AgentService handler as a fetch-compatible fn. */
export function buildConnectHandler(deps: AgentDeps) {
  const router = createConnectRouter({ grpc: true, grpcWeb: true, connect: true })

  const impl: ServiceImpl<typeof AgentService> = {
    async health() {
      return { ok: true, name: 'zergx-agent' }
    },
    async listSessions() {
      const r = await Sessions.list(deps.db)
      if (r.isErr()) throw new Error(r.error)
      return { sessions: r.value.map(sessionToMsg) }
    },
    async createSession(req) {
      const body = req as { name: string; model?: string; preset?: string }
      const exists = await Sessions.exists(deps.db, body.name)
      if (exists.isErr()) throw new Error(exists.error)
      if (exists.value) throw new Error('Session already exists')
      const name = await Sessions.create(deps.db, {
        name: body.name,
        model: body.model,
        preset: body.preset,
      })
      if (name.isErr()) throw new Error(name.error)
      publishLifecycle(deps.bus, 'created', { session_name: body.name })
      return { ok: true, sessionName: name.value }
    },
    async getSession(req) {
      const r = await Sessions.get(deps.db, req.id)
      if (r.isErr()) throw new Error(r.error)
      if (r.value === null) throw new Error('session not found')
      return { session: sessionToMsg(r.value) }
    },
    async deleteSession(req) {
      const id = req.id
      interruptRun(id)
      const r = await Sessions.delete(deps.db, id)
      if (r.isErr()) throw new Error(r.error)
      publishLifecycle(deps.bus, 'deleted', { session_name: id })
      return { ok: true }
    },
    async listMessages(req) {
      const { id, limit: l, before } = req
      const limit = l && l > 0 ? l : 50
      const tipRes = await Sessions.tip(deps.db, id)
      const tipId = tipRes.isErr() ? null : tipRes.value
      const r = await Messages.chain(deps.db, tipId, limit, before ?? null)
      if (r.isErr()) throw new Error(r.error)
      const messages = r.value.map(m => ({
        id: m.id,
        role: m.role,
        prevId: m.prev_id ?? '',
        createdAt: m.created_at ?? '',
        parts: [],
      }))
      return { ok: true, messages }
    },
    async *prompt(req) {
      const { id, prompt, attachments } = req
      const session = await Sessions.get(deps.db, id)
      if (session.isErr()) throw new Error(session.error)
      if (session.value === null) throw new Error('session not found')

      const tipRes = await Sessions.tip(deps.db, id)
      const tipId = tipRes.isErr() ? null : tipRes.value
      const insert = await Messages.insert(deps.db, 'user', tipId)
      if (insert.isErr()) throw new Error(insert.error)
      let seq = 0
      for (const att of attachments ?? []) {
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
      if (prompt !== '') {
        await Parts.insert(deps.db, insert.value, 'text', seq++, {
          text: prompt,
        })
      }
      await Sessions.setTip(deps.db, id, insert.value)
      fireAndForget(
        appendSessionId(deps.bus, id, insert.value),
        'appendSessionIds',
      )
      await new AbcAgent(deps.bus).publishMailbox(id, 'user_prompt', {
        text: prompt,
        attachments: attachments ?? [],
      })
      yield {
        event: 'accepted',
        params: { message_id: insert.value },
        eid: '',
      }
    },
    async fork(req) {
      const { id, name, messageId, preset } = req
      const parent = await Sessions.get(deps.db, id)
      if (parent.isErr()) throw new Error(parent.error)
      if (parent.value === null) throw new Error('session not found')
      const p = parent.value
      const exists = await Sessions.exists(deps.db, name)
      if (exists.isErr()) throw new Error(exists.error)
      if (exists.value) throw new Error('Session already exists')
      let forkTip: string | null = p.tip_id
      if (messageId !== undefined) {
        const target = await Messages.get(deps.db, messageId)
        if (target.isErr()) throw new Error(target.error)
        if (target.value === null) throw new Error('fork message not found')
        if (p.tip_id !== null && p.tip_id !== '') {
          const inChain = await Messages.isInChain(deps.db, p.tip_id, messageId)
          if (inChain.isErr()) throw new Error(inChain.error)
          if (!inChain.value) throw new Error('fork message not in this session chain')
        }
        forkTip = messageId
      }
      const created = await Sessions.create(deps.db, {
        name,
        model: p.model,
        preset: preset ?? (p.preset !== '' ? p.preset : DEFAULT_PRESET),
        systemPrompt: p.system_prompt,
        maxTurns: p.max_turns,
        locale: p.locale,
        tipId: forkTip,
      })
      if (created.isErr()) throw new Error(created.error)
      publishLifecycle(deps.bus, 'forked', { session_name: name, parent: id })
      return { session: sessionToMsg({ ...p, name }) }
    },
    async rename(req) {
      const { id, name } = req
      if (name === id) return { session: sessionToMsg({ name }) }
      const parent = await Sessions.get(deps.db, id)
      if (parent.isErr()) throw new Error(parent.error)
      if (parent.value === null) throw new Error('session not found')
      const exists = await Sessions.exists(deps.db, name)
      if (exists.isErr()) throw new Error(exists.error)
      if (exists.value) throw new Error('Session already exists')
      const p = parent.value
      const created = await Sessions.create(deps.db, {
        name,
        model: p.model,
        preset: p.preset !== '' ? p.preset : DEFAULT_PRESET,
        systemPrompt: p.system_prompt,
        maxTurns: p.max_turns,
        locale: p.locale,
        tipId: p.tip_id,
      })
      if (created.isErr()) throw new Error(created.error)
      const removed = await Sessions.delete(deps.db, id)
      if (removed.isErr()) {
        void Sessions.delete(deps.db, name)
        throw new Error(removed.error)
      }
      publishLifecycle(deps.bus, 'renamed', { from: id, to: name })
      return { session: sessionToMsg({ ...p, name }) }
    },
    async setModel(req) {
      const { id, model } = req
      const r = await Sessions.setModel(deps.db, id, model)
      if (r.isErr()) throw new Error(r.error)
      const s = await Sessions.get(deps.db, id)
      return { session: s.isOk() && s.value ? sessionToMsg(s.value) : sessionToMsg({ name: id }) }
    },
    async undo(req) {
      const { id, messageId } = req
      const session = await Sessions.get(deps.db, id)
      if (session.isErr()) throw new Error(session.error)
      const s = session.value
      if (s === null) throw new Error('session not found')
      const tip = s.tip_id
      if (tip === null || tip === '') return { session: s ? sessionToMsg(s) : sessionToMsg({ name: id }) }
      const targetId = messageId ?? tip
      const target = await Messages.get(deps.db, targetId)
      if (target.isErr()) throw new Error(target.error)
      if (target.value === null) return { session: sessionToMsg(s) }
      const inChain = await Messages.isInChain(deps.db, tip, targetId)
      if (inChain.isErr()) throw new Error(inChain.error)
      if (!inChain.value) return { session: sessionToMsg(s) }
      await Sessions.setTip(deps.db, id, target.value.prev_id)
      fireAndForget(deleteSessionIds(deps.bus, id), 'deleteSessionIds')
      return { session: sessionToMsg(s) }
    },
    async state(req) {
      const id = req.id
      const running = await isSessionRunning(deps.bus, id)
      return { state: { status: running ? 'busy' : 'idle' } }
    },
    async mailbox(req) {
      const id = req.id
      const r = await Mailbox.list(deps.db, id)
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
    async updateSettings(req) {
      const { id, ...patch } = req
      const r = await Sessions.updateSettings(deps.db, id, patch)
      if (r.isErr()) throw new Error(r.error)
      const s = await Sessions.get(deps.db, id)
      return { session: s.isOk() && s.value ? sessionToMsg(s.value) : sessionToMsg({ name: id }) }
    },
    async interrupt(req) {
      const id = req.id
      interruptRun(id)
      void deps.bus
        .publish(mailboxSubject(id), { type: 'interrupt', session_name: id })
        .catch(() => undefined)
      return { ok: true, interrupted: true }
    },
    async compact(req) {
      const id = req.id
      const r = await compactSession(deps, id)
      if (r.isErr()) throw new Error(r.error)
      return { ok: r.value }
    },
    async listProviders() {
      const r = await Providers.list(deps.db)
      if (r.isErr()) throw new Error(r.error)
      return { providers: r.value.map(providerToMsg) }
    },
    async listProvidersCatalog() {
      return { providers: {} }
    },
    async registerProvider(req) {
      const p = req.provider
      if (!p) throw new Error('provider required')
      const r = await Providers.upsert(deps.db, {
        providerId: p.providerId,
        apiType: p.apiType,
        baseUrl: p.baseUrl,
        apiKey: p.apiKey ?? '',
        headers: p.headers ?? {},
        models: p.models ?? [],
      })
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },
    async deleteProvider(req) {
      const id = req.providerId
      const r = await Providers.delete(deps.db, id)
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },
    async testProvider(req) {
      const r = req
      return { ok: true, result: `testing ${r.providerId ?? r.apiType ?? ''}${r.model ? ' ' + r.model : ''}` }
    },
    async listModels() {
      const r = await Providers.list(deps.db)
      if (r.isErr()) throw new Error(r.error)
      const models: { id: string; name: string }[] = []
      for (const p of r.value) {
        if (!p) continue
        let arr: string[] = []
        try {
          arr = JSON.parse(p.models ?? '[]') ?? []
        } catch {}
        for (const id of arr) models.push({ id, name: id })
      }
      return { models }
    },
    async listPresets() {
      const r = await Presets.list(deps.bus)
      if (r.isErr()) throw new Error(r.error)
      return { presets: r.value.map(presetToMsg) }
    },
    async upsertPreset(req) {
      const p = req.preset
      if (!p) throw new Error('preset required')
      const r = await Presets.upsert(deps.bus, {
        id: p.id,
        systemPrompt: p.systemPrompt ?? '',
        systemPromptI18n: p.systemPromptI18n ?? '{}',
        tools: JSON.stringify(p.tools ?? []),
        maxTurns: p.maxTurns ?? 0,
      })
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },
    async deletePreset(req) {
      const id = req.id
      const r = await Presets.delete(deps.bus, id)
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },
    async previewPreset(req) {
      const id = req.id
      const r = await Presets.get(deps.bus, id)
      if (r.isErr()) throw new Error(r.error)
      if (r.value === null) throw new Error('preset not found')
      const template =
        r.value.system_prompt_i18n !== undefined && r.value.system_prompt_i18n !== '{}'
          ? r.value.system_prompt_i18n
          : r.value.system_prompt
      const rendered = await renderTemplate(template, deps.bus)
      return { template, rendered }
    },
    async getConfig(req) {
      const key = req.key
      const r = await Config.get(deps.bus, key)
      return { key, value: r.isOk() && r.value ? r.value : '' }
    },
    async setConfig(req) {
      const { key, value } = req
      const r = await Config.set(deps.bus, key, value)
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },
    async listTools(req) {
      const tools = await discoverTools(deps.bus)
      const configLocale = (await Config.get(deps.bus, 'locale')).unwrapOr(null)
      const locale = resolveLocale(
        req.locale,
        configLocale,
        process.env.ZERGX_LOCALE ?? 'en',
      )
      return create(ListToolsResponseSchema, {
        tools: tools.map(t => ({
          name: t.name,
          description: pickDescription(t.description, t.descriptions, locale),
          category: t.extId,
          parameters: (localizeSchema(t.inputSchema ?? null, locale) ?? {}) as Record<string, never>,
          configFields: (t.extConfig ?? []).map(c => ({
            name: c.name,
            type: c.type,
            enumValues: c.enum_values ?? [],
            description: c.description ?? '',
            scope: c.scope ?? 'global',
          })),
          requiredConfig: t.requiredConfig ?? [],
        })),
      })
    },
    async getToolConfig() {
      const value = await toolConfigMap(deps.bus)
      return { config: { values: value } }
    },
    async setToolConfig(req) {
      const config = req.config
      const r = await Config.set(deps.bus, 'tool_config', JSON.stringify(config ?? {}))
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },
    async setExtensionConfig(req) {
      const { extId, name, value } = req
      const agent = new AbcAgent(deps.bus)
      await agent.discover(500)
      await agent.setConfig(extId, name, value)
      return { ok: true }
    },
    async uploadFile(req) {
      const file = req.file
      const data = req.data
      if (!file?.code || data === undefined) throw new Error('code and data required')
      const bytes = new Uint8Array(Buffer.from(data, 'base64'))
      await deps.files.put(file.code, { code: file.code, name: file.name ?? '', mime: file.mime ?? 'application/octet-stream' } as any, bytes)
      await upsertFile(deps.bus, { code: file.code, name: file.name ?? '', mime: file.mime ?? 'application/octet-stream', sha256: getSha(bytes) } as any)
      return { ok: true, code: file.code }
    },
    async ingestFile(req) {
      const { data: raw, name, mime } = req
      if (raw === undefined) throw new Error('data required')
      const bytes = typeof raw === 'string' ? new Uint8Array(Buffer.from(raw, 'base64')) : new Uint8Array(raw)
      const record = await storeBytes(deps, bytes, name ?? 'artifact', mime ?? 'application/octet-stream', '')
      return { ok: true, code: record.code }
    },
    async getFile(req) {
      const code = req.code
      const row = await fileByCode(deps.bus, code)
      if (row.isErr()) throw new Error(row.error)
      if (row.value === null) throw new Error('file not found')
      const got = await deps.files.get(code)
      return {
        data: Buffer.from(got.data).toString('base64'),
        name: row.value.name ?? '',
        mime: row.value.mime ?? 'application/octet-stream',
      }
    },
    async getFileMeta(req) {
      const code = req.code
      const row = await fileByCode(deps.bus, code)
      if (row.isErr()) throw new Error(row.error)
      if (row.value === null) throw new Error('file not found')
      return { name: row.value.name ?? '', mime: row.value.mime ?? '', size: row.value.size ?? 0 }
    },
    async listWorksheets(req) {
      const id = req.id
      const r = await Worksheets.listBySession(deps.db, id)
      if (r.isErr()) throw new Error(r.error)
      return { worksheets: r.value.map(worksheetToMsg) }
    },
    async decideWorksheet(req) {
      const { id, wid, decision } = req
      const row = await Worksheets.get(deps.db, wid)
      if (row.isErr()) throw new Error(row.error)
      if (row.value === null || row.value.session_name !== id) throw new Error('worksheet not found')
      const claimed = await Worksheets.claimForDispatch(deps.db, wid)
      if (claimed.isErr()) throw new Error(claimed.error)
      if (claimed.value === null) throw new Error('worksheet is not pending')
      let args: Record<string, unknown> = {}
      try {
        const v = JSON.parse(row.value.args)
        if (typeof v === 'object' && v !== null) args = v as Record<string, unknown>
      } catch {
        args = {}
      }
      const err = await dispatchDecision(deps, wid, row.value.session_name, row.value.ext_id, row.value.action, args, decision as 'approve' | 'reject')
      if (err !== null && decision === 'approve') {
        await Worksheets.rollbackToPending(deps.db, wid)
        throw new Error(err)
      }
      return { ok: true }
    },
    async getZergxConfig() {
      const r = await Providers.list(deps.db)
      if (r.isErr()) throw new Error(r.error)
      const providers: Record<string, string> = {}
      for (const p of r.value) {
        if (p) providers[p.provider_id] = JSON.stringify(providerToMsg(p))
      }
      return create(GetZergxConfigResponseSchema, { config: { providers, http_proxy: process.env.ZERGX_HTTP_PROXY ?? '', self_base: process.env.ZERGX_SELF_BASE ?? '' } })
    },
  }

  router.service(AgentService, impl)

  const handlers = router.handlers
  const rpcFetch = async (req: Request): Promise<Response> => {
    const path = new URL(req.url).pathname
    const handler = handlers.find(h => h.requestPath === path)
    if (!handler) return new Response('not found', { status: 404 })
    const uReq = universalServerRequestFromFetch(req, {})
    const uRes = await handler(uReq)
    return universalServerResponseToFetch(uRes)
  }
  return rpcFetch
}

import { upsertFile, fileBySha, Worksheets, randomCode, type FileRecord } from '@zergx-agent/agent'
import { createHash } from 'node:crypto'

function getSha(data: Uint8Array): string {
  return createHash('sha256').update(data).digest('hex')
}

/** Dedup + store a single file. Shared by ingest/upload in the Connect surface. */
async function storeBytes(
  deps: AgentDeps,
  data: Uint8Array,
  name: string,
  mime: string,
  uploader: string,
): Promise<FileRecord> {
  const sha = getSha(data)
  const existing = await fileBySha(deps.bus, sha)
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
  await deps.files.put(code, record, data)
  await upsertFile(deps.bus, record)
  return record
}
