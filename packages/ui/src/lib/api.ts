import type { SessionJson } from '@easylab-agent/schema'
import { SessionRowSchema } from '@easylab-agent/schema'
import { createClient, type Transport } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { AgentService, type Session as AgentSession } from '@easylab/client-sdk/agent'
import { err, ok, type Result, ResultAsync } from 'neverthrow'
import { z } from 'zod'

/**
 * Type-safe Connect client over agent.v1.AgentService (same-origin with the
 * agent backend that serves this SPA). Message shapes are proto-derived; the
 * session row keeps the zod-validated UI contract. Live events use the
 * WatchSession server-streaming RPC (the SSE endpoint's replacement).
 *
 * Every network call returns a `neverthrow` Result — no try/catch/throw.
 */

const origin =
  typeof window !== 'undefined' ? window.location.origin : 'http://localhost'

const transport: Transport = createConnectTransport({ baseUrl: origin })

export const agent = createClient(AgentService, transport)

const sessionJsonSchema = SessionRowSchema

export type Session = z.infer<typeof sessionJsonSchema>

function decode<T>(
  schema: z.ZodType<T>,
  data: unknown,
  label: string,
): Result<T, string> {
  const r = schema.safeParse(data)
  return r.success ? ok(r.data) : err(`${label}: ${z.treeifyError(r.error)}`)
}

/** Run a unary RPC, mapping Connect errors into Result failures. */
function rpc<T>(
  op: () => Promise<T>,
  schema: z.ZodType<T>,
  label: string,
): ResultAsync<T, string> {
  const run = async (): Promise<Result<T, string>> => {
    let raw: T
    try {
      raw = await op()
    } catch (e) {
      return err<T, string>(`${label}: ${String(e)}`)
    }
    return decode(schema, raw, label)
  }
  const wrapped = ResultAsync.fromThrowable(
    async () => {
      const r = await run()
      if (r.isErr()) throw new Error(r.error)
      return r.value
    },
    e => `${label}: ${String(e)}`,
  )
  return wrapped()
}

/** proto Session → UI session row (snake_case zod contract). */
function sessionToRow(s: AgentSession): Record<string, unknown> {
  return {
    name: s.name,
    model: s.model,
    preset: s.preset,
    tip_id: s.tipId || null,
    max_turns: s.maxTurns,
    system_prompt: s.systemPrompt,
    input_tokens: s.inputTokens,
    output_tokens: s.outputTokens,
    total_tokens: s.totalTokens,
    last_input_tokens: s.lastInputTokens,
    last_output_tokens: s.lastOutputTokens,
    created_at: s.createdAt,
    updated_at: s.updatedAt,
    last_used_at: s.lastUsedAt || null,
    locale: s.locale || undefined,
  }
}

/** Concatenate a message's text parts into display content. */
function messageContent(parts: { type: string; data: string }[]): string {
  let out = ''
  for (const p of parts) {
    if (p.type !== 'text') continue
    try {
      const j = JSON.parse(p.data) as { text?: string }
      out += j.text ?? ''
    } catch {
      out += p.data
    }
  }
  return out
}

export const api = {
  listSessions: () =>
    rpc(
      async () => {
        const res = await agent.listSessions({})
        return { sessions: res.sessions.map(sessionToRow) }
      },
      z.object({ sessions: z.array(sessionJsonSchema) }),
      'list sessions',
    ).map(r => r.sessions),

  createSession: (body: { name: string; model?: string; preset?: string }) =>
    rpc(
      async () => {
        const res = await agent.createSession({
          name: body.name,
          model: body.model ?? '',
          preset: body.preset ?? '',
        })
        return { ok: res.ok, session_name: res.sessionName }
      },
      z.object({ ok: z.boolean(), session_name: z.string() }),
      'create session',
    ),

  listMessages: (sid: string) =>
    rpc(
      async () => {
        const res = await agent.listMessages({ id: sid, limit: 200 })
        return {
          messages: res.messages.map(m => ({
            id: m.id,
            role: m.role,
            content: messageContent(m.parts),
            created_at: m.createdAt,
          })),
        }
      },
      z.object({
        messages: z.array(
          z.object({
            id: z.string(),
            role: z.string(),
            content: z.string(),
            created_at: z.string(),
          }),
        ),
      }),
      'list messages',
    ).map(r => r.messages),

  compact: (sid: string) =>
    rpc(
      async () => {
        const res = await agent.compact({ id: sid })
        return { ok: res.ok }
      },
      z.object({ ok: z.boolean() }),
      'compact session',
    ),

  prompt: (sid: string, prompt: string) =>
    rpc(
      async () => {
        // Server-streaming prompt: consume until the accepted/first event.
        const stream = await agent.prompt({ id: sid, prompt })
        for await (const _ev of stream) {
          void _ev
          break // the first event (accepted) acknowledges the enqueue
        }
        return { ok: true }
      },
      z.object({ ok: z.boolean() }),
      'prompt',
    ),

  // ---- file upload / download (agent-native) ----

  uploadFile: async (file: File, uploaderSession: string) => {
    const buf = new Uint8Array(await file.arrayBuffer())
    // base64 without stack overflow
    let bin = ''
    const chunk = 0x8000
    for (let i = 0; i < buf.length; i += chunk) {
      bin += String.fromCharCode(...buf.subarray(i, i + chunk))
    }
    const data = btoa(bin)
    const res = await agent.uploadFile({
      file: { code: '', name: file.name, mime: file.type || 'application/octet-stream', size: buf.length },
      data,
    })
    if (!res.ok) {
      throw new Error('upload failed')
    }
    void uploaderSession
    return {
      code: res.code,
      name: file.name,
      mime: file.type || 'application/octet-stream',
      size: buf.length,
      sha256: '',
    }
  },

  fileUrl: (code: string) => `${origin}/api/v1/files/${code}`,

  interrupt: (sid: string) =>
    rpc(
      async () => {
        const res = await agent.interrupt({ id: sid })
        return { interrupted: res.interrupted }
      },
      z.object({ interrupted: z.boolean() }),
      'interrupt',
    ),

  /** Live session events (Connect WatchSession; SSE replacement). */
  watchSession: (
    sid: string,
    options?: { signal?: AbortSignal },
  ) => agent.watchSession({ id: sid }, options),

  listProviders: () =>
    rpc(
      async () => {
        const res = await agent.listProviders({})
        const providers: Record<string, unknown> = {}
        for (const p of res.providers) {
          providers[p.providerId] = {
            provider_id: p.providerId,
            api_type: p.apiType,
            base_url: p.baseUrl,
            api_key: p.apiKey,
            headers: p.headers,
            models: p.models.map(id => ({ id, name: id })),
          }
        }
        return { providers }
      },
      z.object({ providers: z.record(z.string(), z.unknown()) }),
      'list providers',
    ).map(r => r.providers as Record<string, {
      provider_id: string
      api_type: string
      base_url: string
      api_key: string
      headers: Record<string, string>
      models: { id: string; name: string }[]
    }>),

  registerProvider: (p: {
    provider_id: string
    api_type: string
    base_url: string
    api_key?: string
    models?: string[]
  }) =>
    rpc(
      async () => {
        const res = await agent.registerProvider({
          provider: {
            providerId: p.provider_id,
            apiType: p.api_type,
            baseUrl: p.base_url,
            apiKey: p.api_key ?? '',
            models: p.models ?? [],
          },
        })
        return { ok: res.ok, provider_id: p.provider_id }
      },
      z.object({ ok: z.boolean(), provider_id: z.string() }),
      'register provider',
    ),

  deleteProvider: (pid: string) =>
    rpc(
      async () => {
        await agent.deleteProvider({ providerId: pid })
        return { ok: true }
      },
      z.object({ ok: z.boolean() }),
      'delete provider',
    ),

  testProvider: (p: { api_type: string; base_url: string; api_key?: string }) =>
    rpc(
      async () => {
        const res = await agent.testProvider({
          apiType: p.api_type,
          baseUrl: p.base_url,
          apiKey: p.api_key ?? '',
        })
        return { ok: res.ok, result: res.result }
      },
      z.object({
        ok: z.boolean(),
        models: z.unknown().optional(),
        error: z.string().optional(),
      }).passthrough(),
      'test provider',
    ),

  listModels: () =>
    rpc(
      async () => {
        const res = await agent.listModels({})
        return { models: res.models.map(m => m.id) }
      },
      z.object({ models: z.array(z.string()) }),
      'list models',
    ).map(r => r.models),

  setSessionModel: (sid: string, model: string) =>
    rpc(
      async () => {
        await agent.setModel({ id: sid, model })
        return { model }
      },
      z.object({ model: z.string() }),
      'set model',
    ),

  updateSettings: (sid: string, patch: { model?: string; preset?: string; locale?: string }) =>
    rpc(
      async () => {
        const res = await agent.updateSettings({
          id: sid,
          model: patch.model ?? '',
          preset: patch.preset ?? '',
          locale: patch.locale ?? '',
        })
        if (!res.session) throw new Error('session not found')
        return { session: sessionToRow(res.session) }
      },
      z.object({ session: sessionJsonSchema }),
      'update settings',
    ),

  listPresets: () =>
    rpc(
      async () => {
        const res = await agent.listPresets({})
        return {
          presets: res.presets.map(p => ({
            id: p.id,
            system_prompt: p.systemPrompt,
            tools: JSON.stringify(p.tools ?? []),
            max_turns: p.maxTurns,
            is_system: p.isSystem,
          })),
        }
      },
      z.object({
        presets: z.array(
          z.object({
            id: z.string(),
            system_prompt: z.string(),
            tools: z.string(),
            max_turns: z.number(),
            is_system: z.boolean().optional(),
          }),
        ),
      }),
      'list presets',
    ).map(r => r.presets),

  upsertPreset: (p: {
    id: string
    system_prompt?: string
    tools?: unknown
    max_turns?: number
  }) =>
    rpc(
      async () => {
        let tools: string[] = []
        if (Array.isArray(p.tools)) tools = p.tools as string[]
        const res = await agent.upsertPreset({
          preset: {
            id: p.id,
            systemPrompt: p.system_prompt ?? '',
            tools,
            maxTurns: p.max_turns ?? 0,
          },
        })
        return { ok: res.ok }
      },
      z.object({ ok: z.boolean() }),
      'upsert preset',
    ),

  deletePreset: (id: string) =>
    rpc(
      async () => {
        const res = await agent.deletePreset({ id })
        return { ok: res.ok }
      },
      z.object({ ok: z.boolean() }),
      'delete preset',
    ),

  previewPreset: (id: string) =>
    rpc(
      async () => {
        const res = await agent.previewPreset({ id })
        return { template: res.template, rendered: res.rendered }
      },
      z.object({ template: z.string(), rendered: z.string() }),
      'preview preset',
    ),

  listMailbox: (sid: string) =>
    rpc(
      async () => {
        const res = await agent.mailbox({ id: sid })
        return { entries: res.mailbox as unknown[] }
      },
      z.object({ entries: z.array(z.unknown()) }),
      'list mailbox',
    ).map(r => r.entries),

  fork: (sid: string, name: string) =>
    rpc(
      async () => {
        await agent.fork({ id: sid, name })
        return { ok: true, session_name: name }
      },
      z.object({ ok: z.boolean(), session_name: z.string() }),
      'fork',
    ),

  rename: (sid: string, name: string) =>
    rpc(
      async () => {
        await agent.rename({ id: sid, name })
        return { ok: true, session_name: name }
      },
      z.object({ ok: z.boolean(), session_name: z.string() }),
      'rename',
    ),

  undo: (sid: string, messageId?: string) =>
    rpc(
      async () => {
        await agent.undo({ id: sid, messageId: messageId ?? '' })
        return { ok: true, undone: true }
      },
      z.object({ ok: z.boolean(), undone: z.boolean() }),
      'undo',
    ),

  catalogProviders: () =>
    rpc(
      async () => {
        const res = await agent.listProvidersCatalog({})
        return { catalog: res.providers as unknown }
      },
      z.object({ catalog: z.record(z.string(), z.unknown()) }),
      'catalog providers',
    ).map(r => r.catalog as Record<string, unknown>),
}

export type { SessionJson }
