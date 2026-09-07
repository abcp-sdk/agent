import type { CreateSessionBody, SessionJson } from '@easylab-agent/schema'
import {
  CatalogProvidersSchema,
  ProviderJsonSchema,
  SessionRowSchema,
} from '@easylab-agent/schema'
import type { AppType } from '@easylab-agent/server'
import { hc } from 'hono/client'
import { err, ok, type Result, ResultAsync } from 'neverthrow'
import { z } from 'zod'

/**
 * Type-safe Hono RPC client. The client type is derived from the real server
 * router (`AppType = typeof app` in @easylab-agent/server) so paths, `:id`
 * params, request bodies and response shapes are all inferred. Every response
 * body is additionally validated at runtime against the shared zod schemas.
 *
 * Every network call returns a `neverthrow` Result — no try/catch/throw.
 */

const origin =
  typeof window !== 'undefined' ? window.location.origin : 'http://localhost'

export const client = hc<AppType>(`${origin}/api/v1`)

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

function request<T>(
  op: () => Promise<Response>,
  schema: z.ZodType<T>,
  label: string,
): ResultAsync<T, string> {
  const parseBody = (res: Response): Promise<unknown> =>
    res.json().then(
      v => v,
      () => null,
    )

  const run = async (): Promise<Result<T, string>> => {
    const res = await op()
    const body: unknown = await parseBody(res)
    if (!res.ok) {
      const msg = z
        .object({ error: z.string().optional() })
        .parse(body ?? {}).error
      return err<T, string>(msg ?? `${label} failed (HTTP ${res.status})`)
    }
    return decode(schema, body, label)
  }

  return ResultAsync.fromPromise(
    run().then(r => (r.isOk() ? r.value : Promise.reject(new Error(r.error)))),
    e => `${label}: ${String(e)}`,
  )
}

export const api = {
  listSessions: () =>
    request(
      () => client.sessions.$get(),
      z.object({ sessions: z.array(sessionJsonSchema) }),
      'list sessions',
    ).map(r => r.sessions),

  createSession: (body: CreateSessionBody) =>
    request(
      () => client.sessions.$post({ json: body }),
      z.object({ ok: z.boolean(), session_name: z.string() }),
      'create session',
    ),

  listMessages: (sid: string) =>
    request(
      () =>
        client.sessions[':id'].messages.$get({ param: { id: sid }, query: {} }),
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
    request(
      () => client.sessions[':id'].compact.$post({ param: { id: sid } }),
      z.object({ ok: z.boolean() }),
      'compact session',
    ),

  prompt: (sid: string, prompt: string) =>
    request(
      () =>
        client.sessions[':id'].prompt.$post({
          json: { prompt },
          param: { id: sid },
        }),
      z.object({ ok: z.boolean() }),
      'prompt',
    ),

  // ---- file upload / download (agent-native) ----

  uploadFile: async (file: File, uploaderSession: string) => {
    const fd = new FormData()
    fd.append('file', file)
    if (uploaderSession !== '') fd.append('uploader_session', uploaderSession)
    const res = await fetch(`${origin}/api/v1/files`, {
      method: 'POST',
      body: fd,
    })
    if (!res.ok) {
      throw new Error(`upload failed (HTTP ${res.status})`)
    }
    return (await res.json()) as {
      code: string
      name: string
      mime: string
      size: number
      sha256: string
    }
  },

  fileUrl: (code: string) => `${origin}/api/v1/files/${code}`,

  interrupt: (sid: string) =>
    request(
      () => client.sessions[':id'].interrupt.$post({ param: { id: sid } }),
      z.object({ interrupted: z.boolean() }),
      'interrupt',
    ),

  sessionsStreamUrl: (sid: string) =>
    client.sessions[':id'].stream.$url({ param: { id: sid } }).toString(),

  listProviders: () =>
    request(
      () => client.providers.$get(),
      z.object({ providers: z.record(z.string(), ProviderJsonSchema) }),
      'list providers',
    ).map(r => r.providers),

  registerProvider: (p: {
    provider_id: string
    api_type: string
    base_url: string
    api_key?: string
    models?: string[]
  }) =>
    request(
      () => client.providers.$post({ json: p }),
      z.object({ ok: z.boolean(), provider_id: z.string() }),
      'register provider',
    ),

  deleteProvider: (pid: string) =>
    request(
      () => client.providers[':id'].$delete({ param: { id: pid } }),
      z
        .object({ deleted: z.boolean().optional() })
        .or(z.object({ ok: z.boolean() })),
      'delete provider',
    ),

  testProvider: (p: { api_type: string; base_url: string; api_key?: string }) =>
    request(
      () => client.providers.test.$post({ json: p }),
      z.object({
        ok: z.boolean(),
        models: z.unknown().optional(),
        error: z.string().optional(),
      }),
      'test provider',
    ),

  listModels: () =>
    request(
      () => client.models.$get(),
      z.object({ models: z.array(z.string()) }),
      'list models',
    ).map(r => r.models),

  setSessionModel: (sid: string, model: string) =>
    request(
      () =>
        client.sessions[':id'].model.$post({
          json: { model },
          param: { id: sid },
        }),
      z.object({ model: z.string() }),
      'set model',
    ),

  updateSettings: (sid: string, patch: { model?: string; preset?: string; locale?: string }) =>
    request(
      () =>
        client.sessions[':id'].settings.$patch({
          json: patch,
          param: { id: sid },
        }),
      z.object({ session: sessionJsonSchema }),
      'update settings',
    ),

  listPresets: () =>
    request(
      () => client.presets.$get(),
      z.array(
        z.object({
          id: z.string(),
          system_prompt: z.string(),
          tools: z.string(),
          max_turns: z.number(),
          is_system: z.boolean().optional(),
        }),
      ),
      'list presets',
    ),

  upsertPreset: (p: {
    id: string
    system_prompt?: string
    tools?: unknown
    max_turns?: number
  }) =>
    request(
      () => client.presets.$post({ json: p }),
      z.object({ ok: z.boolean() }),
      'upsert preset',
    ),

  deletePreset: (id: string) =>
    request(
      () => client.presets[':id'].$delete({ param: { id } }),
      z.object({ ok: z.boolean() }),
      'delete preset',
    ),

  previewPreset: (id: string) =>
    request(
      () => client.presets[':id'].preview.$get({ param: { id } }),
      z.object({ template: z.string(), rendered: z.string() }),
      'preview preset',
    ),

  listMailbox: (sid: string) =>
    request(
      () => client.sessions[':id'].mailbox.$get({ param: { id: sid } }),
      z.object({ entries: z.array(z.unknown()) }),
      'list mailbox',
    ).map(r => r.entries),

  fork: (sid: string, name: string) =>
    request(
      () =>
        client.sessions[':id'].fork.$post({
          json: { name },
          param: { id: sid },
        }),
      z.object({ ok: z.boolean(), session_name: z.string() }),
      'fork',
    ),

  rename: (sid: string, name: string) =>
    request(
      () =>
        client.sessions[':id'].rename.$post({
          json: { name },
          param: { id: sid },
        }),
      z.object({ ok: z.boolean(), session_name: z.string() }),
      'rename',
    ),

  undo: (sid: string, messageId?: string) =>
    request(
      () =>
        client.sessions[':id'].undo.$post({
          json: messageId ? { message_id: messageId } : {},
          param: { id: sid },
        }),
      z.object({ ok: z.boolean(), undone: z.boolean() }),
      'undo',
    ),

  catalogProviders: () =>
    request(
      () => client.providers.catalog.$get(),
      z.object({ catalog: CatalogProvidersSchema }),
      'catalog providers',
    ).map(r => r.catalog),
}

export type { SessionJson }
