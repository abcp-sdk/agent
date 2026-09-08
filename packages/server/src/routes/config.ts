import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  Config,
  discoverTools,
  localizeSchema,
  Presets,
  Providers,
  parse,
  pickDescription,
  renderTemplate,
  resolveLocale,
  toolConfigMap,
} from '@easylab-agent/agent'
import { ConfigBodySchema, PresetBodySchema } from '@easylab-agent/schema'
import { ResultAsync } from 'neverthrow'
import { z } from 'zod'
import { type Ctx, type Router } from '../http.js'

const ModelsArraySchema = z.array(z.string())
const HeadersRecordSchema = z.record(z.string(), z.unknown())

const ExtensionConfigValueSchema = z.object({ value: z.unknown() })

/** Narrow a caught error to its optional string `code` (abc error codes). */
function errorCode(e: unknown): string | undefined {
  if (typeof e !== 'object' || e === null || !('code' in e)) return undefined
  const v: unknown = e.code
  return typeof v === 'string' ? v : undefined
}

export function configRoutes(r: Router): void {
  // ---- presets ----

  r.get('/presets', async c => {
    const { bus } = c.deps
    const res = await Presets.list(bus)
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    return c.json(
      res.value.map(p => ({
        id: p.id,
        system_prompt: p.system_prompt,
        system_prompt_i18n: p.system_prompt_i18n ?? '{}',
        tools: parse(z.array(z.string()), p.tools).unwrapOr([]),
        max_turns: p.max_turns,
        is_system: p.is_system ?? false,
      })),
      200,
    )
  })

  r.post(
    '/presets',
    async c => {
      const { bus } = c.deps
      const b = c.body
      const res = await Presets.upsert(bus, {
        id: b.id,
        systemPrompt: b.system_prompt ?? '',
        systemPromptI18n:
          typeof b.system_prompt_i18n === 'string'
            ? b.system_prompt_i18n
            : JSON.stringify(b.system_prompt_i18n ?? {}),
        tools: JSON.stringify(b.tools ?? []),
        maxTurns: b.max_turns ?? 0,
      })
      if (res.isErr()) {
        // The only expected rejection is a read-only system preset.
        const msg = String(res.error)
        const isSystem = msg.includes('is immutable')
        return c.json(
          { ok: false, error: isSystem ? 'system preset is read-only' : msg },
          isSystem ? 409 : 500,
        )
      }
      return c.json({ ok: true }, 200)
    },
    PresetBodySchema,
  )

  r.delete('/presets/:id', async c => {
    const { bus } = c.deps
    const res = await Presets.delete(bus, c.req.params['id'] ?? '')
    if (res.isErr()) {
      const msg = String(res.error)
      const isSystem = msg.includes('is immutable')
      return c.json(
        { ok: false, error: isSystem ? 'system preset is read-only' : msg },
        isSystem ? 409 : 500,
      )
    }
    return c.json({ ok: true }, 200)
  })

  r.get('/presets/:id/preview', async c => {
    const { bus } = c.deps
    const id = c.req.params['id'] ?? ''
    const res = await Presets.get(bus, id)
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    if (res.value === null) {
      return c.json({ ok: false, error: 'preset not found' }, 404)
    }
    const template =
      res.value.system_prompt_i18n !== undefined &&
      res.value.system_prompt_i18n !== '{}'
        ? res.value.system_prompt_i18n
        : res.value.system_prompt
    const rendered = await renderTemplate(template, bus)
    return c.json({ template, rendered }, 200)
  })

  // ---- generic config ----

  r.get('/config', async c => {
    const { bus } = c.deps
    const res = await Config.get(bus, 'providers')
    // Providers live in their own table now; expose them for UI compatibility.
    const providers =
      res.isOk() && res.value !== null
        ? parse(z.unknown(), res.value).unwrapOr({})
        : {}
    return c.json({ providers }, 200)
  })

  r.get('/config/:key', async c => {
    const { bus } = c.deps
    const key = c.req.params['key'] ?? ''
    const res = await Config.get(bus, key)
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    return res.value === null
      ? c.json({ ok: false, error: 'config not found' }, 404)
      : c.json({ key, value: res.value }, 200)
  })

  r.put(
    '/config',
    async c => {
      const { bus } = c.deps
      const b = c.body
      const res = await Config.set(bus, b.key, b.value)
      return res.isErr()
        ? c.json({ ok: false, error: res.error }, 500)
        : c.json({ ok: true }, 200)
    },
    ConfigBodySchema,
  )

  // ---- tool config ----

  r.get('/tool-config', async c => {
    const deps = c.deps
    // Aggregate from the `cfg` KV bucket (the store backing per-knob PUTs),
    // so a saved value is immediately visible here and the UI's badge/seed
    // reflect the real applied config.
    const value = await toolConfigMap(deps.bus)
    return c.json(value, 200)
  })

  r.put('/tool-config', async c => {
    const body = await ResultAsync.fromPromise(c.req.raw.json(), () => null)
    if (body.isErr() || body.value === null) {
      return c.json({ ok: false, error: 'invalid json body' }, 400)
    }
    const { bus } = c.deps
    const res = await Config.set(bus, 'tool_config', JSON.stringify(body.value))
    return res.isErr()
      ? c.json({ ok: false, error: res.error }, 500)
      : c.json({ ok: true, config: body.value }, 200)
  })

  // Set an extension config knob by id (e.g. memory / vlm_model). Delivers the
  // validated change to the extension's config store (abc.config.<extId> + cfg
  // KV) so tools like image-read pick the model up immediately.
  r.put(
    '/tool-config/:extId/:name',
    async c => {
      const deps = c.deps
      const extId = c.req.params['extId'] ?? ''
      const name = c.req.params['name'] ?? ''
      const body = c.body
      const agent = new AbcAgent(deps.bus)
      try {
        // Discover keeps the manifest cache warm; SetConfig validates against
        // the extension's declared config knobs and persists cfg KV + delivers
        // to the live extension. fail if the extension is unknown.
        await agent.discover(500)
        await agent.setConfig(extId, name, body.value)
        return c.json({ ok: true }, 200)
      } catch (e) {
        const code = errorCode(e)
        if (code === 'not_found') {
          return c.json({ ok: false, error: 'no manifest for ' + extId }, 404)
        }
        if (code === 'invalid_argument') {
          return c.json({ ok: false, error: (e as Error).message }, 400)
        }
        return c.json({ ok: false, error: (e as Error).message }, 500)
      }
    },
    ExtensionConfigValueSchema,
  )

  // ---- tools ----

  r.get('/tools', async c => {
    const deps = c.deps
    const tools = await discoverTools(deps.bus)
    // Localize for the request — exact → primary-language → default. The
    // agent's own turn uses session → KV config → env; the /tools surface has
    // no session context, so it falls back to KV config → env and an optional
    // `?locale=` query override. Tool-level and per-property descriptions are
    // resolved; the non-standard `descriptions` keys are stripped so the
    // consumer only sees standard JSON-Schema fields.
    const configLocale = (await Config.get(deps.bus, 'locale')).unwrapOr(null)
    const locale = resolveLocale(
      c.req.query.get('locale') ?? undefined,
      configLocale,
      process.env.LOCALE ?? 'en',
    )
    return c.json(
      {
        tools: tools.map(t => ({
          name: t.name,
          description: pickDescription(t.description, t.descriptions, locale),
          category: t.extId,
          parameters: localizeSchema(t.inputSchema ?? null, locale),
          configFields: null,
          config:
            (t.extConfig ?? []).map(cf => ({
              name: cf.name,
              type: cf.type,
              enum_values: cf.enum_values ?? [],
              default: cf.default,
              description: cf.description,
              scope: cf.scope ?? 'global',
            })) || null,
          required_config: t.requiredConfig ?? [],
        })),
      },
      200,
    )
  })

  // ---- models ----

  r.get('/models', async c => {
    const { db, llm } = c.deps
    const res = await Providers.list(db)
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    const models: string[] = []
    for (const p of res.value) {
      const arr = parse(ModelsArraySchema, p.models)
      if (arr.isOk()) models.push(...arr.value)
    }
    // Only surface an env-configured default model when the operator actually
    // set one. With no configured provider/model, list exactly what the
    // registered providers advertise — never a synthetic default.
    const defaultModel = llm.defaultModelId()
    if (defaultModel !== '' && !models.includes(defaultModel))
      models.unshift(defaultModel)
    return c.json({ models: models.map(id => ({ id, name: id })) }, 200)
  })

  // ---- agent config ----

  r.get('/agent-config', async (c: Ctx) => {
    const deps = c.deps
    const res = await Providers.list(deps.db)
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    const providers: Record<string, unknown> = {}
    for (const p of res.value) {
      providers[p.provider_id] = {
        provider_id: p.provider_id,
        api_type: p.api_type,
        base_url: p.base_url,
        api_key: p.api_key,
        headers: parse(HeadersRecordSchema, p.headers).unwrapOr({}),
        models: parse(ModelsArraySchema, p.models).unwrapOr([]),
      }
    }
    return c.json(
      {
        providers,
        http_proxy: process.env.AGENT_HTTP_PROXY ?? '',
        self_base: process.env.SELF_BASE ?? '',
      },
      200,
    )
  })
}
