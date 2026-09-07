import { Agent as AbcAgent } from '@abc-protocol/sdk'
import { createRoute, OpenAPIHono } from '@hono/zod-openapi'
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
import type { AppEnv } from '../context.js'

const ModelsArraySchema = z.array(z.string())
const HeadersRecordSchema = z.record(z.string(), z.unknown())

const ErrorSchema = z.object({ ok: z.boolean(), error: z.string() })

// ---- presets ----

const listPresetsRoute = createRoute({
  method: 'get',
  path: '/presets',
  summary: 'List presets',
  responses: {
    200: {
      description: 'Preset rows',
      content: {
        'application/json': {
          schema: z.array(
            z.object({
              id: z.string(),
              system_prompt: z.string(),
              tools: z.array(z.string()),
              max_turns: z.number(),
            }),
          ),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const upsertPresetRoute = createRoute({
  method: 'post',
  path: '/presets',
  summary: 'Create or update a preset',
  request: {
    body: { content: { 'application/json': { schema: PresetBodySchema } } },
  },
  responses: {
    200: {
      description: 'Ok',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    409: {
      description: 'System preset is read-only',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const deletePresetRoute = createRoute({
  method: 'delete',
  path: '/presets/{id}',
  summary: 'Delete a preset',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Ok',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    409: {
      description: 'System preset is read-only',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const previewPresetRoute = createRoute({
  method: 'get',
  path: '/presets/{id}/preview',
  summary: 'Preview a preset system prompt with template variables rendered',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Rendered prompt',
      content: {
        'application/json': {
          schema: z.object({
            template: z.string(),
            rendered: z.string(),
          }),
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

// ---- generic config ----

const getConfigRoute = createRoute({
  method: 'get',
  path: '/config',
  summary: 'Get providers config (compat)',
  responses: {
    200: {
      description: 'Providers',
      content: {
        'application/json': { schema: z.object({ providers: z.unknown() }) },
      },
    },
  },
})

const getConfigKeyRoute = createRoute({
  method: 'get',
  path: '/config/{key}',
  summary: 'Get a single config value',
  request: { params: z.object({ key: z.string() }) },
  responses: {
    200: {
      description: 'Config value',
      content: {
        'application/json': {
          schema: z.object({ key: z.string(), value: z.string() }),
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

const putConfigRoute = createRoute({
  method: 'put',
  path: '/config',
  summary: 'Set a config value',
  request: {
    body: { content: { 'application/json': { schema: ConfigBodySchema } } },
  },
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

// ---- tool config ----

const getToolConfigRoute = createRoute({
  method: 'get',
  path: '/tool-config',
  summary: 'Get tool config',
  responses: {
    200: {
      description: 'Tool config',
      content: { 'application/json': { schema: z.unknown() } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const putToolConfigRoute = createRoute({
  method: 'put',
  path: '/tool-config',
  summary: 'Set tool config',
  responses: {
    200: {
      description: 'Updated config',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), config: z.unknown() }),
        },
      },
    },
    400: {
      description: 'Bad request',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

// Set an extension config knob by id (e.g. memory / vlm_model). Delivers the
// validated change to the extension's config store (abc.config.<extId> + cfg
// KV) so tools like image-read pick the model up immediately.
const setExtensionConfigRoute = createRoute({
  method: 'put',
  path: '/tool-config/{extId}/{name}',
  summary: 'Set an extension config value',
  request: {
    params: z.object({ extId: z.string(), name: z.string() }),
    body: {
      content: {
        'application/json': { schema: z.object({ value: z.unknown() }) },
      },
    },
  },
  responses: {
    200: {
      description: 'Ok',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    400: {
      description: 'Bad request',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    404: {
      description: 'No manifest',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

// ---- tools ----

const listToolsRoute = createRoute({
  method: 'get',
  path: '/tools',
  summary: 'Discover tools',
  request: {
    query: z.object({ locale: z.string().optional() }),
  },
  responses: {
    200: {
      description: 'Tools',
      content: {
        'application/json': {
          schema: z.object({
            tools: z.array(
              z.object({
                name: z.string(),
                description: z.string(),
                category: z.string(),
                parameters: z.record(z.string(), z.unknown()).nullable(),
                configFields: z.array(z.unknown()).nullable(),
              }),
            ),
          }),
        },
      },
    },
  },
})

// ---- models ----

const listModelsRoute = createRoute({
  method: 'get',
  path: '/models',
  summary: 'List available models',
  responses: {
    200: {
      description: 'Models',
      content: {
        'application/json': {
          schema: z.object({
            models: z.array(z.object({ id: z.string(), name: z.string() })),
          }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

// ---- agent config ----

const agentConfigRoute = createRoute({
  method: 'get',
  path: '/agent-config',
  summary: 'Rucoder config',
  responses: {
    200: {
      description: 'Rucoder config',
      content: { 'application/json': { schema: z.unknown() } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

export const configRoutes = new OpenAPIHono<AppEnv>()
  .openapi(listPresetsRoute, async c => {
    const { bus } = c.get('deps')
    const r = await Presets.list(bus)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    return c.json(
      r.value.map(p => ({
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
  .openapi(upsertPresetRoute, async c => {
    const { bus } = c.get('deps')
    const b = c.req.valid('json')
    const r = await Presets.upsert(bus, {
      id: b.id,
      systemPrompt: b.system_prompt ?? '',
      systemPromptI18n:
        typeof b.system_prompt_i18n === 'string'
          ? b.system_prompt_i18n
          : JSON.stringify(b.system_prompt_i18n ?? {}),
      tools: JSON.stringify(b.tools ?? []),
      maxTurns: b.max_turns ?? 0,
    })
    if (r.isErr()) {
      // The only expected rejection is a read-only system preset.
      const msg = String(r.error)
      const isSystem = msg.includes('is immutable')
      return c.json(
        { ok: false, error: isSystem ? 'system preset is read-only' : msg },
        isSystem ? 409 : 500,
      )
    }
    return c.json({ ok: true }, 200)
  })
  .openapi(deletePresetRoute, async c => {
    const { bus } = c.get('deps')
    const r = await Presets.delete(bus, c.req.valid('param').id)
    if (r.isErr()) {
      const msg = String(r.error)
      const isSystem = msg.includes('is immutable')
      return c.json(
        { ok: false, error: isSystem ? 'system preset is read-only' : msg },
        isSystem ? 409 : 500,
      )
    }
    return c.json({ ok: true }, 200)
  })
  .openapi(previewPresetRoute, async c => {
    const { bus } = c.get('deps')
    const id = c.req.valid('param').id
    const r = await Presets.get(bus, id)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    if (r.value === null) {
      return c.json({ ok: false, error: 'preset not found' }, 404)
    }
    const template =
      r.value.system_prompt_i18n !== undefined &&
      r.value.system_prompt_i18n !== '{}'
        ? r.value.system_prompt_i18n
        : r.value.system_prompt
    const rendered = await renderTemplate(template, bus)
    return c.json({ template, rendered }, 200)
  })
  .openapi(getConfigRoute, async c => {
    const { bus } = c.get('deps')
    const r = await Config.get(bus, 'providers')
    // Providers live in their own table now; expose them for UI compatibility.
    const providers =
      r.isOk() && r.value !== null
        ? parse(z.unknown(), r.value).unwrapOr({})
        : {}
    return c.json({ providers }, 200)
  })
  .openapi(getConfigKeyRoute, async c => {
    const { bus } = c.get('deps')
    const { key } = c.req.valid('param')
    const r = await Config.get(bus, key)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    return r.value === null
      ? c.json({ ok: false, error: 'config not found' }, 404)
      : c.json({ key, value: r.value }, 200)
  })
  .openapi(putConfigRoute, async c => {
    const { bus } = c.get('deps')
    const b = c.req.valid('json')
    const r = await Config.set(bus, b.key, b.value)
    return r.isErr()
      ? c.json({ ok: false, error: r.error }, 500)
      : c.json({ ok: true }, 200)
  })
  .openapi(getToolConfigRoute, async c => {
    const deps = c.get('deps')
    // Aggregate from the `cfg` KV bucket (the store backing per-knob PUTs),
    // so a saved value is immediately visible here and the UI's badge/seed
    // reflect the real applied config.
    const value = await toolConfigMap(deps.bus)
    return c.json(value, 200)
  })
  .openapi(putToolConfigRoute, async c => {
    const body = await ResultAsync.fromPromise(c.req.json(), () => null)
    if (body.isErr() || body.value === null) {
      return c.json({ ok: false, error: 'invalid json body' }, 400)
    }
    const { bus } = c.get('deps')
    const r = await Config.set(bus, 'tool_config', JSON.stringify(body.value))
    return r.isErr()
      ? c.json({ ok: false, error: r.error }, 500)
      : c.json({ ok: true, config: body.value }, 200)
  })
  .openapi(setExtensionConfigRoute, async c => {
    const deps = c.get('deps')
    const { extId, name } = c.req.valid('param')
    const body = c.req.valid('json')
    const agent = new AbcAgent(deps.bus)
    try {
      // Discover keeps the manifest cache warm; SetConfig validates against
      // the extension's declared config knobs and persists cfg KV + delivers
      // to the live extension. fail if the extension is unknown.
      await agent.discover(500)
      await agent.setConfig(extId, name, body.value)
      return c.json({ ok: true }, 200)
    } catch (e) {
      const code = (e as { code?: string })?.code
      if (code === 'not_found') {
        return c.json({ ok: false, error: 'no manifest for ' + extId }, 404)
      }
      if (code === 'invalid_argument') {
        return c.json({ ok: false, error: (e as Error).message }, 400)
      }
      return c.json({ ok: false, error: (e as Error).message }, 500)
    }
  })
  .openapi(listToolsRoute, async c => {
    const deps = c.get('deps')
    const tools = await discoverTools(deps.bus)
    // Localize for the request — exact → primary-language → default. The
    // agent's own turn uses session → KV config → env; the /tools surface has
    // no session context, so it falls back to KV config → env and an optional
    // `?locale=` query override. Tool-level and per-property descriptions are
    // resolved; the non-standard `descriptions` keys are stripped so the
    // consumer only sees standard JSON-Schema fields.
    const configLocale = (await Config.get(deps.bus, 'locale')).unwrapOr(null)
    const locale = resolveLocale(
      c.req.query('locale'),
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
            (t.extConfig ?? []).map(c => ({
              name: c.name,
              type: c.type,
              enum_values: c.enum_values ?? [],
              default: c.default,
              description: c.description,
              scope: c.scope ?? 'global',
            })) || null,
          required_config: t.requiredConfig ?? [],
        })),
      },
      200,
    )
  })
  .openapi(listModelsRoute, async c => {
    const { db, llm } = c.get('deps')
    const r = await Providers.list(db)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    const models: string[] = []
    for (const p of r.value) {
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
  .openapi(agentConfigRoute, async c => {
    const deps = c.get('deps')
    const r = await Providers.list(deps.db)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    const providers: Record<string, unknown> = {}
    for (const p of r.value) {
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
