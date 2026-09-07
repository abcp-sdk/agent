import { createRoute, OpenAPIHono } from '@hono/zod-openapi'
import {
  buildModelForApiType,
  getModelsDev,
  Providers,
  parse,
  validateApiType,
} from '@easylab-agent/agent'
import { ProviderBodySchema, ProviderTestBodySchema } from '@easylab-agent/schema'
import { generateText } from 'ai'
import { ResultAsync } from 'neverthrow'
import { z } from 'zod'
import type { AppEnv } from '../context.js'

const HeadersRecordSchema = z.record(z.string(), z.unknown())
const ModelsArraySchema = z.array(z.string())

const ErrorSchema = z.object({ ok: z.boolean(), error: z.string() })

function providerToJson(p: {
  provider_id: string
  api_type: string
  base_url: string
  api_key: string
  headers: string
  models: string
}): Record<string, unknown> {
  const headers = parse(HeadersRecordSchema, p.headers)
  const models = parse(ModelsArraySchema, p.models)
  return {
    provider_id: p.provider_id,
    api_type: p.api_type,
    base_url: p.base_url,
    api_key: p.api_key,
    headers: headers.isOk() ? headers.value : {},
    models: (models.isOk() ? models.value : []).map(id => ({ id, name: id })),
  }
}

const listProvidersRoute = createRoute({
  method: 'get',
  path: '/providers',
  summary: 'List registered providers',
  responses: {
    200: {
      description: 'Providers',
      content: {
        'application/json': {
          schema: z.object({ providers: z.record(z.string(), z.unknown()) }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const getCatalogRoute = createRoute({
  method: 'get',
  path: '/providers/catalog',
  summary: 'Prefilled provider catalog from models.dev cache',
  responses: {
    200: {
      description: 'Catalog',
      content: {
        'application/json': { schema: z.object({ catalog: z.unknown() }) },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const registerProviderRoute = createRoute({
  method: 'post',
  path: '/providers',
  summary: 'Register or update a provider',
  request: {
    body: { content: { 'application/json': { schema: ProviderBodySchema } } },
  },
  responses: {
    200: {
      description: 'Registered',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), provider_id: z.string() }),
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

const deleteProviderRoute = createRoute({
  method: 'delete',
  path: '/providers/{id}',
  summary: 'Delete a provider',
  request: { params: z.object({ id: z.string() }) },
  responses: {
    200: {
      description: 'Deleted',
      content: {
        'application/json': { schema: z.object({ deleted: z.boolean() }) },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const testProviderRoute = createRoute({
  method: 'post',
  path: '/providers/test',
  summary: 'Test a provider by generating text with a model',
  request: {
    body: {
      content: { 'application/json': { schema: ProviderTestBodySchema } },
    },
  },
  responses: {
    200: {
      description: 'Test result',
      content: {
        'application/json': {
          schema: z.object({
            ok: z.boolean(),
            models: z.unknown().optional(),
            error: z.string().optional(),
          }),
        },
      },
    },
  },
})

export const providerRoutes = new OpenAPIHono<AppEnv>()
  .openapi(listProvidersRoute, async c => {
    const { db } = c.get('deps')
    const r = await Providers.list(db)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    const providers: Record<string, unknown> = {}
    for (const p of r.value) providers[p.provider_id] = providerToJson(p)
    return c.json({ providers }, 200)
  })
  .openapi(getCatalogRoute, async c => {
    const { bus } = c.get('deps')
    const catalog = await getModelsDev(bus)
    return c.json({ catalog: catalog ?? {} }, 200)
  })
  .openapi(registerProviderRoute, async c => {
    const deps = c.get('deps')
    const b = c.req.valid('json')

    const valid = validateApiType(b.api_type)
    if (valid.isErr()) return c.json({ ok: false, error: valid.error }, 400)
    if (
      !b.base_url.startsWith('http://') &&
      !b.base_url.startsWith('https://')
    ) {
      return c.json({ ok: false, error: 'base_url must be http(s)' }, 400)
    }

    const r = await Providers.upsert(deps.db, {
      providerId: b.provider_id,
      apiType: b.api_type,
      baseUrl: b.base_url,
      apiKey: b.api_key ?? '',
      headers: b.headers ?? null,
      models: (b.models ?? []).map(m => m.id),
    })
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    deps.llm.invalidate()
    return c.json({ ok: true, provider_id: b.provider_id }, 200)
  })
  .openapi(deleteProviderRoute, async c => {
    const deps = c.get('deps')
    const r = await Providers.delete(deps.db, c.req.valid('param').id)
    if (r.isErr()) return c.json({ ok: false, error: r.error }, 500)
    deps.llm.invalidate()
    return c.json({ deleted: true }, 200)
  })
  .openapi(testProviderRoute, async c => {
    const b = c.req.valid('json')

    // Testing requires a model: prove the provider (via its api_type's SDK)
    // can actually generate text with a single lightweight completion. This
    // catches providers that only expose POST /chat/completions (no GET
    // /models) and confirms the chosen model is usable.
    if (b.model === undefined || b.model === '') {
      return c.json(
        { ok: false, error: 'select a model to test the provider' },
        200,
      )
    }

    const creds = {
      apiType: b.api_type,
      baseUrl: b.base_url,
      apiKey: b.api_key ?? '',
      headers: {},
    }
    const built = buildModelForApiType(creds, b.model)
    if (built.isErr()) {
      return c.json({ ok: false, error: built.error }, 200)
    }
    const gen = await ResultAsync.fromPromise(
      generateText({
        model: built.value,
        prompt: 'hi',
        maxOutputTokens: 8,
      }),
      e => `provider test: generation failed: ${String(e)}`,
    )
    if (gen.isErr()) {
      return c.json({ ok: false, error: gen.error }, 200)
    }
    return c.json({ ok: true, model: b.model, text: gen.value.text }, 200)
  })
