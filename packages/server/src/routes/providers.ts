import {
  buildModelForApiType,
  getModelsDev,
  Providers,
  parse,
  validateApiType,
} from '@easylab-agent/agent'
import {
  ProviderBodySchema,
  ProviderTestBodySchema,
} from '@easylab-agent/schema'
import { generateText } from 'ai'
import { ResultAsync } from 'neverthrow'
import { z } from 'zod'
import { type Ctx, type Router } from '../http.js'

const HeadersRecordSchema = z.record(z.string(), z.unknown())
const ModelsArraySchema = z.array(z.string())

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

export function providerRoutes(r: Router): void {
  r.get('/providers', async c => {
    const { db } = c.deps
    const res = await Providers.list(db)
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    const providers: Record<string, unknown> = {}
    for (const p of res.value) providers[p.provider_id] = providerToJson(p)
    return c.json({ providers }, 200)
  })

  r.get('/providers/catalog', async c => {
    const { bus } = c.deps
    const catalog = await getModelsDev(bus)
    return c.json({ catalog: catalog ?? {} }, 200)
  })

  r.post(
    '/providers',
    async c => {
      const deps = c.deps
      const b = c.body

      const valid = validateApiType(b.api_type)
      if (valid.isErr()) return c.json({ ok: false, error: valid.error }, 400)
      if (
        !b.base_url.startsWith('http://') &&
        !b.base_url.startsWith('https://')
      ) {
        return c.json({ ok: false, error: 'base_url must be http(s)' }, 400)
      }

      const res = await Providers.upsert(deps.db, {
        providerId: b.provider_id,
        apiType: b.api_type,
        baseUrl: b.base_url,
        apiKey: b.api_key ?? '',
        headers: b.headers ?? null,
        models: (b.models ?? []).map(m => m.id),
      })
      if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
      deps.llm.invalidate()
      return c.json({ ok: true, provider_id: b.provider_id }, 200)
    },
    ProviderBodySchema,
  )

  r.delete('/providers/:id', async c => {
    const deps = c.deps
    const res = await Providers.delete(deps.db, c.req.params['id'] ?? '')
    if (res.isErr()) return c.json({ ok: false, error: res.error }, 500)
    deps.llm.invalidate()
    return c.json({ deleted: true }, 200)
  })

  r.post(
    '/providers/test',
    async c => {
      const b = c.body

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
    },
    ProviderTestBodySchema,
  )
}
