import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { connectDb, type Db, LlmRegistry } from '@abcp-agent/agent'
import {
  Code,
  ConnectError,
  createContextValues,
  type HandlerContext,
} from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'
import { kIdentity } from '../src/auth.js'
import { providersHandlers } from '../src/handlers/providers.js'

/**
 * Registration semantics under the capability matrix: `model_type` is the
 * DECLARED capability (text requires context_limit > 0, non-text requires 0)
 * and the (apiType x capability) pair is validated against the matrix. The
 * gateway carries no special-casing anymore — several gateway providers with
 * arbitrary ids are allowed.
 */
const dbs: Db[] = []
afterEach(async () => {
  for (const db of dbs) {
    const c = db.$client as { close?: () => void }
    c.close?.()
  }
  dbs.length = 0
})

async function handlers() {
  const dir = mkdtempSync(join(tmpdir(), 'prov-matrix-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value)
  // registerProvider drops the llm client cache; the deps bag needs a real
  // registry or invalidate() crashes.
  return providersHandlers({ db: r.value, llm: new LlmRegistry() } as never)
}
const ctx = (): HandlerContext => {
  const values = createContextValues()
  values.set(kIdentity, { role: 'tenant', tenant: 'default' })
  return { values } as unknown as HandlerContext
}

describe('registerProvider capability validation (semantic grouping)', () => {
  // A provider serves EXACTLY ONE modality; `capability` is the provider's.
  const reg = (
    providerId: string,
    capability: string,
    apiType: string,
    models: Array<{ id: string; contextLimit?: bigint }>,
  ) => ({
    provider: {
      providerId,
      capability,
      apiType,
      baseUrl: 'http://x/v1',
      apiKey: 'k',
      models: models.map(m => ({
        id: m.id,
        name: m.id,
        contextLimit: m.contextLimit ?? (capability === 'text' ? 1000n : 0n),
      })),
    },
  })

  it('a multi-modality host registers one provider per modality', async () => {
    const h = await handlers()
    const r = await h.registerProvider!(
      reg('oa-text', 'text', 'openai-compatible', [{ id: 'chat' }]) as never,
      ctx(),
    )
    const r2 = await h.registerProvider!(
      reg('oa-image', 'image', 'openai-compatible', [{ id: 'img' }]) as never,
      ctx(),
    )
    expect((r as { ok: boolean }).ok && (r2 as { ok: boolean }).ok).toBe(true)
  })

  it('video on an openai provider is rejected', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'video', 'openai', [{ id: 'vid' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/cannot serve capability 'video'/)
  })

  it('rerank on an openai provider is rejected; cohere serves it', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'rerank', 'openai', [{ id: 'r' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/cannot serve capability 'rerank'/)
    const ok2 = await h.registerProvider!(
      reg('co', 'rerank', 'cohere', [{ id: 'rerank-v3.5' }]) as never,
      ctx(),
    )
    expect((ok2 as { ok: boolean }).ok).toBe(true)
  })

  it('several gateway providers with arbitrary ids are allowed', async () => {
    const h = await handlers()
    const a = await h.registerProvider!(
      reg('gateway', 'text', 'vercel-compatible-gateway', [
        { id: 'm/1' },
      ]) as never,
      ctx(),
    )
    const b = await h.registerProvider!(
      reg('gateway-two', 'image', 'vercel-compatible-gateway', [
        { id: 'm/3' },
      ]) as never,
      ctx(),
    )
    expect((a as { ok: boolean }).ok && (b as { ok: boolean }).ok).toBe(true)
  })

  it('text models require context_limit > 0; non-text require 0', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'text', 'openai', [{ id: 't', contextLimit: 0n }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/context_limit is required/)
    await expect(
      h.registerProvider!(
        reg('oa', 'image', 'openai', [
          { id: 't', contextLimit: 500n },
        ]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/context_limit must be 0/)
  })

  it('an unknown capability is rejected with the capability list', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'vibe', 'openai', [{ id: 'x' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/unknown capability/)
  })

  it('anthropic rejects non-text providers', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('an', 'image', 'anthropic', [{ id: 'c' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/cannot serve/)
  })

  it('registration errors surface as ConnectError InvalidArgument', async () => {
    const h = await handlers()
    try {
      await h.registerProvider!(
        reg('oa', 'video', 'openai', [{ id: 'v' }]) as never,
        ctx(),
      )
      expect.unreachable()
    } catch (e) {
      expect(e).toBeInstanceOf(ConnectError)
      expect((e as ConnectError).code).toBe(Code.InvalidArgument)
    }
  })
})

describe('ListProvidersCatalog serves the capability matrix', () => {
  it('returns every api type with its served capabilities', async () => {
    const h = await handlers()
    const r = (await h.listProvidersCatalog!({} as never, ctx())) as {
      apiTypes: Record<string, { capabilities: string[] }>
    }
    const t = r.apiTypes
    expect(t['openai']?.capabilities.sort()).toEqual([
      'embedding',
      'image',
      'realtime',
      'speech',
      'text',
      'transcription',
    ])
    expect(t['vercel-compatible-gateway']?.capabilities.length).toBe(8)
    expect(t['cohere']?.capabilities).toEqual(['text', 'rerank'])
    expect(t['anthropic']?.capabilities).toEqual(['text'])
    // Historical aliases validate but are HIDDEN from the served catalog.
    expect(Object.keys(t).sort()).toEqual([
      'anthropic',
      'cohere',
      'deepseek',
      'google',
      'openai',
      'openai-compatible',
      'vercel-compatible-gateway',
    ])
  })

  it('covers the same api types the validator accepts', async () => {
    const h = await handlers()
    const r = (await h.listProvidersCatalog!({} as never, ctx())) as {
      apiTypes: Record<string, { capabilities: string[] }>
    }
    // Every known api type is present with at least `text`.
    for (const caps of Object.values(r.apiTypes)) {
      expect(caps.capabilities).toContain('text')
    }
  })
})
