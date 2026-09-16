import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  Code,
  ConnectError,
  createContextValues,
  type HandlerContext,
} from '@connectrpc/connect'
import { connectDb, type Db } from '@easylab-agent/agent'
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
  return providersHandlers({ db: r.value } as never)
}
const ctx = (): HandlerContext => {
  const values = createContextValues()
  values.set(kIdentity, { role: 'tenant', tenant: 'default' })
  return { values } as unknown as HandlerContext
}

describe('registerProvider capability validation', () => {
  const reg = (
    providerId: string,
    apiType: string,
    models: Array<{ id: string; modelType?: string; contextLimit?: bigint }>,
  ) => ({
    provider: {
      providerId,
      apiType,
      baseUrl: 'http://x/v1',
      apiKey: 'k',
      models: models.map(m => ({
        id: m.id,
        name: m.id,
        contextLimit:
          m.contextLimit ??
          (m.modelType && m.modelType !== 'text' ? 0n : 1000n),
        modelType: m.modelType ?? '',
      })),
    },
  })

  it('an openai provider may carry mixed-modality models', async () => {
    const h = await handlers()
    const r = await h.registerProvider!(
      reg('oa', 'openai-compatible', [
        { id: 'chat', modelType: 'text' },
        { id: 'embed', modelType: 'embedding' },
        { id: 'tts', modelType: 'speech' },
        { id: 'asr', modelType: 'transcription' },
        { id: 'img', modelType: 'image' },
      ]) as never,
      ctx(),
    )
    expect((r as { ok: boolean }).ok).toBe(true)
  })

  it('video on an openai provider is rejected', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'openai', [{ id: 'vid', modelType: 'video' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/cannot serve capability 'video'/)
  })

  it('rerank on an openai provider is rejected; cohere serves it', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'openai', [{ id: 'r', modelType: 'rerank' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/cannot serve capability 'rerank'/)
    const ok2 = await h.registerProvider!(
      reg('co', 'cohere', [
        { id: 'rerank-v3.5', modelType: 'rerank' },
      ]) as never,
      ctx(),
    )
    expect((ok2 as { ok: boolean }).ok).toBe(true)
  })

  it('several gateway providers with arbitrary ids are allowed', async () => {
    const h = await handlers()
    const a = await h.registerProvider!(
      reg('gateway', 'vercel-compatible-gateway', [
        { id: 'm/1', modelType: 'text' },
        { id: 'm/2', modelType: 'image' },
      ]) as never,
      ctx(),
    )
    const b = await h.registerProvider!(
      reg('gateway-two', 'vercel-compatible-gateway', [
        { id: 'm/3', modelType: 'speech' },
      ]) as never,
      ctx(),
    )
    expect((a as { ok: boolean }).ok && (b as { ok: boolean }).ok).toBe(true)
  })

  it('text models require context_limit > 0; non-text require 0', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'openai', [
          { id: 't', modelType: 'text', contextLimit: 0n },
        ]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/context_limit is required/)
    await expect(
      h.registerProvider!(
        reg('oa', 'openai', [
          { id: 't', modelType: 'image', contextLimit: 500n },
        ]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/context_limit must be 0/)
  })

  it('an unknown model_type is rejected with the capability list', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('oa', 'openai', [{ id: 'x', modelType: 'vibe' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/unknown capability/)
  })

  it('anthropic rejects non-text models', async () => {
    const h = await handlers()
    await expect(
      h.registerProvider!(
        reg('an', 'anthropic', [{ id: 'c', modelType: 'image' }]) as never,
        ctx(),
      ),
    ).rejects.toThrow(/cannot serve/)
  })

  it('registration errors surface as ConnectError InvalidArgument', async () => {
    const h = await handlers()
    try {
      await h.registerProvider!(
        reg('oa', 'openai', [{ id: 'v', modelType: 'video' }]) as never,
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
      'speech',
      'text',
      'transcription',
    ])
    expect(t['vercel-compatible-gateway']?.capabilities.length).toBe(7)
    expect(t['cohere']?.capabilities).toEqual(['text', 'rerank'])
    expect(t['anthropic']?.capabilities).toEqual(['text'])
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
