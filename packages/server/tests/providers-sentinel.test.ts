import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createContextValues, type HandlerContext } from '@connectrpc/connect'
import {
  connectDb,
  type Db,
  maskSecret,
  Providers,
  rawAll,
} from '@abcp-agent/agent'
import { afterEach, describe, expect, it } from 'vitest'
import { kIdentity } from '../src/auth.js'
import { providersHandlers } from '../src/handlers/providers.js'

/** A tenant HandlerContext carrying a verified tenant identity. */
function ctx(): HandlerContext {
  const values = createContextValues()
  values.set(kIdentity, { role: 'tenant', tenant: 'default' })
  return { values } as unknown as HandlerContext
}

/**
 * The provider key edit-time SENTINEL: ListProviders returns a MASK, and a
 * client that prefills the form with the mask and saves without touching the
 * key must NOT corrupt the stored secret (a real flow the Flutter settings
 * page exercises on every save).
 */
describe('registerProvider api-key sentinel', () => {
  const dbs: Db[] = []
  afterEach(async () => {
    for (const db of dbs) {
      const c = db.$client as { close?: () => void }
      c.close?.()
    }
    dbs.length = 0
  })

  async function handlers() {
    const dir = mkdtempSync(join(tmpdir(), 'prov-sentinel-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    dbs.push(r.value)
    return providersHandlers({
      db: r.value,
    } as never)
  }

  const provider = (apiKey: string) => ({
    providerId: 'openai',
    apiType: 'openai-compatible',
    baseUrl: 'http://mock.test/v1',
    apiKey,
    models: [
      { id: 'm1', name: 'M1', contextLimit: 100000n, modelType: 'text' },
    ],
  })

  it('list masks the stored key', async () => {
    const h = await handlers()
    await h.registerProvider!(
      { provider: provider('sk-secret-key-123456') } as never,
      ctx(),
    )
    const list = await h.listProviders!({} as never, ctx())
    const p = (list as { providers: Array<{ apiKey: string }> }).providers.find(
      x => (x as { providerId: string }).providerId === 'openai',
    )
    expect(p?.apiKey).toBe(maskSecret('sk-secret-key-123456'))
  })

  it('saving the MASK back keeps the stored secret', async () => {
    const h = await handlers()
    const first = await h.registerProvider!(
      { provider: provider('sk-orig-secret-abcdef') } as never,
      ctx(),
    )
    expect((first as { ok: boolean }).ok).toBe(true)
    // Re-register with the MASKED key (exactly what an untouched edit form
    // round-trips).
    const mask = maskSecret('sk-orig-secret-abcdef')
    await h.registerProvider!({ provider: provider(mask) } as never, ctx())
    const rows = await rawAll(
      dbs[0]!,
      'SELECT api_key FROM providers WHERE provider_id = ?',
      ['openai'],
    )
    expect(rows[0]!['api_key']).toBe('sk-orig-secret-abcdef')
  })

  it('a DIFFERENT typed key overwrites the stored secret', async () => {
    const h = await handlers()
    await h.registerProvider!(
      { provider: provider('sk-orig-secret-abcdef') } as never,
      ctx(),
    )
    await h.registerProvider!(
      { provider: provider('sk-brand-new-987654') } as never,
      ctx(),
    )
    const rows = await rawAll(
      dbs[0]!,
      'SELECT api_key FROM providers WHERE provider_id = ?',
      ['openai'],
    )
    expect(rows[0]!['api_key']).toBe('sk-brand-new-987654')
  })

  it('Providers.list on the fresh DB sees the registered row (wiring sanity)', async () => {
    const h = await handlers()
    await h.registerProvider!(
      { provider: provider('sk-wiring-1234567890') } as never,
      ctx(),
    )
    const rows = await Providers.list(dbs[0]!, 'default')
    expect(rows.isOk() && rows.value.length).toBe(1)
  })
})
