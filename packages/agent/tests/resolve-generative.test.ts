import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, type Db } from '../src/db-client.js'
import { Providers } from '../src/db-providers.js'
import { LlmRegistry } from '../src/llm.js'

/**
 * resolveGenerative enforces the DECLARED capability: pointing a non-text
 * knob at a model the provider registered as a different kind is a config
 * error surfaced before any network call. Legacy rows (empty stored
 * model_type) are exempt — they predate capability declarations.
 */
describe('LlmRegistry.resolveGenerative capability matching', () => {
  const dbs: Db[] = []
  afterEach(async () => {
    for (const db of dbs) {
      const c = db.$client as { close?: () => void }
      c.close?.()
    }
    dbs.length = 0
  })

  async function setup(models: string) {
    const dir = mkdtempSync(join(tmpdir(), 'gen-cap-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    dbs.push(r.value)
    await Providers.upsert(r.value, 't', {
      providerId: 'gw',
      apiType: 'vercel-compatible-gateway',
      baseUrl: 'https://gw.example/v4/ai',
      apiKey: 'k',
      headers: {},
      models,
    })
    return new LlmRegistry()
  }

  it('matches the declared capability', async () => {
    const llm = await setup([{ id: 'asr/1', model_type: 'transcription' }])
    const r = await llm.resolveGenerative(
      dbs[0]!,
      't',
      'gw/asr/1',
      'transcription',
    )
    expect(r.isOk()).toBe(true)
  })

  it('rejects a capability mismatch with the declared kind', async () => {
    const llm = await setup([{ id: 'asr/1', model_type: 'transcription' }])
    const r = await llm.resolveGenerative(dbs[0]!, 't', 'gw/asr/1', 'speech')
    expect(r.isErr()).toBe(true)
    expect(r._unsafeUnwrapErr()).toContain(
      "registered as 'transcription', not 'speech'",
    )
  })

  it('rejects an unregistered model id', async () => {
    const llm = await setup([{ id: 'asr/1', model_type: 'speech' }])
    const r = await llm.resolveGenerative(dbs[0]!, 't', 'gw/nope', 'speech')
    expect(r.isErr()).toBe(true)
    expect(r._unsafeUnwrapErr()).toContain('not registered')
  })

  it('LEGACY rows (empty model_type) skip the strict match', async () => {
    const llm = await setup([{ id: 'asr/1', model_type: '' }])
    // Would fail the strict check ('text' != 'transcription'), but the row
    // predates capabilities so the matrix alone decides.
    const r = await llm.resolveGenerative(
      dbs[0]!,
      't',
      'gw/asr/1',
      'transcription',
    )
    expect(r.isOk()).toBe(true)
  })

  it('the matrix still rejects legacy rows for unsupported kinds', async () => {
    // openai-protocol provider with a legacy row asked for VIDEO: the matrix
    // has no OpenAI video endpoint, so it fails regardless of the exemption.
    const dir = mkdtempSync(join(tmpdir(), 'gen-cap2-'))
    const r2 = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r2.isErr()) throw new Error(r2.error)
    dbs.push(r2.value)
    await Providers.upsert(r2.value, 't', {
      providerId: 'oa',
      apiType: 'openai-compatible',
      baseUrl: 'http://oa/v1',
      apiKey: 'k',
      headers: {},
      models: [{ id: 'v', model_type: '' }],
    })
    const llm2 = new LlmRegistry()
    const r = await llm2.resolveGenerative(
      dbs[dbs.length - 1]!,
      't',
      'oa/v',
      'video',
    )
    expect(r.isErr()).toBe(true)
    expect(r._unsafeUnwrapErr()).toContain("cannot serve capability 'video'")
  })
})
