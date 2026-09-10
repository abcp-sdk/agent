import { describe, expect, it } from 'vitest'
import {
  buildModelForApiType,
  LlmRegistry,
  modelRef,
  parseProviderModelRef,
} from '../src/llm.js'
import type { Db } from '../src/db-client.js'
import { catalogModel, variantsForApiType } from '../src/variants.js'

// LLM endpoint smoke: prove the selected variant wiring composes with the
// model factory for the supported api types (no network call).

const catalog = {
  openai: {
    models: {
      'gpt-5.4': {
        reasoning: true,
        reasoning_options: [
          { type: 'effort', values: ['none', 'low', 'medium', 'high'] },
        ],
      },
    },
  },
}

// A db stub is never reached for malformed/empty refs (validation is pure).
const fakeDb = {} as Db

describe('LlmRegistry.resolve (no fallback)', () => {
  it('rejects an empty model reference', async () => {
    const r = await new LlmRegistry().resolve(fakeDb, '')
    expect(r.isErr()).toBe(true)
    expect(r._unsafeUnwrapErr()).toContain('no model selected')
  })

  it('rejects a bare model id (no flat lookup)', async () => {
    const r = await new LlmRegistry().resolve(fakeDb, 'gpt-5.4')
    expect(r.isErr()).toBe(true)
    expect(r._unsafeUnwrapErr()).toContain('no model selected')
  })

  it('rejects a malformed ref', async () => {
    const r = await new LlmRegistry().resolve(fakeDb, '/x')
    expect(r.isErr()).toBe(true)
  })
})

describe('variant + model composition', () => {
  it('builds a model and resolves the variant providerOptions', () => {
    const built = buildModelForApiType(
      { apiType: 'openai', baseUrl: 'http://x/v1', apiKey: 'k', headers: {} },
      'gpt-5.4',
    )
    expect(built.isOk()).toBe(true)

    const meta = catalogModel(catalog, 'openai', 'gpt-5.4')!
    const high = variantsForApiType(meta, 'openai').find(v => v.id === 'high')!
    expect(high.providerOptions).toEqual({ openai: { reasoningEffort: 'high' } })
  })

  it('modelRef round-trips through parseProviderModelRef', () => {
    const ref = modelRef('openai', 'gpt-5.4')
    expect(parseProviderModelRef(ref)).toEqual({
      providerId: 'openai',
      modelId: 'gpt-5.4',
    })
  })
})
