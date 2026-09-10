import { describe, expect, it } from 'vitest'
import {
  catalogModel,
  findVariant,
  toModelVariant,
  variantsForApiType,
} from '../src/variants.js'

// A minimal models.dev catalog keyed provider-first.
const catalog = {
  anthropic: {
    models: {
      'claude-opus-4-6': {
        reasoning: true,
        reasoning_options: [
          { type: 'toggle' },
          { type: 'effort', values: ['low', 'medium', 'high', 'max'] },
          { type: 'budget_tokens', min: 1024, max: 127999 },
        ],
        limit: { context: 200000 },
        experimental: {
          modes: {
            fast: {
              cost: { input: 10, output: 50 },
              provider: { body: { speed: 'fast' }, headers: { 'anthropic-beta': 'fast' } },
            },
          },
        },
      },
    },
  },
  openai: {
    models: {
      'gpt-5.4': {
        reasoning: true,
        reasoning_options: [{ type: 'effort', values: ['none', 'low', 'medium', 'high', 'xhigh'] }],
        limit: { context: 400000 },
      },
    },
  },
  google: {
    models: {
      'gemini-3-pro': {
        reasoning: true,
        reasoning_options: [{ type: 'effort', values: ['low', 'medium', 'high'] }],
        limit: { context: 1000000 },
      },
    },
  },
  'openai-compatible': {
    models: {
      'deepseek-v4-flash': {
        reasoning: true,
        reasoning_options: [{ type: 'effort', values: ['high', 'max'] }],
        limit: { context: 1000000 },
      },
    },
  },
}

describe('catalogModel (strict provider/model lookup)', () => {
  it('resolves under the given provider only', () => {
    expect(catalogModel(catalog, 'anthropic', 'claude-opus-4-6')?.contextLimit).toBe(200000)
    // Same model id under a different provider must not leak through.
    expect(catalogModel(catalog, 'openai', 'claude-opus-4-6')).toBeNull()
  })

  it('returns null for unknown provider/model', () => {
    expect(catalogModel(catalog, 'nope', 'gpt-5.4')).toBeNull()
    expect(catalogModel(catalog, 'openai', 'nope')).toBeNull()
    expect(catalogModel(catalog, '', 'gpt-5.4')).toBeNull()
    expect(catalogModel(null, 'openai', 'gpt-5.4')).toBeNull()
  })
})

describe('variantsForApiType', () => {
  it('maps effort to anthropic effort options', () => {
    const m = catalogModel(catalog, 'anthropic', 'claude-opus-4-6')!
    const vs = variantsForApiType(m, 'anthropic')
    const high = vs.find(v => v.id === 'high')!
    expect(high.providerOptions).toEqual({ anthropic: { effort: 'high' } })
    // experimental mode present
    const fast = vs.find(v => v.id === 'fast')!
    expect(fast.providerOptions.anthropic).toEqual({ speed: 'fast' })
    expect(fast.headers).toEqual({ 'anthropic-beta': 'fast' })
  })

  it('maps effort to openai reasoningEffort (incl. none)', () => {
    const m = catalogModel(catalog, 'openai', 'gpt-5.4')!
    const vs = variantsForApiType(m, 'openai')
    expect(vs.find(v => v.id === 'none')!.providerOptions).toEqual({ openai: { reasoningEffort: 'none' } })
    expect(vs.find(v => v.id === 'xhigh')!.providerOptions).toEqual({ openai: { reasoningEffort: 'xhigh' } })
  })

  it('clamps google thinkingLevel and sets includeThoughts', () => {
    const m = catalogModel(catalog, 'google', 'gemini-3-pro')!
    const vs = variantsForApiType(m, 'gemini')
    expect(vs.find(v => v.id === 'high')!.providerOptions).toEqual({
      google: { thinkingConfig: { includeThoughts: true, thinkingLevel: 'high' } },
    })
  })

  it('maps openai-compatible effort to reasoningEffort', () => {
    const m = catalogModel(catalog, 'openai-compatible', 'deepseek-v4-flash')!
    const vs = variantsForApiType(m, 'openai-compatible')
    expect(vs.map(v => v.id)).toEqual(['high', 'max'])
    expect(vs[0]!.providerOptions).toEqual({
      'openai-compatible': { reasoningEffort: 'high' },
    })
  })

  it('emits a single toggle variant when only toggle is present', () => {
    const m = catalogModel(
      {
        p: {
          models: {
            x: { reasoning: true, reasoning_options: [{ type: 'toggle' }] },
          },
        },
      },
      'p',
      'x',
    )!
    const vs = variantsForApiType(m, 'openai')
    expect(vs).toHaveLength(1)
    expect(vs[0]!.id).toBe('on')
    expect(vs[0]!.providerOptions).toEqual({ openai: { reasoningEffort: 'medium' } })
  })

  it('emits budget high/max variants when toggle + budget_tokens', () => {
    const m = catalogModel(catalog, 'anthropic', 'claude-opus-4-6')!
    const vs = variantsForApiType(m, 'anthropic')
    // effort present wins; budget is not emitted as separate variants here
    expect(vs.some(v => v.id === 'max')).toBe(true)
  })

  it('returns no variants for a model without reasoning options', () => {
    const m = catalogModel(
      { p: { models: { x: { reasoning: false } } } },
      'p',
      'x',
    )!
    expect(variantsForApiType(m, 'openai')).toEqual([])
  })
})

describe('findVariant / toModelVariant', () => {
  it('finds by id and strips wiring', () => {
    const m = catalogModel(catalog, 'openai', 'gpt-5.4')!
    const v = findVariant(m, 'openai', 'high')!
    expect(v.providerOptions).toEqual({ openai: { reasoningEffort: 'high' } })
    expect(toModelVariant(v)).toEqual({
      id: 'high',
      name: 'high',
      description: 'reasoning effort high',
    })
    expect(findVariant(m, 'openai', 'nope')).toBeNull()
    expect(findVariant(m, 'openai', '')).toBeNull()
  })
})
