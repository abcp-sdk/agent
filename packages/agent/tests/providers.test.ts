import { describe, expect, it } from 'vitest'
import { modelRef, parseProviderModelRef } from '../src/llm.js'

describe('parseProviderModelRef', () => {
  it('splits provider/model', () => {
    expect(parseProviderModelRef('anthropic/claude-opus-4-6')).toEqual({
      providerId: 'anthropic',
      modelId: 'claude-opus-4-6',
    })
  })

  it('keeps slashes in the model id', () => {
    expect(parseProviderModelRef('openrouter/anthropic/claude')).toEqual({
      providerId: 'openrouter',
      modelId: 'anthropic/claude',
    })
  })

  it('rejects malformed and bare refs', () => {
    expect(parseProviderModelRef('')).toBeNull()
    expect(parseProviderModelRef('gpt-5')).toBeNull()
    expect(parseProviderModelRef('/x')).toBeNull()
    expect(parseProviderModelRef('x/')).toBeNull()
  })
})

describe('modelRef', () => {
  it('joins provider + model', () => {
    expect(modelRef('pa', 'gpt-5')).toBe('pa/gpt-5')
  })
})
