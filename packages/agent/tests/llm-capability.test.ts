import { describe, expect, it } from 'vitest'
import {
  buildGenerativeModel,
  parseCapability,
  type ProviderCredentials,
} from '../src/llm.js'

const openai: ProviderCredentials = {
  apiType: 'openai',
  baseUrl: 'http://x/v1',
  apiKey: 'k',
  headers: {},
}
const google: ProviderCredentials = {
  apiType: 'google',
  baseUrl: '',
  apiKey: 'k',
  headers: {},
}
const anthropic: ProviderCredentials = {
  apiType: 'anthropic',
  baseUrl: '',
  apiKey: 'k',
  headers: {},
}

describe('parseCapability', () => {
  it('empty defaults to text', () => {
    const r = parseCapability('')
    expect(r.isOk()).toBe(true)
    expect(r._unsafeUnwrap()).toBe('text')
  })

  it('accepts image/video/speech case-insensitively', () => {
    for (const [raw, want] of [
      ['IMAGE', 'image'],
      ['Video', 'video'],
      [' speech ', 'speech'],
      ['text', 'text'],
    ] as const) {
      const r = parseCapability(raw)
      expect(r.isOk()).toBe(true)
      expect(r._unsafeUnwrap()).toBe(want)
    }
  })

  it('rejects unknown values', () => {
    expect(parseCapability('audio').isErr()).toBe(true)
  })
})

describe('buildGenerativeModel (api-type × capability matrix)', () => {
  it('openai builds image + speech', () => {
    expect(buildGenerativeModel(openai, 'gpt-image-1', 'image').isOk()).toBe(true)
    expect(buildGenerativeModel(openai, 'gpt-4o-mini-tts', 'speech').isOk()).toBe(true)
  })

  it('openai rejects video', () => {
    const r = buildGenerativeModel(openai, 'veo-3', 'video')
    expect(r.isErr()).toBe(true)
  })

  it('google builds image + video + speech', () => {
    expect(buildGenerativeModel(google, 'gemini-2.5-flash-image', 'image').isOk()).toBe(true)
    expect(buildGenerativeModel(google, 'veo-3.1-generate', 'video').isOk()).toBe(true)
    expect(buildGenerativeModel(google, 'gemini-2.5-flash-preview-tts', 'speech').isOk()).toBe(true)
  })

  it('text-only api types reject every generation capability', () => {
    for (const cap of ['image', 'video', 'speech'] as const) {
      expect(buildGenerativeModel(anthropic, 'x', cap).isErr()).toBe(true)
    }
  })
})
