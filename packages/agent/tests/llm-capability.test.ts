import { describe, expect, it } from 'vitest'
import {
  buildGenerativeModel,
  buildModelForApiType,
  GATEWAY_API_TYPE,
  type ProviderCredentials,
  parseCapability,
} from '../src/llm.js'

const gateway: ProviderCredentials = {
  providerId: 'gw',
  apiType: GATEWAY_API_TYPE,
  baseUrl: 'https://gateway.example/v4/ai',
  apiKey: 'EMPTY',
  headers: {},
}
const openai: ProviderCredentials = {
  providerId: 'openai',
  apiType: 'openai',
  baseUrl: 'http://x/v1',
  apiKey: 'k',
  headers: {},
}

describe('parseCapability', () => {
  it('empty defaults to text', () => {
    const r = parseCapability('')
    expect(r.isOk()).toBe(true)
    expect(r._unsafeUnwrap()).toBe('text')
  })

  it('accepts every capability case-insensitively', () => {
    for (const [raw, want] of [
      ['IMAGE', 'image'],
      ['Video', 'video'],
      [' speech ', 'speech'],
      ['Transcription', 'transcription'],
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

describe('buildModelForApiType', () => {
  it('builds a language model for the gateway', () => {
    expect(buildModelForApiType(gateway, 'qwen3.8-flash-next').isOk()).toBe(
      true,
    )
  })
})

describe('buildGenerativeModel (gateway-only multimodal)', () => {
  it('the gateway builds every multimodal capability', () => {
    for (const cap of ['image', 'video', 'speech', 'transcription'] as const) {
      expect(buildGenerativeModel(gateway, 'qwen3-tts', cap).isOk()).toBe(true)
    }
  })

  it('non-gateway providers reject every multimodal capability', () => {
    for (const cap of ['image', 'video', 'speech', 'transcription'] as const) {
      const r = buildGenerativeModel(openai, 'gpt-image-1', cap)
      expect(r.isErr()).toBe(true)
      expect(r._unsafeUnwrapErr()).toContain(GATEWAY_API_TYPE)
    }
  })
})
