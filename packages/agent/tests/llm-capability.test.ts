import { describe, expect, it } from 'vitest'
import {
  buildGenerativeModel,
  buildModelForApiType,
  COHERE_API_TYPE,
  capabilitiesOf,
  GATEWAY_API_TYPE,
  type ProviderCredentials,
  parseCapability,
  supportsCapability,
  validateApiType,
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
      ['Embedding', 'embedding'],
      ['rerank', 'rerank'],
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

describe('capability matrix', () => {
  it('openai-protocol providers serve text/embedding/image/speech/transcription', () => {
    for (const t of ['openai', 'openai-compatible']) {
      expect(supportsCapability(t, 'text')).toBe(true)
      expect(supportsCapability(t, 'embedding')).toBe(true)
      expect(supportsCapability(t, 'image')).toBe(true)
      expect(supportsCapability(t, 'speech')).toBe(true)
      expect(supportsCapability(t, 'transcription')).toBe(true)
      // No standard OpenAI endpoints for these:
      expect(supportsCapability(t, 'video')).toBe(false)
      expect(supportsCapability(t, 'rerank')).toBe(false)
    }
  })

  it('the gateway serves every capability', () => {
    expect(capabilitiesOf(GATEWAY_API_TYPE).size).toBe(7)
  })

  it('cohere serves text + rerank only', () => {
    expect(supportsCapability(COHERE_API_TYPE, 'text')).toBe(true)
    expect(supportsCapability(COHERE_API_TYPE, 'rerank')).toBe(true)
    expect(supportsCapability(COHERE_API_TYPE, 'image')).toBe(false)
  })

  it('anthropic/deepseek/google are text-only', () => {
    for (const t of ['anthropic', 'deepseek', 'google']) {
      expect(supportsCapability(t, 'text')).toBe(true)
      expect(supportsCapability(t, 'image')).toBe(false)
    }
  })

  it('cohere is a valid api type', () => {
    expect(validateApiType('cohere').isOk()).toBe(true)
    expect(validateApiType('nope').isErr()).toBe(true)
  })
})

describe('buildGenerativeModel (matrix dispatch)', () => {
  it('the gateway builds every capability', () => {
    for (const cap of [
      'image',
      'video',
      'speech',
      'transcription',
      'embedding',
      'rerank',
    ] as const) {
      expect(buildGenerativeModel(gateway, 'm/x', cap).isOk()).toBe(true)
    }
  })

  it('openai-protocol providers build their four generative kinds', () => {
    for (const cap of [
      'embedding',
      'image',
      'speech',
      'transcription',
    ] as const) {
      expect(buildGenerativeModel(openai, 'm/x', cap).isOk()).toBe(true)
    }
  })

  it('video on an openai provider is rejected (gateway-only)', () => {
    const r = buildGenerativeModel(openai, 'm/x', 'video')
    expect(r.isErr()).toBe(true)
    expect(r._unsafeUnwrapErr()).toContain('cannot serve')
  })

  it('rerank on an openai provider is rejected; cohere serves it', () => {
    const cohere: ProviderCredentials = {
      providerId: 'co',
      apiType: COHERE_API_TYPE,
      baseUrl: 'https://co.example',
      apiKey: 'k',
      headers: {},
    }
    expect(buildGenerativeModel(openai, 'm/x', 'rerank').isErr()).toBe(true)
    expect(buildGenerativeModel(cohere, 'rerank-v3.5', 'rerank').isOk()).toBe(
      true,
    )
  })

  it('cohere rejects non-rerank generative kinds', () => {
    const cohere: ProviderCredentials = {
      providerId: 'co',
      apiType: COHERE_API_TYPE,
      baseUrl: 'https://co.example',
      apiKey: 'k',
      headers: {},
    }
    expect(buildGenerativeModel(cohere, 'm/x', 'image').isErr()).toBe(true)
  })

  it('text models build for cohere', () => {
    const cohere: ProviderCredentials = {
      providerId: 'co',
      apiType: COHERE_API_TYPE,
      baseUrl: 'https://co.example',
      apiKey: 'k',
      headers: {},
    }
    expect(buildModelForApiType(cohere, 'command-x').isOk()).toBe(true)
  })
})
