import { maskSecret } from '@abcp-agent/agent'
import { describe, expect, it } from 'vitest'
import { providerToMsg } from '../src/connect.js'

describe('providerToMsg masks the stored api key (A2 regression)', () => {
  it('never returns the plaintext key', () => {
    const msg = providerToMsg({
      provider_id: 'gateway',
      api_type: 'vercel-compatible-gateway',
      base_url: 'https://gw.example/v4/ai',
      api_key: 'gw-0c53f638b62edf7275d9443a7282123d3a9a7f7a96ce9ee0',
      headers: '{}',
      models: '[]',
      updated_at: '',
    })
    expect(msg.apiKey).toBe(
      maskSecret('gw-0c53f638b62edf7275d9443a7282123d3a9a7f7a96ce9ee0'),
    )
    expect(msg.apiKey).toBe('gw-0****9ee0')
    expect(msg.apiKey).not.toContain('f638b62edf')
  })

  it('masks short/empty keys fully', () => {
    for (const key of ['', 'EMPTY', 'short']) {
      const msg = providerToMsg({
        provider_id: 'p',
        api_type: 'openai-compatible',
        base_url: 'https://x',
        api_key: key,
        headers: '{}',
        models: '[]',
        updated_at: '',
      })
      expect(msg.apiKey).toBe('****')
    }
  })

  it('other fields pass through verbatim', () => {
    const msg = providerToMsg({
      provider_id: 'tal',
      api_type: 'openai-compatible',
      base_url: 'http://ai.example/v1',
      api_key: 'k'.repeat(40),
      headers: '{"x-a":"1"}',
      models: '[{"id":"m","context_limit":100000}]',
      updated_at: '2026-09-16',
    })
    expect(msg.providerId).toBe('tal')
    expect(msg.baseUrl).toBe('http://ai.example/v1')
    expect(msg.updatedAt).toBe('2026-09-16')
  })
})
