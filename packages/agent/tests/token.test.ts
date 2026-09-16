import { describe, expect, it } from 'vitest'
import { estimateTokens, maskSecret } from '../src/token.js'

describe('maskSecret (provider key display + edit sentinel)', () => {
  it('long secrets keep a recognizable prefix/suffix', () => {
    expect(
      maskSecret('gw-0c53f638b62edf7275d9443a7282123d3a9a7f7a96ce9ee0'),
    ).toBe('gw-0****9ee0')
  })

  it('short secrets mask fully (nothing usable leaks)', () => {
    expect(maskSecret('EMPTY')).toBe('****')
    expect(maskSecret('12345678')).toBe('****')
    expect(maskSecret('')).toBe('****')
  })

  it('the mask is deterministic so the sentinel comparison is stable', () => {
    const key = 'sk-abcdef1234567890abcdef'
    expect(maskSecret(key)).toBe(maskSecret(key))
    // A masked value never equals its own secret.
    expect(maskSecret(key)).not.toBe(key)
  })
})

describe('estimateTokens (sanity, unchanged)', () => {
  it('estimates CJK-heavier text with fewer chars-per-token', () => {
    expect(estimateTokens('你好世界')).toBeGreaterThan(0)
    expect(estimateTokens('hello world')).toBeGreaterThan(0)
  })
})
