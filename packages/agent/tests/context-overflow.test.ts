import { describe, expect, it } from 'vitest'
import {
  isContextOverflow,
  isContextOverflowFailure,
} from '../src/context-overflow.js'

describe('isContextOverflow', () => {
  it('matches classic overflow messages', () => {
    expect(isContextOverflow('prompt is too long')).toBe(true)
    expect(isContextOverflow('input exceeds the context window')).toBe(true)
    expect(
      isContextOverflow(
        "input (5000 tokens) is longer than the model's context length (4096 tokens)",
      ),
    ).toBe(true)
    expect(isContextOverflow('model_context_window_exceeded')).toBe(true)
  })

  it('matches the self-hosted gateway wording', () => {
    expect(
      isContextOverflow(
        'Input exceeds the context limit (1048568 tokens). Please shorten the input. Request id: 0217906650563477dd244b6caaa2409295dae72fd71f67976366e',
      ),
    ).toBe(true)
    expect(
      isContextOverflow(
        'GatewayInvalidRequestError: Input exceeds the context limit (1048568 tokens). Please shorten the input.',
      ),
    ).toBe(true)
    expect(
      isContextOverflow('Input tokens exceed the model context limit.'),
    ).toBe(true)
  })

  it('excludes throttling/rate-limit errors', () => {
    expect(isContextOverflow('throttling error: try later')).toBe(false)
    expect(isContextOverflow('rate limit exceeded')).toBe(false)
    expect(isContextOverflow('too many requests')).toBe(false)
  })
})

describe('isContextOverflowFailure', () => {
  it('detects overflow from message text', () => {
    expect(
      isContextOverflowFailure(
        new Error('maximum context length is 4096 tokens'),
      ),
    ).toBe(true)
  })

  it('detects overflow from status codes', () => {
    // Bare 400 is NOT overflow (false positives trigger irreversible
    // compaction); 413 is.
    expect(isContextOverflowFailure({ statusCode: 400, message: 'bad' })).toBe(
      false,
    )
    expect(
      isContextOverflowFailure({ statusCode: 413, message: 'large' }),
    ).toBe(true)
  })

  it('rejects unrelated failures', () => {
    expect(isContextOverflowFailure(new Error('network down'))).toBe(false)
    expect(isContextOverflowFailure(null)).toBe(false)
    expect(isContextOverflowFailure(undefined)).toBe(false)
  })
})
