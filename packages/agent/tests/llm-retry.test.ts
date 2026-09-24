import { APICallError, StreamProviderError, ToolChoiceViolationError } from 'ai'
import { describe, expect, it } from 'vitest'
import {
  isRetryableThrown,
  isToolChoiceViolation,
  RETRY_MAX_DELAY_NO_HEADERS_MS,
  retryDelayMs,
} from '../src/llm-retry.js'

/** Build an APICallError with a given status + retryable flag. */
function apiError(
  statusCode: number,
  isRetryable: boolean,
  responseHeaders?: Record<string, string>,
): APICallError {
  return new APICallError({
    message: `HTTP ${statusCode}`,
    url: 'http://provider.test/v1/chat',
    requestBodyValues: {},
    statusCode,
    isRetryable,
    ...(responseHeaders !== undefined ? { responseHeaders } : {}),
  })
}

describe('isRetryableThrown', () => {
  it('retries a retryable APICallError (429)', () => {
    expect(isRetryableThrown(apiError(429, true))).toBe(true)
  })

  it('does NOT retry a non-retryable APICallError (400)', () => {
    expect(isRetryableThrown(apiError(400, false))).toBe(false)
  })

  it('retries a retryable StreamProviderError (mid-stream disconnect)', () => {
    const err = new StreamProviderError({
      message: 'connection reset',
      isRetryable: true,
    })
    expect(isRetryableThrown(err)).toBe(true)
  })

  it('does NOT retry a non-retryable StreamProviderError', () => {
    const err = new StreamProviderError({
      message: 'bad request',
      isRetryable: false,
    })
    expect(isRetryableThrown(err)).toBe(false)
  })

  it('retries a raw transport error by message (ECONNRESET)', () => {
    expect(isRetryableThrown(new Error('read ECONNRESET'))).toBe(true)
    expect(isRetryableThrown(new Error('socket hang up'))).toBe(true)
    expect(isRetryableThrown(new Error('fetch failed'))).toBe(true)
  })

  it('retries by status when the SDK did not classify it', () => {
    expect(isRetryableThrown({ statusCode: 503 })).toBe(true)
    expect(isRetryableThrown({ statusCode: 429 })).toBe(true)
    expect(isRetryableThrown({ statusCode: 404 })).toBe(false)
  })

  it('does not retry a generic programming error', () => {
    expect(
      isRetryableThrown(new Error('cannot read property x of undefined')),
    ).toBe(false)
  })

  it('ignores null/undefined', () => {
    expect(isRetryableThrown(null)).toBe(false)
    expect(isRetryableThrown(undefined)).toBe(false)
  })
})

describe('isToolChoiceViolation', () => {
  it('detects the SDK tool-choice violation', () => {
    expect(
      isToolChoiceViolation(new ToolChoiceViolationError({ message: 'no' })),
    ).toBe(true)
    expect(isToolChoiceViolation(new Error('other'))).toBe(false)
  })
})

describe('retryDelayMs', () => {
  it('honors retry-after-ms when present', () => {
    const err = apiError(429, true, { 'retry-after-ms': '1500' })
    expect(retryDelayMs(1, err)).toBe(1500)
  })

  it('honors retry-after seconds when present', () => {
    const err = apiError(429, true, { 'retry-after': '2' })
    expect(retryDelayMs(1, err)).toBe(2000)
  })

  it('is exponential without headers (2s, 4s, 8s) with zero jitter', () => {
    expect(retryDelayMs(1, undefined, 0)).toBe(2000)
    expect(retryDelayMs(2, undefined, 0)).toBe(4000)
    expect(retryDelayMs(3, undefined, 0)).toBe(8000)
  })

  it('caps header-less delay at 30s', () => {
    expect(retryDelayMs(20, undefined, 0)).toBe(RETRY_MAX_DELAY_NO_HEADERS_MS)
  })
})
