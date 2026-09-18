import { describe, expect, it } from 'vitest'
import { z } from 'zod'
import {
  ExtensionManifestSchema,
  MailboxRowSchema,
  SessionRowSchema,
  parse,
} from '../src/index.js'

describe('parse', () => {
  it('parses valid JSON against a schema', () => {
    const r = parse(z.object({ x: z.number() }), '{"x":1}')
    expect(r.isOk()).toBe(true)
    expect(r._unsafeUnwrap()).toEqual({ x: 1 })
  })

  it('errors on malformed JSON', () => {
    const r = parse(z.object({}), '{not json')
    expect(r.isErr()).toBe(true)
  })

  it('errors on schema mismatch', () => {
    const r = parse(z.object({ x: z.number() }), '{"x":"nope"}')
    expect(r.isErr()).toBe(true)
  })

  it('errors on null/undefined input', () => {
    expect(parse(z.object({}), null).isErr()).toBe(true)
    expect(parse(z.object({}), undefined).isErr()).toBe(true)
  })

  it('parses Uint8Array payloads', () => {
    const r = parse(
      z.object({ y: z.string() }),
      new TextEncoder().encode('{"y":"ok"}'),
    )
    expect(r.isOk()).toBe(true)
    expect(r._unsafeUnwrap()).toEqual({ y: 'ok' })
  })
})

describe('proto-derived row schemas', () => {
  it('accepts a mailbox row', () => {
    const r = MailboxRowSchema.safeParse({
      id: 'e1',
      session_name: 'a:b:main',
      msg_type: 'event',
      payload: '{"text":"hi"}',
      effective_at: null,
      status: 'pending',
      created_at: '2026-01-01 00:00:00',
      consumed_at: null,
      seq: null,
    })
    expect(r.success).toBe(true)
  })

  it('session row rejects a non-numeric token count', () => {
    const r = SessionRowSchema.safeParse({ input_tokens: 'many' })
    expect(r.success).toBe(false)
  })
})

describe('extension protocol schemas (re-exported from @abc-protocol/sdk)', () => {
  it('parses an extension manifest with tools', () => {
    const r = ExtensionManifestSchema.safeParse({
      id: 'x',
      version: '1',
      capabilities: ['tools'],
      tools: [{ name: 'read', description: 'd' }],
    })
    expect(r.success).toBe(true)
  })
})
