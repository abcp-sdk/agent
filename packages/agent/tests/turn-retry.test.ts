import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Bus } from '../src/bus.js'
import { connectDb, type Db } from '../src/db-client.js'
import { Messages } from '../src/db-messages.js'
import { Parts } from '../src/db-parts.js'
import { Sessions } from '../src/db-sessions.js'

// Capture every streamText() call so we can drive attempts.
const streamCalls: Array<{ opts: Record<string, unknown> }> = []
let streamFactory: (call: number, opts: Record<string, unknown>) => unknown

vi.mock('ai', async importOriginal => {
  const actual = await importOriginal<typeof import('ai')>()
  return {
    ...actual,
    streamText: (opts: Record<string, unknown>) => {
      const call = streamCalls.length
      streamCalls.push({ opts })
      return streamFactory(call, opts)
    },
  }
})

// No real backoff in tests (classification is unit-tested separately).
vi.mock('../src/llm-retry.js', async importOriginal => {
  const actual = await importOriginal<typeof import('../src/llm-retry.js')>()
  return { ...actual, retryDelayMs: () => 0 }
})

// Bypass the full prepare pipeline (providers/tools) — we only exercise the
// retry loop, so hand it a fixed context.
vi.mock('../src/turn-prepare.js', async importOriginal => {
  const actual = await importOriginal<typeof import('../src/turn-prepare.js')>()
  return {
    ...actual,
    prepare: () =>
      Promise.resolve({
        tools: {},
        system: 'sys',
        maxTurns: 5,
        model: { provider: 'test', modelId: 'm' },
        providerOptions: undefined,
        headers: undefined,
      }),
  }
})

/** An async iterable from a list of parts, or one that throws mid-way. */
function fullStream(
  parts: unknown[],
  throwAfter?: unknown,
): AsyncIterable<unknown> {
  return {
    async *[Symbol.asyncIterator]() {
      for (const p of parts) yield p
      if (throwAfter !== undefined) throw throwAfter
    },
  }
}

const textDelta = (text: string) => ({ type: 'text-delta', text, id: 't1' })
const finishStep = (finishReason = 'stop') => ({
  type: 'finish-step',
  usage: { inputTokens: 1, outputTokens: 1 },
  finishReason,
})
const finish = () => ({ type: 'finish' })

describe('runTurnOnce provider retry', () => {
  const dbs: Db[] = []
  const published: Array<{ event: string; params: Record<string, unknown> }> =
    []

  const fakeBus = {
    kvPut: () => Promise.resolve(),
    kvGet: () => Promise.resolve(null),
    kvDelete: () => Promise.resolve(),
    kvCreate: () => Promise.resolve(1),
    kvCas: () => Promise.resolve(1),
    objectPut: () => Promise.resolve(),
    objectGet: () => Promise.resolve(new Uint8Array()),
    publish: () => Promise.resolve(),
    subscribe: () =>
      Promise.resolve({
        close: () => {},
        [Symbol.asyncIterator]: () => ({ next: async () => ({ done: true }) }),
      }),
    inboxPublish: (
      _subject: string,
      frame: { event: string; params: unknown },
    ) => {
      published.push({
        event: frame.event,
        params: (frame.params ?? {}) as Record<string, unknown>,
      })
      return Promise.resolve()
    },
  } as unknown as Bus

  beforeEach(() => {
    streamCalls.length = 0
    published.length = 0
  })

  afterEach(async () => {
    for (const db of dbs) {
      ;(db.$client as { close?: () => void }).close?.()
    }
    dbs.length = 0
    vi.clearAllMocks()
  })

  async function setup(): Promise<{ deps: unknown; db: Db; sid: string }> {
    const dir = mkdtempSync(join(tmpdir(), 'turn-retry-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    const db = r.value
    dbs.push(db)
    const sid = 'o:r:main'
    await Sessions.create(db, 't', { name: sid })
    const deps = {
      db,
      bus: fakeBus,
      config: { llmMaxRetries: 0, llmStreamRetries: 3, toolTimeoutMs: 1000 },
      llm: {},
    }
    return { deps, db, sid }
  }

  it('retries a THROWN transport error and persists exactly one row', async () => {
    const { deps, db, sid } = await setup()
    const { runTurnOnce } = await import('../src/session-agent.js')

    // Attempt 1 throws mid-stream (retryable); attempt 2 succeeds.
    streamFactory = call => {
      if (call === 0) {
        return {
          fullStream: fullStream(
            [textDelta('par')],
            new Error('read ECONNRESET'),
          ),
        }
      }
      return {
        fullStream: fullStream([textDelta('final'), finishStep(), finish()]),
      }
    }

    const err = await runTurnOnce(deps as never, 't', sid)
    expect(err).toBeNull()
    expect(streamCalls.length).toBe(2)

    // A durable `retry` reset event was published for the SAME step id.
    const retries = published.filter(p => p.event === 'retry')
    expect(retries.length).toBe(1)
    const stepIds = new Set(
      published
        .filter(p => p.event === 'message-added')
        .map(p => String(p.params['message_id'])),
    )
    expect(stepIds.has(String(retries[0]!.params['message_id']))).toBe(true)

    // Exactly ONE chain row, containing ONLY the recovered attempt's text.
    const tip = await Sessions.tip(db, 't', sid)
    const tipId = tip.isOk() ? tip.value : null
    expect(tipId).not.toBeNull()
    const chain = await Messages.chain(db, 't', tipId as string, 50, null)
    expect(chain.isOk() ? chain.value.length : -1).toBe(1)
    const parts = await Parts.listByMessages(db, 't', [tipId as string])
    const textParts = (parts.isOk() ? parts.value : []).filter(
      p => p.type === 'text',
    )
    expect(textParts.length).toBe(1)
    expect(textParts[0]!.data).toContain('final')
    expect(textParts[0]!.data).not.toContain('par')
  })

  it('does NOT retry a non-retryable throw (fails the turn)', async () => {
    const { deps, sid } = await setup()
    const { runTurnOnce } = await import('../src/session-agent.js')

    streamFactory = () => ({
      fullStream: fullStream(
        [textDelta('x')],
        new Error('cannot read property x of undefined'),
      ),
    })

    const err = await runTurnOnce(deps as never, 't', sid)
    expect(typeof err).toBe('string')
    expect(streamCalls.length).toBe(1)
    expect(published.filter(p => p.event === 'retry').length).toBe(0)
  })

  it('stops after the transport retry budget and reports failure', async () => {
    const { deps, sid } = await setup()
    ;(
      deps as { config: { llmStreamRetries: number } }
    ).config.llmStreamRetries = 2
    const { runTurnOnce } = await import('../src/session-agent.js')

    // Every attempt throws a retryable transport error.
    streamFactory = () => ({
      fullStream: fullStream([], new Error('socket hang up')),
    })

    const err = await runTurnOnce(deps as never, 't', sid)
    expect(typeof err).toBe('string')
    // 1 initial + 2 retries = 3 streamText calls.
    expect(streamCalls.length).toBe(3)
    expect(published.filter(p => p.event === 'retry').length).toBe(2)
  })

  it('ends the turn with a clear error on finish_reason:length (truncated output)', async () => {
    const { deps, db, sid } = await setup()
    const { runTurnOnce } = await import('../src/session-agent.js')

    // The step streamed tool calls, then hit the output limit: the SDK emits
    // the tool-call parts but never executes them (finishReason 'length').
    streamFactory = () => ({
      fullStream: fullStream([
        textDelta('working…'),
        {
          type: 'tool-call',
          toolCallId: 'tc1',
          toolName: 'subsession-create',
          input: { name: 'x' },
        },
        {
          type: 'tool-call',
          toolCallId: 'tc2',
          toolName: 'subsession-create',
          input: { name: 'y' },
        },
        finishStep('length'),
        finish(),
      ]),
    })

    const err = await runTurnOnce(deps as never, 't', sid)
    expect(typeof err).toBe('string')
    expect(err).toContain('truncated')

    // A single model call — truncation is terminal, not retried.
    expect(streamCalls.length).toBe(1)

    // An explicit, user-visible error was published (not a cryptic SDK error).
    const errors = published.filter(p => p.event === 'error')
    expect(errors.length).toBe(1)
    expect(String(errors[0]!.params['message'])).toContain('truncated')

    // The partial step IS persisted with fully-paired tool results (one per
    // call), so the chain stays valid for any later turn.
    const tip = await Sessions.tip(db, 't', sid)
    const tipId = tip.isOk() ? tip.value : null
    expect(tipId).not.toBeNull()
    const parts = await Parts.listByMessages(db, 't', [tipId as string])
    const rows = parts.isOk() ? parts.value : []
    const calls = rows.filter(p => p.type === 'tool')
    const results = rows.filter(p => p.type === 'tool_result')
    expect(calls).toHaveLength(2)
    expect(results).toHaveLength(2)
  })
})
