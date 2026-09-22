import { ok } from 'neverthrow'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Bus } from '../src/bus.js'
import { Mailbox } from '../src/db-mailbox.js'
import type { AgentDeps } from '../src/session-agent.js'
import { handleItem, runSessionTurn } from '../src/session-agent.js'
import * as sessionCompact from '../src/session-compact.js'

const T = 't1'

function fakeBus(overrides: {
  claim: (sid: string) => Promise<number | null>
  release?: (sid: string) => Promise<void>
}) {
  return {
    kvCreate: (_b: string, _k: string, _v: string, _t: number) =>
      Promise.resolve(overrides.claim('')),
    kvCas: () => Promise.resolve(1),
    kvDelete: () => Promise.resolve(overrides.release?.('') ?? undefined),
    kvGet: () => Promise.resolve(null),
    kvPut: () => Promise.resolve(),
    objectPut: () => Promise.resolve(),
    objectGet: () => Promise.resolve(new Uint8Array()),
    inboxPublish: () => Promise.resolve(),
    publish: () => Promise.resolve(),
    subscribe: () =>
      Promise.resolve({
        close: () => {},
        [Symbol.asyncIterator]: () => ({
          next: async () => ({ done: true as const, value: undefined }),
        }),
      }),
  } as unknown as Bus
}

afterEach(() => {
  vi.restoreAllMocks()
})

describe('runSessionTurn', () => {
  it('returns without draining when another replica holds the lease', async () => {
    const drain = vi
      .spyOn(Mailbox, 'drainOne')
      .mockResolvedValue(ok(null) as never)
    const bus = fakeBus({ claim: () => Promise.resolve(null) })
    await runSessionTurn(
      { db: {}, bus, config: {}, llm: {} } as AgentDeps,
      T,
      'a:b:main',
    )
    expect(drain).not.toHaveBeenCalled()
  })

  it('drains the mailbox and releases the lease when drained empty', async () => {
    let released = false
    vi.spyOn(Mailbox, 'drainOne').mockResolvedValue(ok(null) as never)
    const bus = fakeBus({
      claim: () => Promise.resolve(5),
      release: () => {
        released = true
        return Promise.resolve()
      },
    })
    await runSessionTurn(
      { db: {}, bus, config: {}, llm: {} } as AgentDeps,
      T,
      'a:b:main',
    )
    expect(released).toBe(true)
  })

  it('releases the lease even when draining throws', async () => {
    let released = false
    vi.spyOn(Mailbox, 'drainOne').mockRejectedValue(new Error('db down'))
    const bus = fakeBus({
      claim: () => Promise.resolve(5),
      release: () => {
        released = true
        return Promise.resolve()
      },
    })
    await expect(
      runSessionTurn(
        { db: {}, bus, config: {}, llm: {} } as AgentDeps,
        T,
        'a:b:main',
      ),
    ).rejects.toThrow('db down')
    expect(released).toBe(true)
  })
})

describe('handleItem (compact branch)', () => {
  it('runs compactSession with reason=manual and does NOT run a turn', async () => {
    const compact = vi
      .spyOn(sessionCompact, 'compactSession')
      .mockResolvedValue(ok(true) as never)
    const deps = {
      db: {},
      bus: { inboxPublish: () => Promise.resolve() },
      config: {},
      llm: {},
    } as unknown as AgentDeps
    await handleItem(deps, T, 'a:b:main', {
      msg_type: 'compact',
      payload: JSON.stringify({ reason: 'manual' }),
    })
    expect(compact).toHaveBeenCalledWith(deps, T, 'a:b:main', 'manual')
  })
})
