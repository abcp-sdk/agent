import { err, ok } from 'neverthrow'
import { describe, expect, it, vi } from 'vitest'
import { Mailbox } from '../src/db-mailbox.js'
import type { AgentDeps } from '../src/session-agent.js'
import {
  handleMailboxMessage,
  shouldWakeMailbox,
  WAKE_MAILBOX_TYPES,
} from '../src/session-agent.js'

const T = 't1'

/** A bus that records whether the run-lease claim path was touched. */
function claimProbeBus() {
  const calls: string[] = []
  const bus = {
    kvCreate: (..._a: unknown[]) => {
      calls.push('kvCreate')
      return Promise.resolve(1)
    },
    kvGet: () => Promise.resolve(null),
    kvDelete: () => Promise.resolve(),
    kvPut: () => Promise.resolve(),
    inboxPublish: () => Promise.resolve(),
  }
  return { bus, calls }
}

describe('shouldWakeMailbox', () => {
  it('wakes for trigger and compact only', () => {
    expect(shouldWakeMailbox('trigger')).toBe(true)
    expect(shouldWakeMailbox('compact')).toBe(true)
    expect(shouldWakeMailbox('event')).toBe(false)
    expect(shouldWakeMailbox('interrupt')).toBe(false)
    expect(shouldWakeMailbox('')).toBe(false)
    expect([...WAKE_MAILBOX_TYPES]).toEqual(['trigger', 'compact'])
  })
})

describe('handleMailboxMessage wake filtering', () => {
  it('does NOT claim the lease for a context-only event', async () => {
    vi.spyOn(Mailbox, 'enqueueIdempotent').mockReturnValue(ok('e1') as never)
    const { bus, calls } = claimProbeBus()
    await handleMailboxMessage({ db: {}, bus } as unknown as AgentDeps, T, {
      id: 'e1',
      sessionName: 'a:b:main',
      type: 'event',
      payload: { text: 'ctx' },
    })
    // Enqueued, but no turn was woken (no lease claim).
    expect(calls).not.toContain('kvCreate')
    vi.restoreAllMocks()
  })

  it('DOES claim the lease for a trigger', async () => {
    vi.spyOn(Mailbox, 'enqueueIdempotent').mockReturnValue(ok('e1') as never)
    const { bus, calls } = claimProbeBus()
    // The woken turn drains an empty mailbox; stub it so the test is hermetic.
    vi.spyOn(Mailbox, 'drainAll').mockResolvedValue(ok([]) as never)
    vi.spyOn(Mailbox, 'hasPending').mockResolvedValue(ok(false) as never)
    await handleMailboxMessage({ db: {}, bus } as unknown as AgentDeps, T, {
      id: 'e1',
      sessionName: 'a:b:main',
      type: 'trigger',
      payload: { text: 'hi' },
    })
    // The wake runs asynchronously; give the claim a tick to land.
    await new Promise(r => setTimeout(r, 10))
    expect(calls).toContain('kvCreate')
    vi.restoreAllMocks()
  })
})

describe('handleMailboxMessage', () => {
  it('persists a valid envelope (handler acks via abc consumer)', async () => {
    const enqueue = vi
      .spyOn(Mailbox, 'enqueueIdempotent')
      .mockReturnValue(ok('e1') as never)
    const deps = {
      db: {},
      bus: {},
      config: {},
      llm: {},
    } as unknown as AgentDeps

    await handleMailboxMessage(deps, T, {
      id: 'e1',
      sessionName: 'a:b:main',
      type: 'event',
      payload: { text: 'hi' },
    })

    expect(enqueue).toHaveBeenCalledWith(
      deps.db,
      T,
      'e1',
      'a:b:main',
      'event',
      { text: 'hi' },
      '',
    )
    enqueue.mockRestore()
  })

  it('rethrows on foreign-key violation (no ack)', async () => {
    vi.spyOn(Mailbox, 'enqueueIdempotent').mockReturnValue(
      err('insert ... violates foreign key constraint (23503)') as never,
    )
    await expect(
      handleMailboxMessage({} as AgentDeps, T, {
        id: 'e2',
        sessionName: 'gone',
        type: 'event',
        payload: {},
      }),
    ).resolves.toBeUndefined() // FK is handled (discard, no throw)
  })

  it('rethrows on transient enqueue failure (consumer naks)', async () => {
    vi.spyOn(Mailbox, 'enqueueIdempotent').mockReturnValue(
      err('connection refused') as never,
    )
    await expect(
      handleMailboxMessage({} as AgentDeps, T, {
        id: 'e3',
        sessionName: 'a:b:main',
        type: 'event',
        payload: {},
      }),
    ).rejects.toBe('connection refused')
  })
})
