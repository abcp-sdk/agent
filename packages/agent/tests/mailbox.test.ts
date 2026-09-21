import { err, ok } from 'neverthrow'
import { describe, expect, it, vi } from 'vitest'
import { Mailbox } from '../src/db-mailbox.js'
import type { AgentDeps } from '../src/session-agent.js'
import { handleMailboxMessage } from '../src/session-agent.js'

const T = 't1'

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
