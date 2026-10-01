import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ok } from 'neverthrow'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Bus } from '../src/bus.js'
import { natsToken } from '../src/bus.js'
import { connectDb, type Db, rawAll, rawRun } from '../src/db-client.js'
import { Mailbox } from '../src/db-mailbox.js'
import { Messages } from '../src/db-messages.js'
import { Sessions } from '../src/db-sessions.js'
import type { AgentDeps } from '../src/session-agent.js'
import {
  handleBatch,
  handleItem,
  renewOrReclaim,
  resolveTurnReason,
  runSessionTurn,
} from '../src/session-agent.js'
import * as sessionCompact from '../src/session-compact.js'
import * as turnPrepare from '../src/turn-prepare.js'

const T = 't1'

const dbs: Db[] = []
afterEach(() => {
  for (const db of dbs) {
    const c = db.$client as { close?: () => void }
    c.close?.()
  }
  dbs.length = 0
  vi.restoreAllMocks()
})

async function db(): Promise<Db> {
  const dir = mkdtempSync(join(tmpdir(), 'turn-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value)
  return r.value
}

function fakeBus(overrides: {
  claim: (sid: string) => Promise<number | null>
  release?: (sid: string) => Promise<void>
  seedKeys?: string[]
}) {
  const kv = new Map<string, string>()
  for (const k of overrides.seedKeys ?? []) kv.set(k, '{"owner":"other"}')
  return {
    kvCreate: (_b: string, k: string, _v: string, _t: number) =>
      Promise.resolve(overrides.claim(k)).then(r => {
        if (r !== null && r !== undefined) kv.set(k, '{"owner":"x"}')
        return r
      }),
    kvCas: () => Promise.resolve(1),
    kvDelete: (_b: string, k: string) => {
      kv.delete(k)
      return Promise.resolve(overrides.release?.('') ?? undefined)
    },
    // Mirror a real KV: a key created by a successful claim is readable, so the
    // outcome probe sees it. A null create with no key = transient error.
    kvGet: (_b: string, k: string) =>
      Promise.resolve(kv.has(k) ? '{"owner":"x"}' : null),
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

describe('runSessionTurn', () => {
  it('returns without draining when another replica holds the lease', async () => {
    const drain = vi
      .spyOn(Mailbox, 'drainAll')
      .mockResolvedValue(ok([]) as never)
    // A pre-existing lock key → the claim reports `busy` (no transient retry).
    const bus = fakeBus({
      claim: () => Promise.resolve(null),
      seedKeys: [`t.${T}.${natsToken('a:b:main')}`],
    })
    await runSessionTurn(
      { db: {}, bus, config: {}, llm: {} } as AgentDeps,
      T,
      'a:b:main',
    )
    expect(drain).not.toHaveBeenCalled()
  })

  it('a transient claim error is retried and never drains (row stays pending)', async () => {
    const drain = vi
      .spyOn(Mailbox, 'drainAll')
      .mockResolvedValue(ok([]) as never)
    // kvCreate always fails AND no key is ever present → every attempt errors.
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
    vi.spyOn(Mailbox, 'drainAll').mockResolvedValue(ok([]) as never)
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
    vi.spyOn(Mailbox, 'drainAll').mockRejectedValue(new Error('db down'))
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

describe('handleBatch', () => {
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

  it('coalesces MULTIPLE compacts in one batch into a SINGLE fold', async () => {
    const compact = vi
      .spyOn(sessionCompact, 'compactSession')
      .mockResolvedValue(ok(true) as never)
    const deps = {
      db: {},
      bus: { inboxPublish: () => Promise.resolve() },
      config: {},
      llm: {},
    } as unknown as AgentDeps
    await handleBatch(deps, T, 'a:b:main', [
      { msg_type: 'compact', payload: '{}' },
      { msg_type: 'compact', payload: '{}' },
      { msg_type: 'compact', payload: '{}' },
    ])
    expect(compact).toHaveBeenCalledTimes(1)
  })

  it('N triggers in one batch run exactly ONE turn', async () => {
    const d = await db()
    const sid = 'a:b:main'
    await Sessions.create(d, T, { name: sid })
    // `runTurnOnce` calls `prepare` first; a string return ends the turn before
    // any stream, which is all we need to count turn invocations.
    const prepare = vi
      .spyOn(turnPrepare, 'prepare')
      .mockResolvedValue('stop: no provider in test')
    const deps = {
      db: d,
      bus: {
        inboxPublish: () => Promise.resolve(),
        publish: () => Promise.resolve(),
        kvGet: () => Promise.resolve(null),
        kvPut: () => Promise.resolve(),
      },
      config: {},
      llm: {},
    } as unknown as AgentDeps
    await handleBatch(deps, T, sid, [
      { msg_type: 'trigger', payload: JSON.stringify({ text: 'one' }) },
      { msg_type: 'trigger', payload: JSON.stringify({ text: 'two' }) },
      { msg_type: 'trigger', payload: JSON.stringify({ text: 'three' }) },
    ])
    // All three prompts landed in the chain…
    const tip = (await Sessions.tip(d, T, sid))._unsafeUnwrap()
    const chain = (await Messages.chain(d, T, tip!, 100, null))._unsafeUnwrap()
    expect(chain.filter(m => m.role === 'user')).toHaveLength(3)
    // …but only ONE turn ran.
    expect(prepare).toHaveBeenCalledTimes(1)
  })

  it('a batch of ONLY events runs no turn', async () => {
    const d = await db()
    const sid = 'a:b:main'
    await Sessions.create(d, T, { name: sid })
    const prepare = vi.spyOn(turnPrepare, 'prepare')
    const deps = {
      db: d,
      bus: {
        inboxPublish: () => Promise.resolve(),
        publish: () => Promise.resolve(),
        kvGet: () => Promise.resolve(null),
        kvPut: () => Promise.resolve(),
      },
      config: {},
      llm: {},
    } as unknown as AgentDeps
    await handleBatch(deps, T, sid, [
      { msg_type: 'event', payload: JSON.stringify({ content: 'e1' }) },
      { msg_type: 'event', payload: JSON.stringify({ content: 'e2' }) },
    ])
    const tip = (await Sessions.tip(d, T, sid))._unsafeUnwrap()
    const chain = (await Messages.chain(d, T, tip!, 100, null))._unsafeUnwrap()
    expect(chain.filter(m => m.role === 'event')).toHaveLength(2)
    expect(prepare).not.toHaveBeenCalled()
  })
})

describe('Mailbox.drainAll', () => {
  it('pops every pending row in arrival order and marks them consumed', async () => {
    const d = await db()
    const sid = 'a:b:main'
    for (const t of ['one', 'two', 'three']) {
      await Mailbox.enqueue(d, T, sid, 'trigger', { text: t })
    }
    const res = await Mailbox.drainAll(d, T, sid)
    const rows = res._unsafeUnwrap()
    expect(rows.map(r => r.msg_type)).toEqual(['trigger', 'trigger', 'trigger'])
    expect(
      rows.map(r => (JSON.parse(r.payload) as { text: string }).text),
    ).toEqual(['one', 'two', 'three'])
    expect(rows.every(r => r.status === 'consumed')).toBe(true)
    // A second drain is empty.
    expect((await Mailbox.drainAll(d, T, sid))._unsafeUnwrap()).toEqual([])
  })

  it('skips a future-dated effective_at row (delivery gate)', async () => {
    const d = await db()
    const sid = 'a:b:main'
    await Mailbox.enqueue(d, T, sid, 'trigger', { text: 'now' })
    const future = new Date(Date.now() + 3_600_000).toISOString()
    // Insert a future row directly (no enqueue API writes effective_at).
    await rawRun(
      d,
      `INSERT INTO mailbox (id, tenant, session_name, msg_type, source, payload, effective_at, status, created_at)
       VALUES (?,?,?,?,?,?,?,?,?)`,
      [
        'future-1',
        T,
        sid,
        'trigger',
        '',
        '{"text":"later"}',
        future,
        'pending',
        new Date().toISOString(),
      ],
    )
    const rows = (await Mailbox.drainAll(d, T, sid))._unsafeUnwrap()
    expect(rows).toHaveLength(1)
    expect((JSON.parse(rows[0]!.payload) as { text: string }).text).toBe('now')
    // The future row stays pending in the table but is NOT deliverable, so
    // `hasPending` (which mirrors the delivery gate) reads false — this is what
    // keeps the turn loop from spinning on an undeliverable row.
    const raw = await rawAll(d, `SELECT status FROM mailbox WHERE id = ?`, [
      'future-1',
    ])
    expect(String(raw[0]!.status)).toBe('pending')
    expect((await Mailbox.hasPending(d, T, sid))._unsafeUnwrap()).toBe(false)
  })
})

describe('Mailbox.pendingSessions', () => {
  it('filters to WAKE types so a lone event is not recovered', async () => {
    const d = await db()
    await Sessions.create(d, T, 'a:b:main')
    await Sessions.create(d, T, 'a:b:ctxonly')
    // One session has a pending trigger, the other only a context event.
    await Mailbox.enqueue(d, T, 'a:b:main', 'trigger', { text: 'go' })
    await Mailbox.enqueue(d, T, 'a:b:ctxonly', 'event', { content: 'note' })

    // Unfiltered: BOTH sessions are returned.
    const all = (await Mailbox.pendingSessions(d))._unsafeUnwrap()
    expect(all.map(r => r.session_name).sort()).toEqual([
      'a:b:ctxonly',
      'a:b:main',
    ])

    // Wake-type filtered: only the trigger session is recovered.
    const wake = (
      await Mailbox.pendingSessions(d, ['trigger', 'compact'])
    )._unsafeUnwrap()
    expect(wake.map(r => r.session_name)).toEqual(['a:b:main'])
  })
})

describe('renewOrReclaim', () => {
  it('stays held when the renew is held', async () => {
    const r = await renewOrReclaim(() => Promise.resolve('held'))
    expect(r).toEqual({ held: true, lost: false })
  })

  it('reports lost only on a definitive foreign-owner loss', async () => {
    const r = await renewOrReclaim(() => Promise.resolve('lost'))
    expect(r).toEqual({ held: false, lost: true })
  })

  it('treats a transient error as HELD (never aborts a live turn)', async () => {
    const r = await renewOrReclaim(() => Promise.resolve('error'))
    expect(r).toEqual({ held: true, lost: false })
  })

  it('treats a thrown renew as transient, never lost, never throws', async () => {
    const r = await renewOrReclaim(() => Promise.reject(new Error('nats down')))
    expect(r).toEqual({ held: true, lost: false })
  })
})

describe('resolveTurnReason', () => {
  it('a user abort (flag or reason) is interrupted', () => {
    expect(resolveTurnReason(true, null)).toBe('interrupted')
    expect(resolveTurnReason(false, 'user')).toBe('interrupted')
  })

  it('a lock-loss abort is locklost (resumable), not interrupted', () => {
    expect(resolveTurnReason(false, 'locklost')).toBe('locklost')
  })

  it('a clean finish with no abort is stop', () => {
    expect(resolveTurnReason(false, null)).toBe('stop')
  })
})
