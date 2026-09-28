import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { natsToken } from '../src/bus.js'
import { connectDb, type Db } from '../src/db-client.js'
import { Sessions } from '../src/db-sessions.js'
import { backfillMessageSeqFromKv } from '../src/session-state.js'

const dbs: Db[] = []
afterEach(() => {
  for (const db of dbs) {
    const c = db.$client as { close?: () => void }
    c.close?.()
  }
  dbs.length = 0
})

async function db(): Promise<Db> {
  const dir = mkdtempSync(join(tmpdir(), 'seq-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value)
  return r.value
}

/** KV stub backed by a Map keyed `bucket\x00key`. */
function fakeBus() {
  const kv = new Map<string, string>()
  const bus = {
    kvGet: (b: string, k: string) =>
      Promise.resolve(kv.get(`${b}\x00${k}`) ?? null),
    kvPut: (b: string, k: string, v: string) => {
      kv.set(`${b}\x00${k}`, v)
      return Promise.resolve()
    },
  }
  return { bus: bus as unknown as Bus, kv }
}

describe('Sessions.appendMessageTip', () => {
  it('moves the tip and bumps message_seq atomically, starting at 1', async () => {
    const d = await db()
    await Sessions.create(d, 't', { name: 's' })
    expect((await Sessions.get(d, 't', 's'))._unsafeUnwrap()?.message_seq).toBe(
      0,
    )
    const seq1 = await Sessions.appendMessageTip(d, 't', 's', 'm1')
    expect(seq1._unsafeUnwrap()).toBe(1)
    const seq2 = await Sessions.appendMessageTip(d, 't', 's', 'm2')
    expect(seq2._unsafeUnwrap()).toBe(2)
    const row = (await Sessions.get(d, 't', 's'))._unsafeUnwrap()
    expect(row?.tip_id).toBe('m2')
    expect(row?.message_seq).toBe(2)
  })

  it('setTip (undo) does NOT change message_seq', async () => {
    const d = await db()
    await Sessions.create(d, 't', { name: 's' })
    await Sessions.appendMessageTip(d, 't', 's', 'm1')
    await Sessions.appendMessageTip(d, 't', 's', 'm2')
    await Sessions.setTip(d, 't', 's', 'm1')
    const row = (await Sessions.get(d, 't', 's'))._unsafeUnwrap()
    expect(row?.tip_id).toBe('m1')
    expect(row?.message_seq).toBe(2)
  })

  it('a fork starts at 0 unless a seq is supplied', async () => {
    const d = await db()
    await Sessions.create(d, 't', { name: 'parent' })
    await Sessions.appendMessageTip(d, 't', 'parent', 'm1')
    await Sessions.create(d, 't', { name: 'child' })
    expect(
      (await Sessions.get(d, 't', 'child'))._unsafeUnwrap()?.message_seq,
    ).toBe(0)
  })

  it('concurrent appends yield distinct increasing counters', async () => {
    const d = await db()
    await Sessions.create(d, 't', { name: 's' })
    const seqs = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        Sessions.appendMessageTip(d, 't', 's', `m${i}`).then(r =>
          r._unsafeUnwrap(),
        ),
      ),
    )
    expect([...new Set(seqs)].length).toBe(20)
    expect(Math.max(...seqs)).toBe(20)
  })
})

describe('backfillMessageSeqFromKv', () => {
  it('seeds sessions.message_seq from the legacy KV fact counter', async () => {
    const d = await db()
    const { bus, kv } = fakeBus()
    await Sessions.create(d, 't', { name: 'a' })
    await Sessions.create(d, 't', { name: 'b' })
    kv.set(
      `abc-session-meta\x00t.t.${natsToken('a')}`,
      JSON.stringify({ tenant: 't', session_name: 'a', message_seq: 7 }),
    )
    kv.set(
      `abc-session-meta\x00t.t.${natsToken('b')}`,
      JSON.stringify({ tenant: 't', session_name: 'b', message_seq: 3 }),
    )
    await backfillMessageSeqFromKv(d, bus, ['t'])
    expect((await Sessions.get(d, 't', 'a'))._unsafeUnwrap()?.message_seq).toBe(
      7,
    )
    expect((await Sessions.get(d, 't', 'b'))._unsafeUnwrap()?.message_seq).toBe(
      3,
    )
  })

  it('never regresses a counter that already advanced on the DB', async () => {
    const d = await db()
    const { bus, kv } = fakeBus()
    await Sessions.create(d, 't', { name: 'a' })
    // DB already advanced to 10 via real appends.
    for (let i = 0; i < 10; i++)
      await Sessions.appendMessageTip(d, 't', 'a', `m${i}`)
    kv.set(
      `abc-session-meta\x00t.t.${natsToken('a')}`,
      JSON.stringify({ tenant: 't', session_name: 'a', message_seq: 4 }),
    )
    await backfillMessageSeqFromKv(d, bus, ['t'])
    expect((await Sessions.get(d, 't', 'a'))._unsafeUnwrap()?.message_seq).toBe(
      10,
    )
  })

  it('is marker-guarded (runs once)', async () => {
    const d = await db()
    const { bus, kv } = fakeBus()
    await Sessions.create(d, 't', { name: 'a' })
    kv.set(
      `abc-session-meta\x00t.t.${natsToken('a')}`,
      JSON.stringify({ tenant: 't', session_name: 'a', message_seq: 5 }),
    )
    await backfillMessageSeqFromKv(d, bus, ['t'])
    // Second run must be a no-op even if KV changes.
    kv.set(
      `abc-session-meta\x00t.t.${natsToken('a')}`,
      JSON.stringify({ tenant: 't', session_name: 'a', message_seq: 99 }),
    )
    await backfillMessageSeqFromKv(d, bus, ['t'])
    expect((await Sessions.get(d, 't', 'a'))._unsafeUnwrap()?.message_seq).toBe(
      5,
    )
  })
})
