import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { natsToken, SESSION_LEASE_MS } from '../src/bus.js'
import {
  ensureLockBuckets,
  factFromPersist,
  HEARTBEAT_TTL_MS,
  isSyntheticSource,
  LEASE_BUCKET,
  OWNER_BUCKET,
  projectMessageFact,
  readMessageFacts,
  readSessionStatuses,
} from '../src/session-state.js'

const T = 't1'

/** In-memory bus stub exposing just the KV + publish surface used here. */
function fakeBus() {
  const kv = new Map<string, string>()
  const published: string[] = []
  const bus = {
    kvCreate: () => Promise.resolve(1),
    kvGet: (_bucket: string, key: string) =>
      Promise.resolve(kv.get(key) ?? null),
    kvPut: (_bucket: string, key: string, value: string) => {
      kv.set(key, value)
      return Promise.resolve()
    },
    publish: (subject: string) => {
      published.push(subject)
      return Promise.resolve()
    },
    kvDelete: (_bucket: string, key: string) => {
      kv.delete(key)
      return Promise.resolve()
    },
  }
  return { bus: bus as unknown as Bus, kv, published }
}

const settle = () => new Promise(r => setImmediate(r))

describe('projectMessageFact', () => {
  it('bumps message_seq monotonically and mirrors the preview', async () => {
    const { bus } = fakeBus()
    projectMessageFact(
      bus,
      T,
      'sess-seq-a',
      factFromPersist('2026-01-01T00:00:00Z', 'user', 'hello'),
    )
    await settle()
    projectMessageFact(
      bus,
      T,
      'sess-seq-a',
      factFromPersist('2026-01-01T00:00:01Z', 'assistant', 'hi there'),
    )
    await settle()

    const facts = await readMessageFacts(bus, T, ['sess-seq-a'])
    const f = facts.get('sess-seq-a')
    expect(f?.message_seq).toBe(2)
    expect(f?.last_message_role).toBe('assistant')
    expect(f?.last_message_preview).toBe('hi there')
    expect(f?.session_name).toBe('sess-seq-a')
  })

  it('keeps concurrent bumps for one session distinct', async () => {
    const { bus } = fakeBus()
    const sid = 'sess-seq-b'
    for (let i = 0; i < 5; i++) {
      projectMessageFact(
        bus,
        T,
        sid,
        factFromPersist('2026-01-01T00:00:00Z', 'assistant', `m${i}`),
      )
    }
    await settle()
    await settle()
    const facts = await readMessageFacts(bus, T, [sid])
    expect(facts.get(sid)?.message_seq).toBe(5)
  })

  it('does not double-signal: the KV watch is the only list trigger', async () => {
    const { bus, published } = fakeBus()
    projectMessageFact(
      bus,
      T,
      'sess-seq-c',
      factFromPersist('2026-01-01T00:00:00Z', 'user', 'x'),
    )
    await settle()
    // The list watcher lives on the KV bucket; no extra pub (which would
    // yield a duplicate upsert per message).
    expect(published).not.toContain('abc.session.changed')
  })
})

describe('readSessionStatuses', () => {
  it('busy only when the lease exists AND its owner heartbeat is alive', async () => {
    const { bus, kv } = fakeBus()
    // Live owner + a lease it holds → busy.
    kv.set('owner-live', '{"at":1}')
    kv.set(
      `t.${T}.${natsToken('busy-sess')}`,
      JSON.stringify({ owner: 'owner-live' }),
    )
    // Lease whose owner has NO heartbeat (crashed) → idle, not busy.
    kv.set(
      `t.${T}.${natsToken('dead-owner-sess')}`,
      JSON.stringify({ owner: 'owner-gone' }),
    )
    // No lease at all → idle.
    const statuses = await readSessionStatuses(bus, T, [
      'busy-sess',
      'dead-owner-sess',
      'idle-sess',
    ])
    expect(statuses.get('busy-sess')).toBe('busy')
    expect(statuses.get('dead-owner-sess')).toBe('idle')
    expect(statuses.get('idle-sess')).toBe('idle')
  })

  it('a legacy owner-less lease (value "running") reads busy', async () => {
    const { bus, kv } = fakeBus()
    kv.set(`t.${T}.${natsToken('legacy-sess')}`, 'running')
    const statuses = await readSessionStatuses(bus, T, ['legacy-sess'])
    expect(statuses.get('legacy-sess')).toBe('busy')
  })

  it('a KV read error reads UNKNOWN, never a false idle', async () => {
    const bus = {
      kvGet: () => Promise.reject(new Error('nats down')),
    } as unknown as Bus
    const statuses = await readSessionStatuses(bus, T, ['s'])
    expect(statuses.get('s')).toBe('unknown')
  })
})

describe('synthetic-source preview suppression', () => {
  it('isSyntheticSource flags system:* but not user/session:*', () => {
    expect(isSyntheticSource('system:idlewatch')).toBe(true)
    expect(isSyntheticSource('system:repo-mr')).toBe(true)
    expect(isSyntheticSource('user')).toBe(false)
    expect(isSyntheticSource('session:parent')).toBe(false)
    expect(isSyntheticSource('')).toBe(false)
  })

  it('preservePreview keeps the prior preview while bumping seq', async () => {
    const { bus } = fakeBus()
    const sid = 'sess-preserve'
    projectMessageFact(
      bus,
      T,
      sid,
      factFromPersist('2026-01-01T00:00:00Z', 'assistant', 'real reply'),
    )
    await settle()
    projectMessageFact(
      bus,
      T,
      sid,
      factFromPersist('2026-01-01T00:00:05Z', 'user', 'synthetic nudge'),
      { preservePreview: true },
    )
    await settle()

    const f = (await readMessageFacts(bus, T, [sid])).get(sid)
    // Preview/time/role stay from the REAL message...
    expect(f?.last_message_preview).toBe('real reply')
    expect(f?.last_message_role).toBe('assistant')
    expect(f?.last_message_at).toBe('2026-01-01T00:00:00Z')
    // ...but the monotonic counter still advances (a message WAS appended).
    expect(f?.message_seq).toBe(2)
  })

  it('preservePreview with no prior fact yields empty preview', async () => {
    const { bus } = fakeBus()
    const sid = 'sess-preserve-empty'
    projectMessageFact(
      bus,
      T,
      sid,
      factFromPersist('2026-01-01T00:00:00Z', 'user', 'nudge'),
      { preservePreview: true },
    )
    await settle()
    const f = (await readMessageFacts(bus, T, [sid])).get(sid)
    expect(f?.last_message_preview).toBe('')
    expect(f?.message_seq).toBe(1)
  })
})

describe('ensureLockBuckets', () => {
  it('creates the lease + owner buckets with their TTLs', async () => {
    const calls: Array<{ bucket: string; ttl: number }> = []
    const bus = {
      kvCreate: (bucket: string, _k: string, _v: string, ttl: number) => {
        calls.push({ bucket, ttl })
        return Promise.resolve(1)
      },
    } as unknown as Bus
    await ensureLockBuckets(bus)
    expect(calls).toContainEqual({
      bucket: LEASE_BUCKET,
      ttl: SESSION_LEASE_MS,
    })
    expect(calls).toContainEqual({
      bucket: OWNER_BUCKET,
      ttl: HEARTBEAT_TTL_MS,
    })
  })

  it('is best-effort: a kvCreate error never throws', async () => {
    const bus = {
      kvCreate: () => Promise.reject(new Error('nats down')),
    } as unknown as Bus
    await expect(ensureLockBuckets(bus)).resolves.toBeUndefined()
  })
})
