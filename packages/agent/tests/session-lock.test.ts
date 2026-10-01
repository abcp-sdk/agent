import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { natsToken } from '../src/bus.js'
import {
  claimLease,
  claimLeaseOutcome,
  clearActiveRun,
  INSTANCE_ID,
  isClaimError,
  readActiveRun,
  readLeaseOwner,
  readSessionStatus,
  reconcileOwnLeases,
  releaseLease,
  renewLease,
  startHeartbeat,
  updateLease,
} from '../src/session-lock.js'

const T = 't1'

/** In-memory bus stub for the lock surface. Tracks create-vs-existing so
 *  claimLease (kvCreate) fails when the key is already present. */
function fakeBus() {
  const kv = new Map<string, string>()
  const bus = {
    kvCreate: (_b: string, key: string, value: string) => {
      if (kv.has(key)) return Promise.resolve(null)
      kv.set(key, value)
      return Promise.resolve(1)
    },
    kvGet: (_b: string, key: string) => Promise.resolve(kv.get(key) ?? null),
    kvPut: (_b: string, key: string, value: string) => {
      kv.set(key, value)
      return Promise.resolve()
    },
    kvDelete: (_b: string, key: string) => {
      kv.delete(key)
      return Promise.resolve()
    },
    kvWatch: () => {
      // Minimal watch: replay current keys once, then end.
      const entries = [...kv.entries()]
      const stream = (async function* () {
        for (const [key, value] of entries) {
          yield { key, value, deleted: false, revision: 1, isUpdate: false }
        }
      })()
      return Promise.resolve({ stream, stop: () => Promise.resolve() })
    },
  }
  return { bus: bus as unknown as Bus, kv }
}

describe('session lock', () => {
  it('claim stamps this instance id; a second claim is refused', async () => {
    const { bus, kv } = fakeBus()
    expect(await claimLease(bus, T, 's')).toBe(true)
    expect(await readLeaseOwner(bus, T, 's')).toBe(INSTANCE_ID)
    // Second claim while held → refused.
    expect(await claimLease(bus, T, 's')).toBe(false)
    // Release → claimable again.
    await releaseLease(bus, T, 's')
    expect(kv.has(`t.${T}.${natsToken('s')}`)).toBe(false)
    expect(await claimLease(bus, T, 's')).toBe(true)
  })

  it('claimLeaseOutcome: held when fresh, busy when any key is present', async () => {
    const { bus } = fakeBus()
    expect(await claimLeaseOutcome(bus, T, 's')).toBe('held')
    // Key present (even owned by us) → busy, so a concurrent invocation on the
    // same replica is never granted the session.
    expect(await claimLeaseOutcome(bus, T, 's')).toBe('busy')
    // Key present + owned by another instance → busy.
    const other = fakeBus()
    other.kv.set(
      `t.${T}.${natsToken('o')}`,
      JSON.stringify({ owner: 'other-instance' }),
    )
    expect(await claimLeaseOutcome(other.bus, T, 'o')).toBe('busy')
    expect(isClaimError('busy')).toBe(false)
    expect(isClaimError('error')).toBe(true)
  })

  it('claimLeaseOutcome: transient create failure with an ABSENT key → error', async () => {
    const { bus, kv } = fakeBus()
    // Simulate a transient kvCreate failure (returns null) with no key present.
    ;(bus as unknown as { kvCreate: () => Promise<null> }).kvCreate = () =>
      Promise.resolve(null)
    const outcome = await claimLeaseOutcome(bus, T, 's')
    expect(outcome).toBe('error')
    expect(kv.has(`t.${T}.${natsToken('s')}`)).toBe(false)
  })

  it('claimLeaseOutcome: transient create failure with a PRESENT foreign key → busy', async () => {
    const { bus, kv } = fakeBus()
    kv.set(
      `t.${T}.${natsToken('s')}`,
      JSON.stringify({ owner: 'other-instance' }),
    )
    ;(bus as unknown as { kvCreate: () => Promise<null> }).kvCreate = () =>
      Promise.resolve(null)
    expect(await claimLeaseOutcome(bus, T, 's')).toBe('busy')
  })

  it('claimLeaseOutcome: unreadable key on probe → error (never false busy)', async () => {
    const { bus } = fakeBus()
    ;(bus as unknown as { kvCreate: () => Promise<null> }).kvCreate = () =>
      Promise.resolve(null)
    ;(bus as unknown as { kvGet: () => Promise<null> }).kvGet = () =>
      Promise.reject(new Error('nats down'))
    expect(await claimLeaseOutcome(bus, T, 's')).toBe('error')
  })

  it('renewLease: held on own key, lost on a foreign key, recreates an absent key', async () => {
    const { bus, kv } = fakeBus()
    // Absent key = the lease merely expired (no foreign owner) → recreate, held.
    expect(await renewLease(bus, T, 's', { runId: 'r1' })).toBe('held')
    expect(kv.get(`t.${T}.${natsToken('s')}`)).toContain('"runId":"r1"')
    // Own key → still held.
    expect(await renewLease(bus, T, 's')).toBe('held')
    // A key owned by ANOTHER instance is the ONLY definitive loss.
    kv.set(
      `t.${T}.${natsToken('other')}`,
      JSON.stringify({ owner: 'other-instance' }),
    )
    expect(await renewLease(bus, T, 'other', { runId: 'r2' })).toBe('lost')
    expect(kv.get(`t.${T}.${natsToken('other')}`)).not.toContain('r2')
  })

  it('renewLease: a read failure is transient error, never lost', async () => {
    const { bus } = fakeBus()
    ;(bus as unknown as { kvGet: () => Promise<null> }).kvGet = () =>
      Promise.reject(new Error('nats down'))
    expect(await renewLease(bus, T, 's')).toBe('error')
  })

  it('updateLease is owner-guarded and refreshes the run anchor', async () => {
    const { bus, kv } = fakeBus()
    await claimLease(bus, T, 's')
    expect(
      await updateLease(bus, T, 's', { runId: 'r1', startedAtMs: 123 }),
    ).toBe(true)
    expect(kv.get(`t.${T}.${natsToken('s')}`)).toContain('"runId":"r1"')
    // A lock owned by ANOTHER instance must not be clobbered.
    kv.set(
      `t.${T}.${natsToken('other')}`,
      JSON.stringify({ owner: 'other-instance' }),
    )
    expect(await updateLease(bus, T, 'other', { runId: 'r2' })).toBe(false)
    expect(kv.get(`t.${T}.${natsToken('other')}`)).not.toContain('r2')
  })

  it('status is busy only while the owner heartbeat is alive', async () => {
    const { bus, kv } = fakeBus()
    await claimLease(bus, T, 's')
    // No heartbeat yet → idle (owner not alive).
    expect(await readSessionStatus(bus, T, 's')).toBe('idle')
    kv.set(INSTANCE_ID, '{"at":1}')
    expect(await readSessionStatus(bus, T, 's')).toBe('busy')
    // Heartbeat gone (crash) → idle immediately.
    kv.delete(INSTANCE_ID)
    expect(await readSessionStatus(bus, T, 's')).toBe('idle')
  })

  it('readActiveRun is owner-aware (dead owner → no live run)', async () => {
    const { bus, kv } = fakeBus()
    await claimLease(bus, T, 's')
    await updateLease(bus, T, 's', { runId: 'run-1', startedAtMs: 555 })
    // Owner heartbeat absent → not a live run.
    expect(await readActiveRun(bus, T, 's')).toBeNull()
    kv.set(INSTANCE_ID, '{"at":1}')
    expect(await readActiveRun(bus, T, 's')).toEqual({
      runId: 'run-1',
      startedAtMs: 555,
    })
  })

  it('clearActiveRun drops the run anchor but keeps the lock', async () => {
    const { bus, kv } = fakeBus()
    await claimLease(bus, T, 's')
    await updateLease(bus, T, 's', { runId: 'run-1', startedAtMs: 555 })
    expect(await clearActiveRun(bus, T, 's')).toBe(true)
    // Lock still present (owner preserved), run anchor gone.
    const raw = kv.get(`t.${T}.${natsToken('s')}`) ?? ''
    expect(raw).toContain(INSTANCE_ID)
    expect(raw).not.toContain('run-1')
  })

  it('startHeartbeat writes then renews, and stop deletes it', async () => {
    const { bus, kv } = fakeBus()
    const stop = startHeartbeat(bus)
    expect(kv.get(INSTANCE_ID)).toBeTruthy()
    stop()
    expect(kv.has(INSTANCE_ID)).toBe(false)
  })

  it('reconcile deletes only locks owned by THIS instance', async () => {
    const { bus, kv } = fakeBus()
    // A stale lock stamped with our id (crash remnant)...
    kv.set(
      `t.${T}.${natsToken('mine')}`,
      JSON.stringify({ owner: INSTANCE_ID }),
    )
    // ...and one owned by another (still-live) instance.
    kv.set(
      `t.${T}.${natsToken('theirs')}`,
      JSON.stringify({ owner: 'other-instance' }),
    )
    await reconcileOwnLeases(bus, [T], 50)
    expect(kv.has(`t.${T}.${natsToken('mine')}`)).toBe(false)
    expect(kv.has(`t.${T}.${natsToken('theirs')}`)).toBe(true)
  })
})
