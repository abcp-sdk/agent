import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { natsToken } from '../src/bus.js'
import {
  claimLease,
  clearActiveRun,
  INSTANCE_ID,
  readActiveRun,
  readLeaseOwner,
  readSessionStatus,
  reconcileOwnLeases,
  releaseLease,
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

  it('updateLease is owner-guarded and refreshes the run anchor', async () => {
    const { bus, kv } = fakeBus()
    // No lock yet → update fails (cannot clobber).
    expect(await updateLease(bus, T, 's', { runId: 'r1' })).toBe(false)
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
