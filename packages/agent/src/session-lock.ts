import { randomUUID } from 'node:crypto'
import type { Bus } from './bus.js'
import { natsToken, SESSION_LEASE_MS, tenantKVKey } from './bus.js'
import { logger } from './logger.js'

/**
 * Session run LOCK — the single authority for a session's busy/idle state.
 *
 * A session's runtime status is the presence of its run lease in
 * `abc-session-state`. This module makes that lease robust enough to be a
 * RELIABLE status source, closing the three ways the raw TTL key drifted:
 *
 *   1. Owner stamping + a per-instance HEARTBEAT. The lease value carries the
 *      owning instance id; the owner writes a heartbeat key in
 *      `abc-session-owner` with a short TTL. A session is `busy` only when its
 *      lease exists AND the owner's heartbeat is alive — so a crashed replica
 *      reads as idle IMMEDIATELY on the next status read, not after the lease
 *      TTL.
 *   2. Continuous hold. The turn loop claims ONCE per busy period and releases
 *      ONCE (see runSessionTurn), so the lease bucket no longer flaps
 *      delete→create between chained runs (which made WatchSessions emit
 *      spurious idle→busy transitions).
 *   3. Boot reconciliation. On startup, before serving, an instance clears any
 *      leases stamped with ITS OWN id (a fresh process cannot have live turns)
 *      and its own heartbeat — instant self-heal after a crash/SIGKILL.
 *
 * A read failure yields `unknown` (never a false `idle`), so a NATS blip does
 * not make working sessions look idle.
 */

/** The lease KV bucket (mirrors the SDK's LEASE_BUCKET). */
export const LEASE_BUCKET = 'abc-session-state'
/** Per-instance heartbeat bucket: one key per live agent instance. */
export const OWNER_BUCKET = 'abc-session-owner'
/** How long an instance heartbeat lives without renewal. */
export const HEARTBEAT_TTL_MS = 30_000
/** Heartbeat renewal interval (TTL/3). */
export const HEARTBEAT_RENEW_MS = 10_000

/** A session's runtime status. `unknown` = the status could not be read. */
export type SessionStatus = 'busy' | 'idle' | 'unknown'

/** The value stored under a session's lease key. */
export interface LeaseValue {
  /** Instance id of the holder (see {@link INSTANCE_ID}). */
  owner: string
  /** The turn's run id (diagnostics). */
  runId?: string
  startedAtMs?: number
}

/**
 * This process's instance id. Stable for the process lifetime; unique per
 * replica (pod name + a random suffix so a fast restart cannot reuse it).
 */
export const INSTANCE_ID = `${process.env['HOSTNAME'] ?? 'agent'}-${randomUUID().slice(0, 8)}`

function leaseKey(tenant: string, sid: string): string {
  return tenantKVKey(tenant, natsToken(sid))
}

function ownerKey(instanceId: string): string {
  return instanceId
}

/** Parse a lease value; tolerate the legacy bare `"running"` string. */
function parseLease(raw: string): LeaseValue | null {
  if (raw === '') return null
  if (raw === 'running') return { owner: '' }
  try {
    const v = JSON.parse(raw) as Partial<LeaseValue>
    if (typeof v.owner !== 'string') return null
    return {
      owner: v.owner,
      ...(typeof v.runId === 'string' ? { runId: v.runId } : {}),
      ...(typeof v.startedAtMs === 'number'
        ? { startedAtMs: v.startedAtMs }
        : {}),
    }
  } catch {
    return null
  }
}

/**
 * Ensure the lease + owner buckets exist with the correct TTL, BEFORE the
 * server listens (hence before any watcher). A NATS KV bucket's TTL is fixed
 * at creation and `bus.kvWatch` would otherwise create it persistent (ttl=0),
 * leaving leases that never expire. Best-effort.
 */
export async function ensureLockBuckets(bus: Bus): Promise<void> {
  await Promise.all([
    bus
      .kvCreate(LEASE_BUCKET, 'bucket-init', '1', SESSION_LEASE_MS)
      .catch(() => null),
    bus
      .kvCreate(OWNER_BUCKET, 'bucket-init', '1', HEARTBEAT_TTL_MS)
      .catch(() => null),
  ])
}

/**
 * Atomically claim a session's run lease, stamped with this instance's id.
 * Returns the KV revision (for renew), or null when another holder owns it.
 */
export async function claimLease(
  bus: Bus,
  tenant: string,
  sid: string,
  meta?: { runId?: string; startedAtMs?: number },
): Promise<number | null> {
  const value: LeaseValue = {
    owner: INSTANCE_ID,
    ...(meta?.runId !== undefined ? { runId: meta.runId } : {}),
    ...(meta?.startedAtMs !== undefined
      ? { startedAtMs: meta.startedAtMs }
      : {}),
  }
  try {
    return await bus.kvCreate(
      LEASE_BUCKET,
      leaseKey(tenant, sid),
      JSON.stringify(value),
      SESSION_LEASE_MS,
    )
  } catch {
    return null
  }
}

/** Renew a held lease via CAS; returns the new revision or null when lost. */
export async function renewLease(
  bus: Bus,
  tenant: string,
  sid: string,
  revision: number,
): Promise<number | null> {
  const value: LeaseValue = { owner: INSTANCE_ID }
  try {
    return await bus.kvCas(
      LEASE_BUCKET,
      leaseKey(tenant, sid),
      JSON.stringify(value),
      revision,
    )
  } catch {
    return null
  }
}

/** Release a session's run lease (back to idle). */
export function releaseLease(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<void> {
  return bus.kvDelete(LEASE_BUCKET, leaseKey(tenant, sid))
}

/** True when an instance's heartbeat key is currently present. */
export async function ownerAlive(bus: Bus, owner: string): Promise<boolean> {
  if (owner === '') return false
  const raw = await bus.kvGet(OWNER_BUCKET, ownerKey(owner)).catch(() => null)
  return raw !== null && raw !== undefined
}

/** The owner instance id stamped on a session's lease, or null when there is
 *  no lease (or it is a legacy owner-less lease → ''). */
export async function readLeaseOwner(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<string | null> {
  const raw = await bus
    .kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
    .catch(() => null)
  if (raw === null || raw === undefined) return null
  const lease = parseLease(raw)
  return lease === null ? null : lease.owner
}

/**
 * Read one session's status. `busy` when the lease exists AND its owner's
 * heartbeat is alive; `idle` when absent or the owner is gone; `unknown` on a
 * read error (never a false idle).
 */
export async function readSessionStatus(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<SessionStatus> {
  let raw: string | null
  try {
    raw = await bus.kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
  } catch {
    return 'unknown'
  }
  if (raw === null || raw === undefined) return 'idle'
  const lease = parseLease(raw)
  if (lease === null) return 'idle'
  // Legacy lease with no owner: treat presence as busy (TTL still bounds it).
  if (lease.owner === '') return 'busy'
  try {
    return (await ownerAlive(bus, lease.owner)) ? 'busy' : 'idle'
  } catch {
    return 'unknown'
  }
}

/** Batch-read statuses (parallel). See {@link readSessionStatus}. */
export async function readSessionStatuses(
  bus: Bus,
  tenant: string,
  sids: readonly string[],
): Promise<Map<string, SessionStatus>> {
  const out = new Map<string, SessionStatus>()
  if (sids.length === 0) return out
  // 1) Read every lease in parallel.
  const leases = await Promise.all(
    sids.map(async sid => {
      try {
        const raw = await bus.kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
        return { sid, raw }
      } catch {
        return { sid, raw: undefined } // read error → unknown
      }
    }),
  )
  // 2) Resolve the set of distinct owners ONCE (heartbeat reads are cheap and
  //    the owner count is ~the replica count, not the session count).
  const owners = new Set<string>()
  for (const { raw } of leases) {
    if (raw === null || raw === undefined) continue
    const lease = parseLease(raw)
    if (lease !== null && lease.owner !== '') owners.add(lease.owner)
  }
  const alive = new Map<string, boolean>()
  await Promise.all(
    [...owners].map(async owner => {
      alive.set(owner, await ownerAlive(bus, owner).catch(() => true))
    }),
  )
  // 3) Map each lease to a status.
  for (const { sid, raw } of leases) {
    if (raw === undefined) {
      out.set(sid, 'unknown')
      continue
    }
    if (raw === null) {
      out.set(sid, 'idle')
      continue
    }
    const lease = parseLease(raw)
    if (lease === null) {
      out.set(sid, 'idle')
      continue
    }
    if (lease.owner === '') {
      // Legacy lease with no owner stamp: presence = busy (TTL still bounds it).
      out.set(sid, 'busy')
      continue
    }
    out.set(sid, alive.get(lease.owner) === false ? 'idle' : 'busy')
  }
  return out
}

/**
 * Start this instance's heartbeat: write + renew `OWNER_BUCKET[INSTANCE_ID]`
 * every {@link HEARTBEAT_RENEW_MS}. Returns a stop function.
 */
export function startHeartbeat(bus: Bus): () => void {
  const beat = () => {
    void bus
      .kvPut(
        OWNER_BUCKET,
        ownerKey(INSTANCE_ID),
        JSON.stringify({ at: Date.now() }),
        HEARTBEAT_TTL_MS,
      )
      .catch(err => {
        logger.warn({ err: String(err) }, 'owner heartbeat failed')
      })
  }
  beat()
  const timer = setInterval(beat, HEARTBEAT_RENEW_MS)
  timer.unref()
  return () => {
    clearInterval(timer)
    void bus.kvDelete(OWNER_BUCKET, ownerKey(INSTANCE_ID)).catch(() => {})
  }
}

/**
 * Boot reconciliation: delete every lease this instance owns (a fresh process
 * has no live turns) and this instance's stale heartbeat. Uses a KV watch over
 * the lease bucket to find them, then deletes those whose owner == INSTANCE_ID.
 * Bounded by a short deadline so a NATS stall never blocks boot.
 *
 * A watcher is used because the Bus has no key-listing API; the initial
 * snapshot replays existing keys, then we stop.
 */
export async function reconcileOwnLeases(
  bus: Bus,
  tenants: readonly string[],
  timeoutMs = 3000,
): Promise<void> {
  await Promise.all(
    tenants.map(async tenant => {
      try {
        const watch = await bus.kvWatch(LEASE_BUCKET, `t.${tenant}.>`)
        const stop = watch.stop
        const done = (async () => {
          for await (const ev of watch.stream) {
            if (ev.deleted) continue
            const lease = parseLease(ev.value)
            if (lease !== null && lease.owner === INSTANCE_ID) {
              await bus.kvDelete(LEASE_BUCKET, ev.key).catch(() => {})
              logger.warn(
                { tenant, key: ev.key },
                'reconciled stale lease owned by this instance',
              )
            }
          }
        })()
        // The watch snapshot replays synchronously-ish; give it a bounded
        // window, then stop. We do NOT await the (never-ending) stream.
        await Promise.race([done, sleep(timeoutMs)])
        await stop().catch(() => {})
      } catch (err) {
        logger.warn(
          { tenant, err: String(err) },
          'lease reconciliation failed (non-fatal)',
        )
      }
    }),
  )
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
