import { randomUUID } from 'node:crypto'
import type { Bus } from './bus.js'
import { natsToken, SESSION_LEASE_MS, tenantKVKey } from './bus.js'
import { logger } from './logger.js'

/**
 * Session run LOCK — the single authority for a session's busy/idle state AND
 * its active-run (replay) anchor.
 *
 * The lock is ONE record in `abc-session-state`, keyed
 * `t.<tenant>.<sessionToken>`:
 *
 *   { owner, runId?, startedAtMs? }
 *
 *   - `owner` is the instance id holding the session (see {@link INSTANCE_ID}).
 *   - `runId`/`startedAtMs` identify the CURRENT run (updated at each run
 *     start), so a reconnecting client can replay exactly the live turn.
 *
 * This replaces the former THREE keys (lease + `abc-session-run` +
 * `abc-session-turn`). The durable "last turn outcome" (reason/finish/tip)
 * lives on the message FACT in `abc-session-meta` instead (this bucket's TTL is
 * 30s, too short for idlewatch's needs) — see `session-state.setTurnEnd`.
 *
 * Reliability properties:
 *   - Owner stamping + a per-instance HEARTBEAT (`abc-session-owner`, short
 *     TTL). A session is `busy` only while the record exists AND the owner's
 *     heartbeat is alive — a crashed replica reads idle IMMEDIATELY.
 *   - Continuous hold: the turn loop claims ONCE per busy period and releases
 *     ONCE, so the key never flaps delete→create between chained runs.
 *   - Owner-guarded read-modify-write for renew/run-start: a stalled former
 *     holder that wakes after its lease expired cannot clobber the new holder
 *     (it reads a different owner and stands down).
 *   - Boot reconciliation clears this instance's own crash remnants.
 *   - A read failure yields `unknown` (never a false `idle`).
 */

/** The run-lock KV bucket (mirrors the SDK's LEASE_BUCKET). */
export const LEASE_BUCKET = 'abc-session-state'
/** Per-instance heartbeat bucket: one key per live agent instance. */
export const OWNER_BUCKET = 'abc-session-owner'
/** How long an instance heartbeat lives without renewal. */
export const HEARTBEAT_TTL_MS = 30_000
/** Heartbeat renewal interval (TTL/3). */
export const HEARTBEAT_RENEW_MS = 10_000

/** A session's runtime status. `unknown` = the status could not be read. */
export type SessionStatus = 'busy' | 'idle' | 'unknown'

/** The value stored under a session's lock key. */
export interface LeaseValue {
  /** Instance id of the holder (see {@link INSTANCE_ID}). */
  owner: string
  /** The CURRENT run's id (diagnostics + replay scoping). */
  runId?: string
  /** Wall-clock start (ms) of the CURRENT run (replay window anchor). */
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

/** Parse a lock value; tolerate the legacy bare `"running"` string. */
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
 * Ensure the lock + owner buckets exist with the correct TTL, BEFORE the
 * server listens (hence before any watcher). A NATS KV bucket's TTL is fixed
 * at creation and `bus.kvWatch` would otherwise create it persistent (ttl=0),
 * leaving locks that never expire. Best-effort.
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
 * The outcome of a claim attempt:
 *   - `held`  — we now own the lock.
 *   - `busy`  — another holder owns the (not-yet-expired) key.
 *   - `error` — the claim could not be evaluated (transient KV/JetStream
 *     failure). NOT a definitive "occupied"; callers must retry rather than
 *     silently drop the wake.
 */
export type ClaimOutcome = 'held' | 'busy' | 'error'

/**
 * Atomically claim a session's lock, stamped with this instance's id.
 *
 * The SDK's `kvCreate` collapses EVERY failure (key-exists AND transient
 * network/JetStream errors) to `null`, so a `null` result alone cannot tell
 * "another holder owns it" from "the claim call failed". We disambiguate by
 * probing the key:
 *   - key present → `busy` (the create failed because the key exists — even
 *     when stamped with our own id, a pre-existing hold must not be treated as
 *     a fresh acquire, or two invocations on one replica would run together)
 *   - key absent → the create failed for a transient reason → `error`
 * A probe failure is also `error` (never a false `busy`, which would drop the
 * wake permanently).
 */
export async function claimLeaseOutcome(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<ClaimOutcome> {
  try {
    const rev = await bus.kvCreate(
      LEASE_BUCKET,
      leaseKey(tenant, sid),
      JSON.stringify({ owner: INSTANCE_ID } satisfies LeaseValue),
      SESSION_LEASE_MS,
    )
    if (rev !== null) return 'held'
  } catch {
    // Fall through to the probe below.
  }
  // kvCreate returned null OR threw: distinguish occupied from transient.
  let raw: string | null
  try {
    raw = await bus.kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
  } catch {
    return 'error'
  }
  // Any present key (ours or another's) means the create lost to an existing
  // hold → busy. A concurrent invocation must NOT be granted the session.
  return raw === null || raw === undefined ? 'error' : 'busy'
}

/**
 * Boolean convenience wrapper around {@link claimLeaseOutcome}: true only when
 * we definitively hold the lock. Transient failures collapse to `false` — use
 * `claimLeaseOutcome` when the caller must retry on transient errors.
 */
export async function claimLease(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<boolean> {
  return (await claimLeaseOutcome(bus, tenant, sid)) === 'held'
}

/** True when a claim failed transiently (worth retrying), not merely busy. */
export function isClaimError(outcome: ClaimOutcome): boolean {
  return outcome === 'error'
}

/**
 * The outcome of a lease RENEW:
 *   - `held`  — we (still) own the lock; its TTL was refreshed.
 *   - `lost`  — a DEFINITIVE loss: the key is owned by ANOTHER instance. This
 *     is the ONLY outcome that justifies aborting an in-flight turn.
 *   - `error` — the renew could not be evaluated (transient KV/network
 *     failure). NOT a loss; the caller must keep the turn running and retry.
 *
 * Distinguishing `lost` from `error` closes the failure that killed live turns:
 * a transient NATS/JetStream timeout during renew was collapsed into "another
 * replica owns it" and aborted a running turn on a SINGLE-replica deployment.
 */
export type LeaseRenewOutcome = 'held' | 'lost' | 'error'

/**
 * Owner-guarded read-modify-write that refreshes the lock's TTL and merges
 * `fields` (e.g. the current run id/start). Serialized per session so a renew
 * and a run-start update never interleave.
 *
 * Only a key owned by a DIFFERENT instance yields `lost`; an absent key is
 * re-created (the lease merely expired, no other holder), and any read/write
 * failure yields `error` (transient). A stalled former holder therefore cannot
 * clobber the new owner, and a transient fault never masquerades as a loss.
 */
const updateLocks = new Map<string, Promise<LeaseRenewOutcome>>()
export function renewLease(
  bus: Bus,
  tenant: string,
  sid: string,
  fields: { runId?: string; startedAtMs?: number; clearRun?: boolean } = {},
): Promise<LeaseRenewOutcome> {
  const key = `${tenant}\n${sid}`
  const prev = updateLocks.get(key) ?? Promise.resolve('held')
  const next = prev
    .catch((): LeaseRenewOutcome => 'error')
    .then(async (): Promise<LeaseRenewOutcome> => {
      let raw: string | null
      try {
        raw = await bus.kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
      } catch {
        return 'error'
      }
      const cur = raw === null || raw === undefined ? null : parseLease(raw)
      // A key owned by ANOTHER instance is the only definitive loss.
      if (cur !== null && cur.owner !== INSTANCE_ID) return 'lost'
      // `clearRun` drops the run anchor but keeps the lock (owner + TTL).
      const value: LeaseValue = { owner: INSTANCE_ID }
      if (fields.clearRun !== true) {
        if (fields.runId !== undefined) value.runId = fields.runId
        if (fields.startedAtMs !== undefined) {
          value.startedAtMs = fields.startedAtMs
        }
      }
      try {
        if (cur === null) {
          // The lease merely expired with no other holder (or we raced a
          // concurrent create): recreate it WITHOUT clobbering a fresh foreign
          // lock, then re-probe to classify the result.
          const rev = await bus.kvCreate(
            LEASE_BUCKET,
            leaseKey(tenant, sid),
            JSON.stringify(value),
            SESSION_LEASE_MS,
          )
          if (rev !== null) return 'held'
          const after = await bus
            .kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
            .catch(() => null)
          const reCur = after == null ? null : parseLease(after)
          if (reCur !== null && reCur.owner !== INSTANCE_ID) return 'lost'
          return 'error'
        }
        await bus.kvPut(
          LEASE_BUCKET,
          leaseKey(tenant, sid),
          JSON.stringify(value),
          SESSION_LEASE_MS,
        )
        return 'held'
      } catch {
        return 'error'
      }
    })
  updateLocks.set(key, next)
  return next
}

/**
 * Boolean convenience wrapper over {@link renewLease}: true only when the lock
 * is still OURS (`held`). Both `lost` and transient `error` collapse to false,
 * so callers that only need "did the write land" (e.g. a fire-and-forget run
 * stamp) keep working. The renew timer MUST use `renewLease` directly — a
 * transient `error` must never be treated as a loss.
 */
export function updateLease(
  bus: Bus,
  tenant: string,
  sid: string,
  fields: { runId?: string; startedAtMs?: number; clearRun?: boolean } = {},
): Promise<boolean> {
  return renewLease(bus, tenant, sid, fields).then(o => o === 'held')
}

/** Release a session's lock (back to idle). Only call while still holding it. */
export function releaseLease(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<void> {
  return bus.kvDelete(LEASE_BUCKET, leaseKey(tenant, sid))
}

/**
 * Clear the session's ACTIVE-run anchor while KEEPING the lock (owner-guarded):
 * used by undo/withdraw so a reconnecting client no longer replays the run
 * whose content is being withdrawn. The lock stays held (the aborted turn's
 * `finally` releases it), so another replica cannot claim mid-abort.
 */
export function clearActiveRun(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<boolean> {
  return updateLease(bus, tenant, sid, { clearRun: true })
}

/** True when an instance's heartbeat key is currently present. */
export async function ownerAlive(bus: Bus, owner: string): Promise<boolean> {
  if (owner === '') return false
  const raw = await bus.kvGet(OWNER_BUCKET, ownerKey(owner)).catch(() => null)
  return raw !== null && raw !== undefined
}

/** Read a session's lock record, or null when absent/unreadable. */
export async function readLease(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<LeaseValue | null> {
  const raw = await bus
    .kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
    .catch(() => null)
  return raw === null || raw === undefined ? null : parseLease(raw)
}

/** The owner instance id stamped on a session's lock, or null when there is
 *  no lock (or it is a legacy owner-less record → ''). */
export async function readLeaseOwner(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<string | null> {
  const lease = await readLease(bus, tenant, sid)
  return lease === null ? null : lease.owner
}

/**
 * The session's ACTIVE run, or null when idle. Owner-aware: a lock whose
 * owner's heartbeat is dead is NOT a live run (so replay never anchors on a
 * crash remnant). Returns the run id + start time for the replay window.
 */
export async function readActiveRun(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<{ runId: string; startedAtMs: number } | null> {
  const lease = await readLease(bus, tenant, sid)
  if (lease === null || lease.owner === '' || lease.runId === undefined) {
    return null
  }
  const alive = await ownerAlive(bus, lease.owner).catch(() => false)
  if (!alive) return null
  return {
    runId: lease.runId,
    startedAtMs: lease.startedAtMs ?? Date.now(),
  }
}

/**
 * Read one session's status. `busy` when the lock exists AND its owner's
 * heartbeat is alive; `idle` when absent or the owner is gone; `unknown` on a
 * read error (never a false idle).
 */
export async function readSessionStatus(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<SessionStatus> {
  let lease: LeaseValue | null
  try {
    const raw = await bus.kvGet(LEASE_BUCKET, leaseKey(tenant, sid))
    if (raw === null || raw === undefined) return 'idle'
    lease = parseLease(raw)
  } catch {
    return 'unknown'
  }
  if (lease === null) return 'idle'
  // Legacy owner-less record: presence = busy (TTL still bounds it).
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
  // 1) Read every lock in parallel.
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
  // 3) Map each lock to a status.
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
 * Boot reconciliation: delete every lock this instance owns (a fresh process
 * has no live turns). Uses a KV watch over the lock bucket to find them, then
 * deletes those whose owner == INSTANCE_ID. Bounded by a short deadline so a
 * NATS stall never blocks boot.
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
                'reconciled stale lock owned by this instance',
              )
            }
          }
        })()
        await Promise.race([done, sleep(timeoutMs)])
        await stop().catch(() => {})
      } catch (err) {
        logger.warn(
          { tenant, err: String(err) },
          'lock reconciliation failed (non-fatal)',
        )
      }
    }),
  )
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}
