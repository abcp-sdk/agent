import { fireAndForget } from './async.js'
import type { Bus } from './bus.js'
import { BUCKET_SESSION_STATE, natsToken, tenantKVKey } from './bus.js'
import type { Db } from './db-client.js'
import { dbBackend, rawAll, rawRun } from './db-client.js'
import { Messages } from './db-messages.js'
import { Sessions } from './db-sessions.js'
import { logger } from './logger.js'

/**
 * Message-fact projection onto the abc Bus KV (`abc-session-state` bucket).
 *
 * The agent owns the message chain in PG (`sessions.tip_id` →
 * `messages.prev_id`); services without DB access (platform, UI backends)
 * need the per-session "latest message" facts for chat-list rendering
 * (preview text, timestamp). This module mirrors those facts to the shared
 * KV at every persist site.
 *
 * Deliberately NOT here: any read/unread state. `last_read_at` lives with
 * the platform (vars KV under its own extension id) — the agent neither
 * stores nor interprets it. This projection is a pure message-data mirror.
 *
 * Failure semantics: KV write failures are logged and swallowed. PG remains
 * the source of truth; a missed projection only degrades chat-list preview
 * freshness until the next message lands in that session.
 */

export interface SessionMessageFact {
  /** Tenant the session belongs to (the isolation key). */
  tenant: string
  /** Session name, carried so a KV watcher can map the hashed key back. */
  session_name: string
  /** Creation timestamp of the newest message (PG `messages.created_at`). */
  last_message_at: string
  /** First text part of the newest message, truncated for a list preview. */
  last_message_preview: string
  /** Role of the newest message (user | assistant | event | compaction). */
  last_message_role: string
  /**
   * The LAST FINISHED turn's outcome (durable, written when a turn ends).
   * Replaces the former `abc-session-turn` KV bucket: idlewatch reads `reason`
   * to tell a user-stopped session from one the model left hanging after a tool
   * call. Absent until the session's first completed turn.
   */
  last_turn_reason?: string
  last_turn_finish?: string
  last_turn_tip?: string
  last_turn_at?: string
}

const PREVIEW_MAX = 80

/** ISO timestamp for fact fields (matches the DB's `nowStr` shape). */
function nowIso(): string {
  return new Date().toISOString()
}

/**
 * Per-session serialization of FACT writes. `projectMessageFact` (a new
 * message) and `setTurnEnd` (turn outcome) are both read-modify-writes of the
 * SAME KV key; without a lock their interleaving could lose one side (e.g. the
 * turn outcome written from a stale read that drops a concurrent preview).
 * Keyed `tenant\nsid`.
 */
const factLocks = new Map<string, Promise<unknown>>()
function serializeFact<T>(
  tenant: string,
  sid: string,
  fn: () => Promise<T>,
): Promise<T> {
  const key = `${tenant}\n${sid}`
  const prev = factLocks.get(key) ?? Promise.resolve()
  const next = prev.catch(() => {}).then(fn)
  factLocks.set(key, next)
  return next
}

function factKey(tenant: string, sid: string): string {
  return tenantKVKey(tenant, natsToken(sid))
}

function parseFact(raw: string): SessionMessageFact | null {
  try {
    const v = JSON.parse(raw) as Partial<SessionMessageFact>
    return {
      tenant: String(v.tenant ?? ''),
      session_name: String(v.session_name ?? ''),
      last_message_at: String(v.last_message_at ?? ''),
      last_message_preview: String(v.last_message_preview ?? ''),
      last_message_role: String(v.last_message_role ?? ''),
      ...(v.last_turn_reason !== undefined
        ? { last_turn_reason: String(v.last_turn_reason) }
        : {}),
      ...(v.last_turn_finish !== undefined
        ? { last_turn_finish: String(v.last_turn_finish) }
        : {}),
      ...(v.last_turn_tip !== undefined
        ? { last_turn_tip: String(v.last_turn_tip) }
        : {}),
      ...(v.last_turn_at !== undefined
        ? { last_turn_at: String(v.last_turn_at) }
        : {}),
    }
  } catch {
    return null
  }
}

/**
 * Create-or-noop the fact bucket with persistent semantics (ttl=0). The
 * NATS KV bucket config is fixed at creation — whoever creates it first
 * dictates the TTL. If a bucket with a wrong (transient) TTL ever exists,
 * it must be deleted out-of-band; this call then rebuilds it correctly.
 * Failure is non-fatal: kvPut's create-or-open will still surface writes.
 */
async function ensureBucket(bus: Bus): Promise<void> {
  try {
    await bus.kvCreate(BUCKET_SESSION_STATE, 'bucket-init', '1', 0)
  } catch {
    // Bucket already exists (or transient error) — kvPut handles the rest.
  }
}
let bucketEnsured = false

export function projectMessageFact(
  bus: Bus,
  tenant: string,
  sid: string,
  fact: Omit<SessionMessageFact, 'tenant' | 'session_name'>,
  opts?: {
    /**
     * Keep the EXISTING preview/time/role. Used for SYNTHETIC triggers
     * (`source` = `system:*`, e.g. the gateway idlewatch nudge): they are not
     * user-visible messages, so they must not become the chat-list preview.
     * Without this, a synthetic nudge that fails to produce an assistant step
     * leaves its own text (or a stale user line) as the visible preview instead
     * of the last real assistant reply.
     */
    preservePreview?: boolean
  },
): void {
  fireAndForget(
    serializeFact(tenant, sid, async () => {
      if (!bucketEnsured) {
        bucketEnsured = true
        await ensureBucket(bus)
      }
      // Read the existing fact once: it carries the durable last-turn fields
      // (idlewatch) and, for a synthetic trigger, the prior preview to keep.
      const raw = await bus
        .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
        .catch(() => null)
      const existing = raw === null ? null : parseFact(raw)
      let fields = fact
      if (opts?.preservePreview === true) {
        // Keep the prior real preview; with NO prior fact, blank it entirely so
        // a synthetic nudge can never surface as a session's preview.
        fields =
          existing !== null
            ? {
                last_message_at: existing.last_message_at,
                last_message_preview: existing.last_message_preview,
                last_message_role: existing.last_message_role,
              }
            : {
                last_message_at: '',
                last_message_preview: '',
                last_message_role: '',
              }
      }
      const full: SessionMessageFact = {
        tenant,
        session_name: sid,
        ...fields,
        // The last-turn outcome is orthogonal to the message preview: carry it
        // forward on every projection so a new message does not erase it.
        ...(existing?.last_turn_reason !== undefined
          ? { last_turn_reason: existing.last_turn_reason }
          : {}),
        ...(existing?.last_turn_finish !== undefined
          ? { last_turn_finish: existing.last_turn_finish }
          : {}),
        ...(existing?.last_turn_tip !== undefined
          ? { last_turn_tip: existing.last_turn_tip }
          : {}),
        ...(existing?.last_turn_at !== undefined
          ? { last_turn_at: existing.last_turn_at }
          : {}),
      }
      await bus.kvPut(
        BUCKET_SESSION_STATE,
        factKey(tenant, sid),
        JSON.stringify(full),
        0,
      )
      // No explicit nudge here: the list watcher observes this KV write
      // directly (abc-session-meta watch), so a second signal would only
      // produce a duplicate upsert.
    }).catch(err => {
      logger.warn({ sid, err: String(err) }, 'session-state kvPut failed')
    }),
    'projectMessageFact',
  )
}

/** True when a message source is a SYNTHETIC (non-user) trigger: `system:*`.
 *  Such a message must never become the chat-list preview (see
 *  `projectMessageFact`'s `preservePreview`). A `session:*` hand-off IS real
 *  task content and stays preview-visible. */
export function isSyntheticSource(source: string): boolean {
  return source.startsWith('system:')
}

/** Build the fact from what the persist site already has in hand. */
export function factFromPersist(
  createdAt: string,
  role: string,
  previewText: string,
): Omit<SessionMessageFact, 'tenant' | 'session_name'> {
  return {
    last_message_at: createdAt,
    last_message_preview: previewText.slice(0, PREVIEW_MAX),
    last_message_role: role,
  }
}

/**
 * Overwrite the message-fact projection for one session (used when the tip
 * MOVES BACKWARDS on undo/revert, so the preview/time reflect the new tip).
 */
export async function writeMessageFact(
  bus: Bus,
  tenant: string,
  sid: string,
  fact: Omit<SessionMessageFact, 'tenant' | 'session_name'>,
): Promise<void> {
  return serializeFact(tenant, sid, async () => {
    if (!bucketEnsured) {
      bucketEnsured = true
      await ensureBucket(bus)
    }
    const raw = await bus
      .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
      .catch(() => null)
    const existing = raw === null ? null : parseFact(raw)
    const full: SessionMessageFact = {
      tenant,
      session_name: sid,
      ...fact,
      // Preserve the durable last-turn outcome across a preview-only rewrite.
      ...(existing?.last_turn_reason !== undefined
        ? { last_turn_reason: existing.last_turn_reason }
        : {}),
      ...(existing?.last_turn_finish !== undefined
        ? { last_turn_finish: existing.last_turn_finish }
        : {}),
      ...(existing?.last_turn_tip !== undefined
        ? { last_turn_tip: existing.last_turn_tip }
        : {}),
      ...(existing?.last_turn_at !== undefined
        ? { last_turn_at: existing.last_turn_at }
        : {}),
    }
    await bus.kvPut(
      BUCKET_SESSION_STATE,
      factKey(tenant, sid),
      JSON.stringify(full),
      0,
    )
  })
}

/**
 * Record the LAST FINISHED turn's outcome on the message fact (durable).
 * Replaces the former `abc-session-turn` KV bucket: idlewatch reads `reason`
 * to tell a user-stopped session from one the model left hanging after a tool
 * call. Read-modify-writes the fact so the message preview is preserved.
 */
export async function setTurnEnd(
  bus: Bus,
  tenant: string,
  sid: string,
  outcome: { reason: string; finish: string; tip: string },
): Promise<void> {
  return serializeFact(tenant, sid, async () => {
    if (!bucketEnsured) {
      bucketEnsured = true
      await ensureBucket(bus)
    }
    const raw = await bus
      .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
      .catch(() => null)
    const existing = raw === null ? null : parseFact(raw)
    const full: SessionMessageFact = {
      tenant,
      session_name: sid,
      last_message_at: existing?.last_message_at ?? '',
      last_message_preview: existing?.last_message_preview ?? '',
      last_message_role: existing?.last_message_role ?? '',
      last_turn_reason: outcome.reason,
      last_turn_finish: outcome.finish,
      last_turn_tip: outcome.tip,
      last_turn_at: nowIso(),
    }
    await bus.kvPut(
      BUCKET_SESSION_STATE,
      factKey(tenant, sid),
      JSON.stringify(full),
      0,
    )
  })
}

/** Remove a session's message-fact projection (session deleted). */
export function deleteMessageFact(
  bus: Bus,
  tenant: string,
  sid: string,
): Promise<void> {
  return bus.kvDelete(BUCKET_SESSION_STATE, factKey(tenant, sid))
}

/**
 * Read the projected message facts for the given sessions from the
 * `abc-session-meta` KV. Best-effort: a missing/malformed entry is simply
 * omitted from the map (callers fall back to `updated_at` / no preview).
 */
export async function readMessageFacts(
  bus: Bus,
  tenant: string,
  sids: readonly string[],
): Promise<Map<string, SessionMessageFact>> {
  const out = new Map<string, SessionMessageFact>()
  await Promise.all(
    sids.map(async sid => {
      const raw = await bus
        .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
        .catch(() => null)
      if (raw === null || raw === undefined) return
      const fact = parseFact(raw)
      if (fact !== null) out.set(sid, fact)
    }),
  )
  return out
}

// Session runtime status + the run lease now live in `session-lock.ts` (the
// single authority). Re-exported here so existing importers keep working.
export {
  claimLease,
  clearActiveRun,
  ensureLockBuckets,
  HEARTBEAT_RENEW_MS,
  HEARTBEAT_TTL_MS,
  INSTANCE_ID,
  LEASE_BUCKET,
  type LeaseValue,
  OWNER_BUCKET,
  readActiveRun,
  readLease,
  readLeaseOwner,
  readSessionStatus,
  readSessionStatuses,
  reconcileOwnLeases,
  releaseLease,
  type SessionStatus,
  startHeartbeat,
  updateLease,
} from './session-lock.js'

/**
 * One-time startup calibration: refresh the KV projection from PG so facts
 * are correct even if earlier writes were missed (agent down, KV wiped,
 * historical sessions predating this feature). One query per session tip is
 * acceptable at startup — this runs once, not per request.
 */
export async function calibrateMessageFacts(
  bus: Bus,
  tenant: string,
  db: Db,
): Promise<void> {
  try {
    await ensureBucket(bus)
    // Backend-neutral: sqlite uses JSON1 `json_extract`, pg uses `jsonb->>'text'`.
    // The query is driven per-backend (placeholder is `?` for sqlite, `$n` for pg).
    const rows = await rawCalibrationRows(db, tenant, PREVIEW_MAX)
    for (const r of rows) {
      const sid = String(r.name)
      // Repair the PREVIEW only. The counter lives on the DB row now.
      const fact: SessionMessageFact = {
        tenant,
        session_name: sid,
        last_message_at: String(r.last_message_at),
        last_message_preview: String(r.last_message_preview ?? ''),
        last_message_role: String(r.last_message_role),
      }
      await bus
        .kvPut(
          BUCKET_SESSION_STATE,
          factKey(tenant, sid),
          JSON.stringify(fact),
          0,
        )
        .catch(err => {
          logger.warn({ sid, err: String(err) }, 'calibration kvPut failed')
        })
    }
    logger.info({ sessions: rows.length }, 'message-fact calibration done')
  } catch (err) {
    // Non-fatal: projections self-heal on the next message per session.
    logger.warn({ err: String(err) }, 'message-fact calibration failed')
  }
}

/**
 * One-time migration: seed `sessions.message_seq` from the legacy KV fact
 * counter (which used to be the authority). Guarded by a marker in
 * `abcp-agent-config`; runs on EVERY boot until it succeeds. For each session
 * it takes `MAX(current DB value, KV value)` so it is idempotent and never
 * regresses a counter that already advanced on the DB. MUST run before serving
 * turns (awaited at boot) so a turn does not start from 0 and reset clients'
 * read watermarks.
 */
export async function backfillMessageSeqFromKv(
  db: Db,
  bus: Bus,
  tenants: readonly string[],
): Promise<void> {
  const markerBucket = 'abcp-agent-config'
  const markerKey = '__message_seq_backfill__'
  if ((await bus.kvGet(markerBucket, markerKey).catch(() => null)) !== null) {
    return
  }
  let failed = false
  for (const tenant of tenants) {
    try {
      const names = await rawAll(
        db,
        `SELECT name FROM sessions WHERE tenant = ?`,
        [tenant],
      )
      for (const row of names) {
        const sid = String(row.name ?? '')
        if (sid === '') continue
        const raw = await bus
          .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
          .catch(() => null)
        const kvSeq =
          raw === null
            ? 0
            : Number(
                (JSON.parse(raw) as { message_seq?: number }).message_seq ?? 0,
              )
        // Fallback ONLY when the KV counter is missing: the CHAIN LENGTH is a
        // safe LOWER bound for the counter (undo does not decrement it; forks
        // share ancestors). This repairs a session whose KV counter was lost or
        // already overwritten by calibration, without a chain walk in the
        // common case. Bounded by the same cap the history walk uses.
        let chainLen = 0
        if (kvSeq <= 0) {
          try {
            const tipRes = await Sessions.tip(db, tenant, sid)
            const tipId = tipRes.isOk() ? tipRes.value : null
            if (tipId !== null && tipId !== '') {
              const chain = await Messages.chain(
                db,
                tenant,
                tipId,
                100_000,
                null,
              )
              if (chain.isOk()) chainLen = chain.value.length
            }
          } catch {
            chainLen = 0
          }
        }
        const target = Math.max(kvSeq, chainLen)
        if (target <= 0) continue
        // `message_seq < ?` keeps it idempotent and never regresses an advanced
        // value (a concurrent turn may have already bumped the DB counter).
        const sql = `UPDATE sessions SET message_seq = ? WHERE tenant = ? AND name = ? AND message_seq < ?`
        await rawRun(db, sql, [target, tenant, sid, target]).catch(() => {})
      }
    } catch (err) {
      failed = true
      logger.warn(
        { tenant, err: String(err) },
        'message_seq backfill failed (will retry next boot)',
      )
    }
  }
  // Only mark done when every tenant succeeded, so a partial failure retries.
  if (!failed) {
    await bus.kvPut(markerBucket, markerKey, '1', 0).catch(() => {})
  }
  logger.info({ failed }, 'message_seq backfill from KV done')
}

/**
 * Fetch the per-session latest message fact for calibration. The query is
 * expressed per backend because the JSON/JSONB extraction differs:
 *   - pg:   `left(p.data::jsonb->>'text', ?)` + `JOIN LATERAL`
 *   - sqlite: JSON1 `substr(json_extract(p.data,'$.text'),1,?)` + scalar subselect
 */
export async function rawCalibrationRows(
  db: Db,
  tenant: string,
  previewMax: number,
): Promise<Record<string, unknown>[]> {
  const pgSQL = `
    SELECT s.name,
           m.created_at AS last_message_at,
           m.role AS last_message_role,
           left(p.data::jsonb->>'text', $2) AS last_message_preview
    FROM sessions s
    JOIN messages m ON m.id = s.tip_id AND m.tenant = s.tenant
    JOIN LATERAL (
      SELECT data FROM parts
      WHERE message_id = m.id AND type = 'text' AND tenant = s.tenant
      ORDER BY seq LIMIT 1
    ) p ON true
    WHERE s.tenant = $1`
  const sqliteSQL = `
    SELECT s.name,
           m.created_at AS last_message_at,
           m.role AS last_message_role,
           substr(json_extract(p.data, '$.text'), 1, ?) AS last_message_preview
    FROM sessions s
    JOIN messages m ON m.id = s.tip_id AND m.tenant = s.tenant
    LEFT JOIN (
      SELECT message_id, MIN(seq) AS min_seq FROM parts
      WHERE type = 'text' GROUP BY message_id
    ) pmin ON pmin.message_id = m.id
    JOIN parts p ON p.message_id = m.id AND p.type = 'text' AND p.seq = pmin.min_seq
    WHERE s.tenant = ?`
  return rawAll(db, dbBackend(db) === 'pg' ? pgSQL : sqliteSQL, [
    tenant,
    previewMax,
  ])
}
