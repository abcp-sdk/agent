import { fireAndForget } from './async.js'
import type { Bus } from './bus.js'
import { BUCKET_SESSION_STATE, natsToken, tenantKVKey } from './bus.js'
import type { Db } from './db-client.js'
import { dbBackend, rawAll } from './db-client.js'
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
   * Monotonic per-session message counter, bumped once per appended message.
   * Clients subtract their locally-persisted read watermark to get the unread
   * count (read state stays client-local).
   */
  message_seq: number
}

const PREVIEW_MAX = 80

/**
 * In-process cache of the last projected `message_seq` per session. The KV
 * read-modify-write would race when two persist sites bump near-simultaneously
 * (a user prompt lands while the turn appends a step); serializing through a
 * cache keeps the counter strictly monotonic. Seeded from KV on first use and
 * by calibration at startup.
 */
const seqCache = new Map<string, number>()
/** Per-session serialization of the seq read-modify-write. */
const seqLocks = new Map<string, Promise<number>>()

function factKey(tenant: string, sid: string): string {
  return tenantKVKey(tenant, natsToken(sid))
}

function bumpSeq(bus: Bus, tenant: string, sid: string): Promise<number> {
  const prev = seqLocks.get(sid) ?? Promise.resolve(seqCache.get(sid) ?? 0)
  const next = prev
    .catch(() => 0)
    .then(async cur => {
      let base = seqCache.get(sid)
      if (base === undefined) {
        const raw = await bus
          .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
          .catch(() => null)
        base = raw === null ? 0 : (parseFact(raw)?.message_seq ?? 0)
      }
      const seq = Math.max(cur, base) + 1
      seqCache.set(sid, seq)
      return seq
    })
  seqLocks.set(sid, next)
  return next
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
      message_seq: Number(v.message_seq ?? 0),
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
  fact: Omit<SessionMessageFact, 'tenant' | 'session_name' | 'message_seq'>,
): void {
  fireAndForget(
    (async () => {
      if (!bucketEnsured) {
        bucketEnsured = true
        await ensureBucket(bus)
      }
      const seq = await bumpSeq(bus, tenant, sid)
      const full: SessionMessageFact = {
        tenant,
        session_name: sid,
        message_seq: seq,
        ...fact,
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
    })().catch(err => {
      logger.warn({ sid, err: String(err) }, 'session-state kvPut failed')
    }),
    'projectMessageFact',
  )
}

/** Build the fact from what the persist site already has in hand. */
export function factFromPersist(
  createdAt: string,
  role: string,
  previewText: string,
): Omit<SessionMessageFact, 'tenant' | 'session_name' | 'message_seq'> {
  return {
    last_message_at: createdAt,
    last_message_preview: previewText.slice(0, PREVIEW_MAX),
    last_message_role: role,
  }
}

/**
 * Overwrite the message-fact projection for one session WITHOUT bumping the
 * monotonic `message_seq`. Used when the tip MOVES BACKWARDS (undo/revert):
 * the preview/time must reflect the new tip, but clients' unread watermarks
 * (which subtract from `message_seq`) must NOT be inflated — a withdraw is not
 * a new message. Seq is preserved from the existing KV entry; it is never
 * decremented either, so the counter stays strictly monotonic.
 */
export async function writeMessageFact(
  bus: Bus,
  tenant: string,
  sid: string,
  fact: Omit<SessionMessageFact, 'tenant' | 'session_name' | 'message_seq'>,
): Promise<void> {
  if (!bucketEnsured) {
    bucketEnsured = true
    await ensureBucket(bus)
  }
  const raw = await bus
    .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
    .catch(() => null)
  const existing = raw === null ? null : parseFact(raw)
  const seq = existing?.message_seq ?? seqCache.get(sid) ?? 0
  seqCache.set(sid, seq)
  const full: SessionMessageFact = {
    tenant,
    session_name: sid,
    message_seq: seq,
    ...fact,
  }
  await bus.kvPut(
    BUCKET_SESSION_STATE,
    factKey(tenant, sid),
    JSON.stringify(full),
    0,
  )
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

/**
 * Runtime status of a session, derived from its cross-replica run LEASE
 * (`abc-session-state` bucket, same key hash as the lease helper). "busy"
 * while a turn holds the lease, "idle" otherwise. This is the SAME signal the
 * `State` RPC returns; it is surfaced on list rows so a list view needs no
 * per-row State poll. A session mid-retry (429/5xx backoff) is "busy".
 */
export type SessionStatus = 'busy' | 'idle'

/** The lease KV bucket (mirrors the SDK's LEASE_BUCKET). */
export const LEASE_BUCKET = 'abc-session-state'

/** Batch-read the runtime status for many sessions (one KV read each, parallel). */
export async function readSessionStatuses(
  bus: Bus,
  tenant: string,
  sids: readonly string[],
): Promise<Map<string, SessionStatus>> {
  const out = new Map<string, SessionStatus>()
  await Promise.all(
    sids.map(async sid => {
      const raw = await bus
        .kvGet(LEASE_BUCKET, tenantKVKey(tenant, natsToken(sid)))
        .catch(() => null)
      out.set(sid, raw !== null && raw !== undefined ? 'busy' : 'idle')
    }),
  )
  return out
}

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
      // Preserve a pre-existing message_seq: calibration repairs the PREVIEW
      // only. Resetting the counter would make clients' persisted read
      // watermarks exceed it and temporarily hide genuinely-new messages.
      const existingRaw = await bus
        .kvGet(BUCKET_SESSION_STATE, factKey(tenant, sid))
        .catch(() => null)
      const existing = existingRaw === null ? null : parseFact(existingRaw)
      const seq = existing?.message_seq ?? seqCache.get(sid) ?? 0
      seqCache.set(sid, seq)
      const fact: SessionMessageFact = {
        tenant,
        session_name: sid,
        message_seq: seq,
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
