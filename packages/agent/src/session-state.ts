import { fireAndForget } from './async.js'
import type { Bus } from './bus.js'
import { BUCKET_SESSION_STATE, natsToken } from './bus.js'
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
  /** Creation timestamp of the newest message (PG `messages.created_at`). */
  last_message_at: string
  /** First text part of the newest message, truncated for a list preview. */
  last_message_preview: string
  /** Role of the newest message (user | assistant | event | compaction). */
  last_message_role: string
}

const PREVIEW_MAX = 80

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
  sid: string,
  fact: SessionMessageFact,
): void {
  fireAndForget(
    (async () => {
      if (!bucketEnsured) {
        bucketEnsured = true
        await ensureBucket(bus)
      }
      return bus.kvPut(
        BUCKET_SESSION_STATE,
        natsToken(sid),
        JSON.stringify(fact),
        0,
      )
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
): SessionMessageFact {
  return {
    last_message_at: createdAt,
    last_message_preview: previewText.slice(0, PREVIEW_MAX),
    last_message_role: role,
  }
}

/**
 * Read the projected message facts for the given sessions from the
 * `abc-session-meta` KV. Best-effort: a missing/malformed entry is simply
 * omitted from the map (callers fall back to `updated_at` / no preview).
 */
export async function readMessageFacts(
  bus: Bus,
  sids: readonly string[],
): Promise<Map<string, SessionMessageFact>> {
  const out = new Map<string, SessionMessageFact>()
  await Promise.all(
    sids.map(async sid => {
      const raw = await bus
        .kvGet(BUCKET_SESSION_STATE, natsToken(sid))
        .catch(() => null)
      if (raw === null || raw === undefined) return
      try {
        const v = JSON.parse(raw) as Partial<SessionMessageFact>
        out.set(sid, {
          last_message_at: String(v.last_message_at ?? ''),
          last_message_preview: String(v.last_message_preview ?? ''),
          last_message_role: String(v.last_message_role ?? ''),
        })
      } catch {
        /* ignore malformed fact */
      }
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
export async function calibrateMessageFacts(bus: Bus, db: Db): Promise<void> {
  try {
    await ensureBucket(bus)
    // Backend-neutral: sqlite uses JSON1 `json_extract`, pg uses `jsonb->>'text'`.
    // The query is driven per-backend (placeholder is `?` for sqlite, `$n` for pg).
    const rows = await rawCalibrationRows(db, PREVIEW_MAX)
    for (const r of rows) {
      const fact: SessionMessageFact = {
        last_message_at: String(r.last_message_at),
        last_message_preview: String(r.last_message_preview ?? ''),
        last_message_role: String(r.last_message_role),
      }
      await bus
        .kvPut(
          BUCKET_SESSION_STATE,
          natsToken(String(r.name)),
          JSON.stringify(fact),
          0,
        )
        .catch(err => {
          logger.warn(
            { sid: String(r.name), err: String(err) },
            'calibration kvPut failed',
          )
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
  previewMax: number,
): Promise<Record<string, unknown>[]> {
  const pgSQL = `
    SELECT s.name,
           m.created_at AS last_message_at,
           m.role AS last_message_role,
           left(p.data::jsonb->>'text', $1) AS last_message_preview
    FROM sessions s
    JOIN messages m ON m.id = s.tip_id
    JOIN LATERAL (
      SELECT data FROM parts
      WHERE message_id = m.id AND type = 'text'
      ORDER BY seq LIMIT 1
    ) p ON true`
  const sqliteSQL = `
    SELECT s.name,
           m.created_at AS last_message_at,
           m.role AS last_message_role,
           substr(json_extract(p.data, '$.text'), 1, ?) AS last_message_preview
    FROM sessions s
    JOIN messages m ON m.id = s.tip_id
    LEFT JOIN (
      SELECT message_id, MIN(seq) AS min_seq FROM parts
      WHERE type = 'text' GROUP BY message_id
    ) pmin ON pmin.message_id = m.id
    JOIN parts p ON p.message_id = m.id AND p.type = 'text' AND p.seq = pmin.min_seq`
  return rawAll(db, dbBackend(db) === 'pg' ? pgSQL : sqliteSQL, [previewMax])
}
