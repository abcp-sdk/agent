import type { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { dbBackend, nowStr, q, rawAll, rawRun } from './db-client.js'
import type { FileRecord } from './files.js'

/**
 * File metadata + sha256 dedup index, stored in the `agent_files` TABLE.
 *
 * Chosen over NATS KV so durable file state (the metadata mapping) leaves NATS
 * together with the bytes. TENANT-SCOPED: a file is content-addressed within a
 * tenant, so the same bytes under two tenants resolve to two independent codes
 * (dedup is per `(tenant, sha256)`). This keeps the s3 mode semantics identical
 * to the NATS mode (whose object names and KV keys are already tenant-scoped)
 * and means a cross-tenant upload can never resolve to another tenant's bytes.
 *
 * Used by the `s3` blob backend; the `nats` backend keeps using the
 * `abc-files-meta` KV bucket (see files.ts).
 */

function toRecord(r: Record<string, unknown>): FileRecord {
  return {
    code: String(r['code'] ?? ''),
    sha256: String(r['sha256'] ?? ''),
    name: String(r['name'] ?? ''),
    mime: String(r['mime'] ?? ''),
    size: Number(r['size'] ?? 0),
    uploader_session: String(r['uploader_session'] ?? ''),
    created_at: String(r['created_at'] ?? ''),
    width: optNum(r['width']),
    height: optNum(r['height']),
    duration_ms: optNum(r['duration_ms']),
    thumb_code: optStr(r['thumb_code']),
    thumbhash: optStr(r['thumbhash']),
  }
}

function optNum(v: unknown): number | null {
  return v === undefined || v === null || v === '' ? null : Number(v)
}

function optStr(v: unknown): string | null {
  return v === undefined || v === null ? null : String(v)
}

export const FilesDb = {
  /** Insert metadata if absent (dedup by (tenant, sha)). Returns the STORED
   *  record (the existing one when the sha was already present for the tenant). */
  upsert(
    db: Db,
    tenant: string,
    record: FileRecord,
  ): ResultAsync<FileRecord, string> {
    const pg = dbBackend(db) === 'pg'
    const insert = pg
      ? `INSERT INTO agent_files (code, tenant, sha256, name, mime, size, uploader_session, created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant, sha256) DO NOTHING`
      : `INSERT OR IGNORE INTO agent_files (code, tenant, sha256, name, mime, size, uploader_session, created_at)
         VALUES (?,?,?,?,?,?,?,?)`
    const params = [
      record.code,
      tenant,
      record.sha256,
      record.name,
      record.mime,
      record.size,
      record.uploader_session,
      record.created_at || nowStr(),
    ]
    const bySha = pg
      ? 'SELECT * FROM agent_files WHERE tenant = $1 AND sha256 = $2'
      : 'SELECT * FROM agent_files WHERE tenant = ? AND sha256 = ?'
    return q(async () => {
      await rawRun(db, insert, params)
      // Re-read: on a (tenant, sha) collision the ORIGINAL row wins (the
      // insert was a no-op), so callers always get the canonical code for
      // these bytes within the tenant.
      const rows = await rawAll(db, bySha, [tenant, record.sha256])
      return rows[0] === undefined ? record : toRecord(rows[0])
    }, 'file upsert')
  },

  /** Look up a record by code within a tenant. */
  byCode(
    db: Db,
    tenant: string,
    code: string,
  ): ResultAsync<FileRecord | null, string> {
    const sql =
      dbBackend(db) === 'pg'
        ? 'SELECT * FROM agent_files WHERE tenant = $1 AND code = $2'
        : 'SELECT * FROM agent_files WHERE tenant = ? AND code = ?'
    return q(
      () =>
        rawAll(db, sql, [tenant, code]).then(rows =>
          rows[0] === undefined ? null : toRecord(rows[0]),
        ),
      'file by code',
    )
  },

  /** Look up a record by sha256 within a tenant (dedup). */
  bySha(
    db: Db,
    tenant: string,
    sha256: string,
  ): ResultAsync<FileRecord | null, string> {
    const sql =
      dbBackend(db) === 'pg'
        ? 'SELECT * FROM agent_files WHERE tenant = $1 AND sha256 = $2'
        : 'SELECT * FROM agent_files WHERE tenant = ? AND sha256 = ?'
    return q(
      () =>
        rawAll(db, sql, [tenant, sha256]).then(rows =>
          rows[0] === undefined ? null : toRecord(rows[0]),
        ),
      'file by sha',
    )
  },

  /** Patch the DERIVED media columns of a stored file. Only the provided
   *  (non-`undefined`) keys are written in one UPDATE, so a probe that only
   *  learned dimensions never clobbers a previously-stored thumbnail. */
  updateMedia(
    db: Db,
    tenant: string,
    code: string,
    patch: Partial<
      Pick<
        FileRecord,
        'width' | 'height' | 'duration_ms' | 'thumb_code' | 'thumbhash' | 'mime'
      >
    >,
  ): ResultAsync<void, string> {
    const pg = dbBackend(db) === 'pg'
    const cols: string[] = []
    const vals: unknown[] = []
    for (const key of [
      'width',
      'height',
      'duration_ms',
      'thumb_code',
      'thumbhash',
      'mime',
    ] as const) {
      const v = patch[key]
      if (v === undefined) continue
      cols.push(key)
      vals.push(v)
    }
    if (cols.length === 0) {
      return q(async () => undefined, 'file update media')
    }
    const assigns = cols
      .map((c, i) => `${c} = ${pg ? `$${i + 1}` : '?'}`)
      .join(', ')
    const where = pg
      ? `tenant = $${cols.length + 1} AND code = $${cols.length + 2}`
      : 'tenant = ? AND code = ?'
    const sql = `UPDATE agent_files SET ${assigns} WHERE ${where}`
    return q(
      () => rawRun(db, sql, [...vals, tenant, code]),
      'file update media',
    )
  },
}
