import type { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { dbBackend, nowStr, q, rawAll, rawRun } from './db-client.js'
import type { FileRecord } from './files.js'

/**
 * File metadata + sha256 dedup index, stored in the `agent_files` TABLE.
 *
 * Chosen over NATS KV so durable file state (the metadata mapping) leaves NATS
 * together with the bytes. TENANT-FREE on purpose: a file is content-addressed
 * by sha256 and shared across every tenant, so identical bytes are stored once
 * and all tenants resolve to the same code (code itself is a global random id).
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
  }
}

export const FilesDb = {
  /** Insert metadata if absent (dedup by sha). Returns the STORED record (the
   *  existing one when the sha was already present). */
  upsert(db: Db, record: FileRecord): ResultAsync<FileRecord, string> {
    const pg = dbBackend(db) === 'pg'
    const insert = pg
      ? `INSERT INTO agent_files (code, sha256, name, mime, size, uploader_session, created_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (sha256) DO NOTHING`
      : `INSERT OR IGNORE INTO agent_files (code, sha256, name, mime, size, uploader_session, created_at)
         VALUES (?,?,?,?,?,?,?)`
    const params = [
      record.code,
      record.sha256,
      record.name,
      record.mime,
      record.size,
      record.uploader_session,
      record.created_at || nowStr(),
    ]
    const bySha = pg
      ? 'SELECT * FROM agent_files WHERE sha256 = $1'
      : 'SELECT * FROM agent_files WHERE sha256 = ?'
    return q(
      async () => {
        await rawRun(db, insert, params)
        // Re-read: on a sha collision the ORIGINAL row wins (the insert was a
        // no-op), so callers always get the canonical code for these bytes.
        const rows = await rawAll(db, bySha, [record.sha256])
        return rows[0] === undefined ? record : toRecord(rows[0])
      },
      'file upsert',
    )
  },

  /** Look up a record by code (tenant-free). */
  byCode(db: Db, code: string): ResultAsync<FileRecord | null, string> {
    const sql =
      dbBackend(db) === 'pg'
        ? 'SELECT * FROM agent_files WHERE code = $1'
        : 'SELECT * FROM agent_files WHERE code = ?'
    return q(
      () =>
        rawAll(db, sql, [code]).then(rows =>
          rows[0] === undefined ? null : toRecord(rows[0]),
        ),
      'file by code',
    )
  },

  /** Look up a record by sha256 (dedup). */
  bySha(db: Db, sha256: string): ResultAsync<FileRecord | null, string> {
    const sql =
      dbBackend(db) === 'pg'
        ? 'SELECT * FROM agent_files WHERE sha256 = $1'
        : 'SELECT * FROM agent_files WHERE sha256 = ?'
    return q(
      () =>
        rawAll(db, sql, [sha256]).then(rows =>
          rows[0] === undefined ? null : toRecord(rows[0]),
        ),
      'file by sha',
    )
  },
}
