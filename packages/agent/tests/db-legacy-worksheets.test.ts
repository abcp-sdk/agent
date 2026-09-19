import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, rawAll, rawRun } from '../src/db-client.js'

/**
 * Legacy-database migration: a pre-removal sqlite file carries the dead
 * `worksheets` table whose `session_name REFERENCES sessions(name)` FK points
 * at a parent key that is no longer unique once `sessions` uses the composite
 * `(tenant, name)` PK. SQLite rejects — at PREPARE time — every session fork
 * (`INSERT … SELECT`), `DELETE FROM sessions`, and PK rename with
 * `foreign key mismatch - "worksheets" referencing "sessions"`.
 *
 * `connectDb` must drop that dead table so those operations work again. These
 * tests build such a legacy file by hand, run the real migration, and assert
 * the previously-broken statements now succeed.
 */
const dirs: string[] = []

function legacyDb(): string {
  const dir = mkdtempSync(join(tmpdir(), 'abcp-legacy-'))
  dirs.push(dir)
  const path = join(dir, 'agent.db')
  const raw = new DatabaseSync(path)
  raw.exec('PRAGMA foreign_keys = ON')
  // v2 (already-tenant) sessions table: composite PK, so the sessions rebuild
  // does NOT run — isolating the worksheets FK as the sole cause.
  raw.exec(`CREATE TABLE sessions (
    tenant TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    variant TEXT NOT NULL DEFAULT '',
    preset TEXT NOT NULL DEFAULT '',
    tip_id TEXT,
    max_turns INTEGER NOT NULL DEFAULT 0,
    system_prompt TEXT NOT NULL DEFAULT '',
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    last_input_tokens INTEGER NOT NULL DEFAULT 0,
    last_output_tokens INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_used_at TEXT,
    locale TEXT NOT NULL DEFAULT '',
    "group" TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (tenant, name)
  )`)
  // The legacy, now-invalid FK: sessions(name) is not unique under the
  // composite PK, so any statement touching this FK fails to prepare.
  raw.exec(`CREATE TABLE worksheets (
    id TEXT PRIMARY KEY,
    session_name TEXT NOT NULL REFERENCES sessions(name) ON DELETE CASCADE,
    ext_id TEXT NOT NULL
  )`)
  raw.exec(
    `INSERT INTO sessions (tenant, name) VALUES ('default', 'legacy-parent')`,
  )
  raw.close()
  return path
}

afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true })
})

describe('legacy worksheets migration', () => {
  it('reproduces the "foreign key mismatch" on an un-migrated legacy DB', () => {
    const path = legacyDb()
    // Before migration: the raw file rejects a session fork.
    const raw = new DatabaseSync(path)
    raw.exec('PRAGMA foreign_keys = ON')
    let failed = false
    try {
      raw.exec(
        `INSERT INTO sessions (tenant, name)
         SELECT tenant, 'child' FROM sessions WHERE name = 'legacy-parent'`,
      )
    } catch (e) {
      failed = String(e).includes('foreign key mismatch')
    }
    raw.close()
    expect(failed).toBe(true)
  })

  it('drops the dead table and restores fork / delete / rename', async () => {
    const path = legacyDb()
    const res = await connectDb('sqlite', `sqlite://${path}`)
    expect(res.isOk()).toBe(true)
    const db = res._unsafeUnwrap()

    // The dead table is gone.
    const tables = await rawAll(
      db,
      "SELECT name FROM sqlite_master WHERE type='table' AND name='worksheets'",
    )
    expect(tables).toHaveLength(0)

    // Fork (INSERT … SELECT) now works.
    await rawRun(
      db,
      `INSERT INTO sessions (tenant, name, model, "group")
       SELECT tenant, 'child', model, name
       FROM sessions WHERE tenant = 'default' AND name = 'legacy-parent'`,
    )
    const forked = await rawAll(
      db,
      "SELECT name FROM sessions WHERE name = 'child'",
    )
    expect(forked).toHaveLength(1)

    // PK rename now works.
    await rawRun(
      db,
      "UPDATE sessions SET name = 'renamed' WHERE name = 'child'",
    )
    const renamed = await rawAll(
      db,
      "SELECT name FROM sessions WHERE name = 'renamed'",
    )
    expect(renamed).toHaveLength(1)

    // DELETE now works.
    await rawRun(db, "DELETE FROM sessions WHERE name = 'renamed'")
    const gone = await rawAll(
      db,
      "SELECT name FROM sessions WHERE name = 'renamed'",
    )
    expect(gone).toHaveLength(0)
  })
})
