import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, rawAll, rawRun } from '../src/db-client.js'

/**
 * Legacy-database migration: the bundled `todo-write` tool used to create its
 * `bundled_todos` table lazily with `CREATE TABLE IF NOT EXISTS` BEFORE the
 * table carried a `tenant` column. On such a file the column can never be
 * added by `IF NOT EXISTS`, so every `DELETE/INSERT … WHERE tenant = ?` failed
 * with `no such column: tenant` — the tool errored on every call.
 *
 * The table is now OWNED by the schema bootstrap (db-client.ts), which adds the
 * missing `tenant` column additively. These tests build the legacy file by hand
 * and assert the migration restores todo writes.
 */
const dirs: string[] = []

/** A legacy sqlite file whose bundled_todos predates the tenant column. */
function legacyDb(): string {
  const dir = mkdtempSync(join(tmpdir(), 'abcp-legacy-todos-'))
  dirs.push(dir)
  const path = join(dir, 'agent.db')
  const raw = new DatabaseSync(path)
  raw.exec(`CREATE TABLE bundled_todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    content TEXT NOT NULL,
    status TEXT NOT NULL,
    priority TEXT NOT NULL,
    created_unix INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER))
  )`)
  raw.exec(
    `INSERT INTO bundled_todos (session_id, content, status, priority)
     VALUES ('legacy-session', 'old item', 'pending', 'medium')`,
  )
  raw.close()
  return path
}

afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true })
})

describe('legacy bundled_todos migration', () => {
  it('reproduces "no such column: tenant" on an un-migrated legacy DB', () => {
    const path = legacyDb()
    const raw = new DatabaseSync(path)
    let failed = false
    try {
      raw
        .prepare(
          'DELETE FROM bundled_todos WHERE tenant = ? AND session_id = ?',
        )
        .run('default', 'legacy-session')
    } catch (e) {
      failed = String(e).includes('no such column: tenant')
    }
    raw.close()
    expect(failed).toBe(true)
  })

  it('adds the tenant column and restores todo replace', async () => {
    const path = legacyDb()
    const res = await connectDb('sqlite', `sqlite://${path}`)
    expect(res.isOk()).toBe(true)
    const db = res._unsafeUnwrap()

    // The column now exists (backfilled to 'default').
    const cols = await rawAll(db, 'PRAGMA table_info(bundled_todos)')
    expect(cols.some(c => c['name'] === 'tenant')).toBe(true)

    // Replace works exactly as the host does it.
    await rawRun(
      db,
      'DELETE FROM bundled_todos WHERE tenant = ? AND session_id = ?',
      ['default', 'legacy-session'],
    )
    await rawRun(
      db,
      'INSERT INTO bundled_todos (tenant, session_id, content, status, priority) VALUES (?,?,?,?,?)',
      ['default', 'legacy-session', 'new item', 'in_progress', 'high'],
    )
    const rows = await rawAll(
      db,
      'SELECT content, status FROM bundled_todos WHERE tenant = ? AND session_id = ?',
      ['default', 'legacy-session'],
    )
    expect(rows).toHaveLength(1)
    expect(rows[0]!['content']).toBe('new item')
  })
})
