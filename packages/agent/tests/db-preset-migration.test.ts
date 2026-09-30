import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, rawAll } from '../src/db-client.js'

/**
 * The `maintainer` preset was merged into `developer`. A session that still
 * references `maintainer` would resolve to a preset that no longer exists,
 * which the agent treats as an EMPTY whitelist = ALL tools with no system
 * prompt. `connectDb` must remap the stale id.
 */
const dirs: string[] = []

function legacyDb(preset: string): string {
  const dir = mkdtempSync(join(tmpdir(), 'abcp-preset-'))
  dirs.push(dir)
  const path = join(dir, 'agent.db')
  const raw = new DatabaseSync(path)
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
  raw.exec(
    `INSERT INTO sessions (tenant, name, preset) VALUES ('default', 'acme:app:main', '${preset}')`,
  )
  raw.close()
  return path
}

afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true })
})

describe('maintainer -> developer preset migration', () => {
  it('remaps a session whose preset is maintainer', async () => {
    const path = legacyDb('maintainer')
    const res = await connectDb('sqlite', `sqlite://${path}`)
    expect(res.isOk()).toBe(true)
    const db = res._unsafeUnwrap()
    const rows = await rawAll(
      db,
      "SELECT preset FROM sessions WHERE name = 'acme:app:main'",
    )
    expect(rows[0]!['preset']).toBe('developer')
  })

  it('leaves a developer session untouched', async () => {
    const path = legacyDb('developer')
    const res = await connectDb('sqlite', `sqlite://${path}`)
    expect(res.isOk()).toBe(true)
    const db = res._unsafeUnwrap()
    const rows = await rawAll(
      db,
      "SELECT preset FROM sessions WHERE name = 'acme:app:main'",
    )
    expect(rows[0]!['preset']).toBe('developer')
  })
})
