import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, type Db, rawAll } from '@abcp-agent/agent'
import { Messages } from '@abcp-agent/agent'

/**
 * `deps.messageChain` semantics (packages/server/src/index.ts): `limit` is a
 * DEPTH cap, and `limit <= 0` means "no depth cap" (return the FULL chain).
 * `deps` is a closure inside `main()`, so this reproduces the exact SQL the
 * handler uses for both branches and asserts the two behaviours.
 */
function messageChain(
  db: Db,
  tenant: string,
  tip: string,
  limit: number,
): Promise<Array<{ id: string; depth: number }>> {
  const capped = Number.isFinite(limit) && limit > 0
  return rawAll(
    db,
    capped
      ? `WITH RECURSIVE chain AS (
           SELECT m.id, m.prev_id, 0 AS depth FROM messages m WHERE m.id = ? AND m.tenant = ?
           UNION ALL
           SELECT m.id, m.prev_id, c.depth + 1 FROM messages m JOIN chain c ON m.id = c.prev_id
           WHERE m.tenant = ?
         )
         SELECT id, depth FROM chain WHERE depth < ? ORDER BY depth ASC`
      : `WITH RECURSIVE chain AS (
           SELECT m.id, m.prev_id, 0 AS depth FROM messages m WHERE m.id = ? AND m.tenant = ?
           UNION ALL
           SELECT m.id, m.prev_id, c.depth + 1 FROM messages m JOIN chain c ON m.id = c.prev_id
           WHERE m.tenant = ?
         )
         SELECT id, depth FROM chain ORDER BY depth ASC`,
    capped ? [tip, tenant, tenant, limit] : [tip, tenant, tenant],
  ).then(rows => rows.map(r => ({ id: String(r['id']), depth: Number(r['depth']) })))
}

const dbs: Db[] = []
afterEach(() => {
  for (const db of dbs) (db.$client as { close?: () => void }).close?.()
  dbs.length = 0
})

async function makeDb(): Promise<Db> {
  const dir = mkdtempSync(join(tmpdir(), 'mchain-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value)
  return r.value
}

async function buildChain(db: Db, n: number): Promise<string> {
  let prev: string | null = null
  let last = ''
  for (let i = 0; i < n; i++) {
    const id = `m${i}`
    const r = await Messages.insertWithId(db, 't', id, 'assistant', prev)
    if (r.isErr()) throw new Error(r.error)
    prev = id
    last = id
  }
  return last
}

describe('messageChain depth cap', () => {
  it('limit > 0 keeps the existing depth-cap behaviour', async () => {
    const db = await makeDb()
    const tip = await buildChain(db, 1000)
    const r = await messageChain(db, 't', tip, 200)
    expect(r).toHaveLength(200)
    expect(r[0]).toEqual({ id: 'm999', depth: 0 })
    expect(r[199]).toEqual({ id: 'm800', depth: 199 })
  })

  it('limit <= 0 returns the FULL chain (no depth cap)', async () => {
    const db = await makeDb()
    const tip = await buildChain(db, 1000)
    const full = await messageChain(db, 't', tip, 0)
    expect(full).toHaveLength(1000)
    expect(full[0]).toEqual({ id: 'm999', depth: 0 })
    expect(full[999]).toEqual({ id: 'm0', depth: 999 })
  })

  it('reaches history older than the old default cap (the reported bug)', async () => {
    const db = await makeDb()
    const tip = await buildChain(db, 1000)
    // Old behaviour (limit=200) cannot see m900..m0; the uncapped walk can.
    const old = await messageChain(db, 't', tip, 200)
    expect(old.some(m => m.id === 'm100')).toBe(false)
    const full = await messageChain(db, 't', tip, 0)
    expect(full.some(m => m.id === 'm100')).toBe(true)
  })
})
