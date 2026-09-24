import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, type Db } from '../src/db-client.js'
import { Messages } from '../src/db-messages.js'

/**
 * `forkBase` decides where a subsession fork anchors. The parent's tip while a
 * tool runs IS the prompt that started the current turn (the assistant step is
 * not persisted yet). Forking onto it would hand the child the parent's own
 * instruction — e.g. "start 10 subsessions" — which made every child believe it
 * owned that task. forkBase must step BACK over that prompt.
 */
describe('Messages.forkBase', () => {
  const dbs: Db[] = []
  afterEach(async () => {
    for (const db of dbs) {
      ;(db.$client as { close?: () => void }).close?.()
    }
    dbs.length = 0
  })

  async function db(): Promise<Db> {
    const dir = mkdtempSync(join(tmpdir(), 'fork-base-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    dbs.push(r.value)
    return r.value
  }

  it('steps back over a USER tip (the current-turn prompt)', async () => {
    const d = await db()
    // earlier turn: user -> assistant
    await Messages.insertWithId(d, 't', 'u1', 'user', null)
    await Messages.insertWithId(d, 't', 'a1', 'assistant', 'u1')
    // current turn: the user prompt that spawned the subsessions
    await Messages.insertWithId(d, 't', 'u2', 'user', 'a1')

    const base = await Messages.forkBase(d, 't', 'u2')
    expect(base.isOk() && base.value).toBe('a1')
  })

  it('keeps an ASSISTANT tip as-is', async () => {
    const d = await db()
    await Messages.insertWithId(d, 't', 'u1', 'user', null)
    await Messages.insertWithId(d, 't', 'a1', 'assistant', 'u1')
    const base = await Messages.forkBase(d, 't', 'a1')
    expect(base.isOk() && base.value).toBe('a1')
  })

  it('returns null when the user tip is the chain start', async () => {
    const d = await db()
    await Messages.insertWithId(d, 't', 'u1', 'user', null)
    const base = await Messages.forkBase(d, 't', 'u1')
    expect(base.isOk() && base.value).toBe(null)
  })

  it('returns null for an empty tip', async () => {
    const d = await db()
    const base = await Messages.forkBase(d, 't', null)
    expect(base.isOk() && base.value).toBe(null)
  })
})
