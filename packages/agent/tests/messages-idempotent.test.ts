import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, type Db } from '../src/db-client.js'
import { Messages } from '../src/db-messages.js'

/**
 * Message ids are the single logical identity of a user message. Two code
 * paths persist one (the HTTP Prompt route and the agent's mailbox handlers),
 * so the insert MUST be idempotent by id — otherwise every prompt is stored
 * twice (a real regression we hit in production).
 */
describe('Messages.insertWithId is idempotent by id', () => {
  const dbs: Db[] = []
  afterEach(async () => {
    for (const db of dbs) {
      const c = db.$client as { close?: () => void }
      c.close?.()
    }
    dbs.length = 0
  })

  async function db(): Promise<Db> {
    const dir = mkdtempSync(join(tmpdir(), 'msg-id-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    dbs.push(r.value)
    return r.value
  }

  it('first insert creates, identical id is a no-op', async () => {
    const d = await db()
    const id = 'user-message-1'
    const first = await Messages.insertWithId(d, 't', id, 'user', null)
    expect(first.isOk() && first.value).toBe(true)

    // A SECOND writer with the same id (e.g. the mailbox handler after the
    // route already stored it) must NOT create a duplicate row.
    const second = await Messages.insertWithId(d, 't', id, 'user', id)
    expect(second.isOk() && second.value).toBe(false)

    const all = await Messages.chain(d, 't', id, 50, null)
    expect(all.isOk()).toBe(true)
    expect(all.isOk() ? all.value.length : -1).toBe(1)
  })

  it('a different id still inserts normally', async () => {
    const d = await db()
    await Messages.insertWithId(d, 't', 'a', 'user', null)
    const b = await Messages.insertWithId(d, 't', 'b', 'user', 'a')
    expect(b.isOk() && b.value).toBe(true)
    const chain = await Messages.chain(d, 't', 'b', 50, null)
    expect(chain.isOk() ? chain.value.map(m => m.id) : []).toEqual(['a', 'b'])
  })

  it('round-trips the message SOURCE through the chain', async () => {
    const d = await db()
    await Messages.insertWithId(d, 't', 'u1', 'user', null, 'session:parent')
    await Messages.insertWithId(d, 't', 'a1', 'assistant', 'u1', '')
    const chain = await Messages.chain(d, 't', 'a1', 50, null)
    expect(chain.isOk()).toBe(true)
    const byId = Object.fromEntries(
      (chain.isOk() ? chain.value : []).map(m => [m.id, m.source]),
    )
    expect(byId['u1']).toBe('session:parent')
    expect(byId['a1']).toBe('')
    // deltaSince must carry it too (incremental sync path).
    const delta = await Messages.deltaSince(d, 't', 'a1', 'u1', 50)
    expect(delta.isOk() && delta.value.messages[0]?.source).toBe('')
  })
})
