import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { connectDb, type Db } from '../src/db-client.js'
import { Messages } from '../src/db-messages.js'
import { Parts } from '../src/db-parts.js'
import { Sessions } from '../src/db-sessions.js'
import { persistStep } from '../src/turn-persist.js'

/**
 * A retried step MUST persist exactly ONE chain row. The turn loop re-runs the
 * model call under the SAME pre-minted `stepMessageId`, so `persistStep` is
 * idempotent by id (ON CONFLICT DO NOTHING) — the second call is a no-op. This
 * mirrors a mid-stream retry: the failed attempt's partial text is discarded
 * (accumulators reset), and only the recovered attempt is written.
 */
describe('persistStep is idempotent across a retried step', () => {
  const dbs: Db[] = []
  afterEach(async () => {
    for (const db of dbs) {
      ;(db.$client as { close?: () => void }).close?.()
    }
    dbs.length = 0
  })

  async function db(): Promise<Db> {
    const dir = mkdtempSync(join(tmpdir(), 'retry-persist-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    dbs.push(r.value)
    return r.value
  }

  const fakeBus = {
    kvPut: () => Promise.resolve(),
    kvGet: () => Promise.resolve(null),
    kvDelete: () => Promise.resolve(),
    kvCreate: () => Promise.resolve(1),
    kvCas: () => Promise.resolve(1),
    objectPut: () => Promise.resolve(),
    objectGet: () => Promise.resolve(new Uint8Array()),
    inboxPublish: () => Promise.resolve(),
    publish: () => Promise.resolve(),
  } as unknown as Bus

  it('a second persist under the same id writes no duplicate row or parts', async () => {
    const d = await db()
    const deps = { db: d, bus: fakeBus, config: {} } as never
    const sid = 'o:r:main'
    const stepId = 'assistant-step-1'
    await Sessions.create(d, 't', { name: sid })

    // First attempt persisted a partial answer (the one that "failed").
    await persistStep(deps, 't', sid, stepId, null, '', 'partial', [], [])

    // Retry: same step id, different (recovered) content. Must be a no-op.
    await persistStep(
      deps,
      't',
      sid,
      stepId,
      null,
      'thinking',
      'final answer',
      [],
      [],
    )

    const chain = await Messages.chain(d, 't', stepId, 50, null)
    expect(chain.isOk() ? chain.value.length : -1).toBe(1)

    const parts = await Parts.listByMessages(d, 't', [stepId])
    const rows = parts.isOk() ? parts.value : []
    // Only the FIRST attempt's text part exists — no duplicate from the retry.
    expect(rows.filter(p => p.type === 'text').length).toBe(1)
    expect(rows.find(p => p.type === 'text')?.data).toContain('partial')
  })

  it('a fresh step id after a retry writes exactly one new row', async () => {
    const d = await db()
    const deps = { db: d, bus: fakeBus, config: {} } as never
    const sid = 'o:r:main'
    await Sessions.create(d, 't', { name: sid })
    await persistStep(deps, 't', sid, 'step-1', null, '', 'one', [], [])
    await persistStep(deps, 't', sid, 'step-1', null, '', 'one-retry', [], [])
    await persistStep(deps, 't', sid, 'step-2', 'step-1', '', 'two', [], [])

    const chain = await Messages.chain(d, 't', 'step-2', 50, null)
    expect(chain.isOk() ? chain.value.map(m => m.id) : []).toEqual([
      'step-1',
      'step-2',
    ])
    const tip = await Sessions.tip(d, 't', sid)
    expect(tip.isOk() ? tip.value : null).toBe('step-2')
  })
})
