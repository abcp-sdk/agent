import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { connectDb, type Db } from '../src/db-client.js'
import { Messages } from '../src/db-messages.js'
import { Parts } from '../src/db-parts.js'
import { Providers } from '../src/db-providers.js'
import { Sessions } from '../src/db-sessions.js'
import { compactSession } from '../src/session-compact.js'

/**
 * Regression: a session whose context is dominated by TOOL RESULTS (tiny text)
 * must still compact. The old budget counted only text + 4 tokens per call, so
 * an 868k-context session measured ~40k and `compactSession` returned ok:false
 * ("nothing to compact — history is short").
 */
describe('compactSession counts tool traffic', () => {
  const dbs: Db[] = []
  afterEach(async () => {
    for (const db of dbs) {
      ;(db.$client as { close?: () => void }).close?.()
    }
    dbs.length = 0
  })

  async function db(): Promise<Db> {
    const dir = mkdtempSync(join(tmpdir(), 'compact-'))
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

  it('folds a tool-heavy chain and returns ok:true', async () => {
    const d = await db()
    const tenant = 't'
    const sid = 'o:r:main'
    await Sessions.create(d, tenant, {
      name: sid,
      model: 'p/m',
    })
    // Register a provider/model with a context limit so budgets are computed.
    await Providers.upsert(d, tenant, {
      providerId: 'p',
      capability: 'text',
      apiType: 'openai-compatible',
      baseUrl: '',
      apiKey: '',
      headers: {},
      models: [{ id: 'm', context_limit: 100_000 }],
    } as never)

    // Build several turns: small text, HUGE tool results (the dominant weight).
    const big = 'R'.repeat(60_000) // ~15k est tokens per result
    let prev: string | null = null
    for (let turn = 0; turn < 6; turn++) {
      const u = (await Messages.insert(d, tenant, 'user', prev))._unsafeUnwrap()
      await Parts.insert(d, tenant, u, 'text', 0, { text: `task ${turn}` })
      const a = (
        await Messages.insert(d, tenant, 'assistant', u)
      )._unsafeUnwrap()
      await Parts.insert(d, tenant, a, 'text', 0, { text: 'done' })
      await Parts.insert(d, tenant, a, 'tool', 1, {
        id: `call_${turn}`,
        name: 'sandbox-exec',
        input: { command: 'run' },
      })
      await Parts.insert(d, tenant, a, 'tool_result', 2, {
        tool_use_id: `call_${turn}`,
        content: big,
        metadata: null,
      })
      prev = a
    }
    await Sessions.setTip(d, tenant, sid, prev as string)

    const deps = { db: d, bus: fakeBus, config: {}, llm: {} } as never
    const r = await compactSession(deps, tenant, sid, 'manual')
    expect(r.isOk()).toBe(true)
    // ok:true means a checkpoint WAS written (not the "too short" skip).
    expect(r.isOk() ? r.value : false).toBe(true)

    // A compaction message now sits on the chain.
    const tip = await Sessions.tip(d, tenant, sid)
    const tipId = tip.isOk() ? tip.value : null
    const chain = await Messages.chain(d, tenant, tipId as string, 100, null)
    const roles = chain.isOk() ? chain.value.map(m => m.role) : []
    expect(roles).toContain('compaction')

    // The summary's `folded_tokens` reflects the TOOL weight (the old formula
    // counted only text + 4/call and would report a tiny number).
    const cmId = chain.isOk()
      ? chain.value.find(m => m.role === 'compaction')?.id
      : undefined
    const cmParts = await Parts.listByMessages(d, tenant, [cmId as string])
    const summaryPart = (cmParts.isOk() ? cmParts.value : []).find(
      p => p.type === 'summary',
    )
    const summary = JSON.parse(summaryPart?.data ?? '{}') as {
      folded_tokens?: number
    }
    expect(summary.folded_tokens).toBeGreaterThan(1000)
  })
})
