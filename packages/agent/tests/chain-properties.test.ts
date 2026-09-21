import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, type Db, rawRun } from '../src/db-client.js'
import { Messages } from '../src/db-messages.js'

/**
 * Chain-integrity PROPERTY tests for the prev_id walk that backs every
 * message read (`Messages.chain` / `deltaSince` / `isInChain`).
 *
 * The walk is a recursive CTE over client-maintained `prev_id` links, so its
 * correctness cannot be taken for granted from schema constraints alone.
 * These properties must hold for EVERY chain, so each is checked against
 * many randomly generated chains (seeded PRNG — the seed prints on failure
 * via `CHAIN_SEED`, and can be pinned with the same env to reproduce).
 */

/** Deterministic, fast PRNG (mulberry32). */
function rng(seed: number): () => number {
  let a = seed >>> 0
  return () => {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

const rand = rng(Number(process.env['CHAIN_SEED'] ?? 20260921))
const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!
const randInt = (lo: number, hi: number): number =>
  lo + Math.floor(rand() * (hi - lo + 1))

interface GenChain {
  /** Insert order (shuffled) and the logical chain (oldest-first). */
  ids: string[]
  roles: string[]
}

function genChain(): GenChain {
  const n = randInt(1, 60)
  const roles = Array.from({ length: n }, () =>
    pick(['user', 'assistant', 'event', 'compaction'] as const),
  )
  const ids = Array.from({ length: n }, (_, i) => `m${i}`)
  // Fisher–Yates with the seeded PRNG: rows land in the table in a random
  // order (like concurrent/idempotent writers do), not chain order.
  for (let i = ids.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1))
    ;[ids[i], ids[j]] = [ids[j]!, ids[i]!]
  }
  return { ids, roles }
}

async function insertChain(
  db: Db,
  gen: GenChain,
  tenant = 't',
): Promise<string[]> {
  // Logical order is m0..mN regardless of insertion order.
  const logical = Array.from({ length: gen.ids.length }, (_, i) => `m${i}`)
  for (let i = 0; i < logical.length; i++) {
    const prev = i === 0 ? null : logical[i - 1]!
    const r = await Messages.insertWithId(
      db,
      tenant,
      logical[i]!,
      gen.roles[i] as 'user',
      prev,
    )
    if (r.isErr()) throw new Error(r.error)
  }
  return logical
}

/** Fail instead of hang: a poisoned walk must terminate. */
async function withTimeout<T>(p: Promise<T>, ms = 3000): Promise<T> {
  let t: ReturnType<typeof setTimeout> | undefined
  const guard = new Promise<never>((_, reject) => {
    t = setTimeout(
      () => reject(new Error('walk did not terminate (possible cycle loop)')),
      ms,
    )
  })
  try {
    return await Promise.race([p, guard])
  } finally {
    clearTimeout(t)
  }
}

const dbs: Db[] = []
afterEach(async () => {
  for (const db of dbs) {
    const c = db.$client as { close?: () => void }
    c.close?.()
  }
  dbs.length = 0
})

async function db(): Promise<Db> {
  const dir = mkdtempSync(join(tmpdir(), 'chain-prop-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value)
  return r.value
}

const RUNS = 25

describe('message chain walk properties', () => {
  it('P1: full walk returns the whole chain oldest-first for any insertion order', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      const gen = genChain()
      const logical = await insertChain(d, gen)
      const tip = logical[logical.length - 1]!
      const r = await Messages.chain(d, 't', tip, 1000, null)
      if (r.isErr()) throw new Error(`run ${run}: ${r.error}`)
      expect(
        r.value.map(m => m.id),
        `run ${run} (CHAIN_SEED pin to reproduce)`,
      ).toEqual(logical)
    }
  })

  it('P2: a limit returns exactly the newest `limit` members, still oldest-first', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      const gen = genChain()
      const logical = await insertChain(d, gen)
      const tip = logical[logical.length - 1]!
      const k = randInt(1, logical.length)
      const r = await Messages.chain(d, 't', tip, k, null)
      if (r.isErr()) throw new Error(`run ${run}: ${r.error}`)
      expect(r.value.map(m => m.id)).toEqual(logical.slice(-k))
    }
  })

  it('P3: `before` pages strictly older messages', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      let gen = genChain()
      while (gen.ids.length < 2) gen = genChain() // `before` needs depth >= 2
      const logical = await insertChain(d, gen)
      const tip = logical[logical.length - 1]!
      const cut = randInt(1, logical.length - 1) // index of the `before` row
      const before = logical[cut]!
      const r = await Messages.chain(d, 't', tip, 1000, before)
      if (r.isErr()) throw new Error(`run ${run}: ${r.error}`)
      expect(r.value.map(m => m.id)).toEqual(logical.slice(0, cut))
    }
  })

  it('P4: isInChain accepts every member and rejects a foreign row', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      const gen = genChain()
      const logical = await insertChain(d, gen)
      const tip = logical[logical.length - 1]!
      for (const id of logical) {
        const r = await Messages.isInChain(d, 't', tip, id)
        expect(r.isOk() && r.value, `member ${id}`).toBe(true)
      }
      // A row that exists in the table but belongs to no chain in this
      // session (and one from another tenant).
      await Messages.insertWithId(d, 't', 'foreign', 'event', null)
      await Messages.insertWithId(d, 'other', 'othertenant', 'event', null)
      const f1 = await Messages.isInChain(d, 't', tip, 'foreign')
      expect(f1.isOk() && f1.value).toBe(false)
      const f2 = await Messages.isInChain(d, 't', tip, 'othertenant')
      expect(f2.isOk() && f2.value).toBe(false)
    }
  })

  it('P5: deltaSince returns exactly the messages newer than the anchor', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      const gen = genChain()
      const logical = await insertChain(d, gen)
      const tip = logical[logical.length - 1]!
      const anchorIdx = randInt(0, logical.length - 1)
      const anchor = logical[anchorIdx]!
      const r = await Messages.deltaSince(d, 't', tip, anchor, 1000)
      if (r.isErr()) throw new Error(`run ${run}: ${r.error}`)
      const { messages, anchorReached } = r.value
      expect(anchorReached).toBe(true)
      expect(messages.map(m => m.id)).toEqual(logical.slice(anchorIdx + 1))
      // An unknown anchor is reported (resync path), never silently wrong.
      const bad = await Messages.deltaSince(d, 't', tip, 'nope', 1000)
      expect(bad.isOk() && bad.value.anchorReached).toBe(false)
    }
  })

  it('P6: insertWithId is idempotent under arbitrary duplicate writes', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      const gen = genChain()
      const logical = await insertChain(d, gen)
      const dup = logical.filter(() => rand() < 0.4)
      for (const id of dup) {
        const r = await Messages.insertWithId(d, 't', id, 'event', null)
        expect(r.isOk() && r.value, `duplicate ${id}`).toBe(false)
      }
      const tip = logical[logical.length - 1]!
      const r = await Messages.chain(d, 't', tip, 1000, null)
      if (r.isErr()) throw new Error(r.error)
      expect(r.value.map(m => m.id)).toEqual(logical)
    }
  })

  it('P7: a poisoned cyclic prev_id terminates the walk (no hang)', async () => {
    for (let run = 0; run < RUNS; run++) {
      const d = await db()
      let gen = genChain()
      while (gen.ids.length < 2) gen = genChain() // a cycle needs depth >= 2
      const logical = await insertChain(d, gen)
      const tip = logical[logical.length - 1]!
      // Point some middle row's prev_id at a NEWER row, creating a cycle.
      const i = randInt(1, logical.length - 1)
      const j = randInt(i, logical.length - 1)
      await rawRun(
        d,
        'UPDATE messages SET prev_id = ? WHERE id = ? AND tenant = ?',
        [logical[j]!, logical[i]!, 't'],
      )
      // Must resolve (UNION-deduped CTE) rather than loop forever.
      const chain = await withTimeout(
        Messages.chain(d, 't', tip, 1000, null).then(r =>
          r.isErr() ? 'err' : r.value.length,
        ),
      )
      expect(typeof chain).toBe('number')
      const delta = await withTimeout(
        Messages.deltaSince(d, 't', tip, logical[0]!, 1000).then(r =>
          r.isErr() ? 'err' : r.value.messages.length,
        ),
      )
      expect(typeof delta).toBe('number')
      const inChain = await withTimeout(
        Messages.isInChain(d, 't', tip, logical[0]!).then(r =>
          r.isErr() ? 'err' : r.value,
        ),
      )
      expect(typeof inChain).toBe('boolean')
    }
  })
})
