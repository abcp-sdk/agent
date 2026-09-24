import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { AgentDeps, Bus, LlmRegistry } from '@abcp-agent/agent'
import { connectDb, Messages, Presets, Sessions } from '@abcp-agent/agent'
import { createContextValues, type HandlerContext } from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'
import { kIdentity } from '../src/auth.js'
import { sessionsHandlers } from '../src/handlers/sessions.js'

/**
 * The Fork RPC must anchor the child at the message BEFORE the parent's
 * current-turn prompt when no explicit fork point is given. While a tool runs
 * the parent's tip IS that prompt (the assistant step is not persisted yet), so
 * copying it would hand the child its parent's own instruction — the same
 * defect the subsession fork fixed. A preset-changing fork must also not inherit
 * the parent's session-level system-prompt override.
 */
const T = 'default'

function fakeBus() {
  const store: Record<string, string> = {}
  return {
    kvGet: async (b: string, k: string) => store[`${b}\n${k}`] ?? null,
    kvPut: async (b: string, k: string, v: string) => {
      store[`${b}\n${k}`] = v
    },
    kvCreate: async (b: string, k: string, v: string) => {
      const key = `${b}\n${k}`
      if (store[key] !== undefined) return null
      store[key] = v
      return 1
    },
    kvDelete: async (b: string, k: string) => {
      delete store[`${b}\n${k}`]
    },
    publish: async () => {},
    inboxPublish: async () => {},
  } as unknown as Bus
}

const dbs: Array<{ $client?: { close?: () => void } }> = []
afterEach(() => {
  for (const db of dbs) db.$client?.close?.()
  dbs.length = 0
})

async function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'fork-anchor-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value as never)
  const bus = fakeBus()
  await Presets.seedDefaults(bus, T)
  const deps = {
    db: r.value,
    bus,
    llm: { resolve: async () => ({ isErr: () => true, error: 'x' }) },
  } as unknown as AgentDeps
  const handlers = sessionsHandlers(deps)
  const values = createContextValues()
  values.set(kIdentity, { role: 'tenant', tenant: T })
  const ctx = { values } as unknown as HandlerContext
  return { handlers, ctx, db: r.value }
}

/** Seed `parent` with a `user -> assistant -> user` chain and return the ids. */
async function seedChain(db: never): Promise<{ u1: string; a1: string; u2: string }> {
  await Messages.insertWithId(db, T, 'u1', 'user', null)
  await Messages.insertWithId(db, T, 'a1', 'assistant', 'u1')
  await Messages.insertWithId(db, T, 'u2', 'user', 'a1')
  return { u1: 'u1', a1: 'a1', u2: 'u2' }
}

async function makeParent(
  db: never,
  tipId: string | null,
  opts: { preset?: string; systemPrompt?: string } = {},
): Promise<void> {
  const r = await Sessions.create(db, T, {
    name: 'parent',
    preset: opts.preset ?? 'maintainer',
    systemPrompt: opts.systemPrompt ?? '',
    tipId,
  })
  if (r.isErr()) throw new Error(r.error)
}

async function childTip(db: never, name: string): Promise<string | null> {
  const r = await Sessions.get(db, T, name)
  if (r.isErr()) throw new Error(r.error)
  return r.value?.tip_id ?? null
}

async function childPrompt(db: never, name: string): Promise<string> {
  const r = await Sessions.get(db, T, name)
  if (r.isErr()) throw new Error(r.error)
  return r.value?.system_prompt ?? '<missing>'
}

describe('Fork RPC anchor', () => {
  it('steps BACK over a user tip (the current-turn prompt)', async () => {
    const { handlers, ctx, db } = await fixture()
    await seedChain(db as never)
    await makeParent(db as never, 'u2')
    await handlers.fork!({ id: 'parent', name: 'child' } as never, ctx)
    expect(await childTip(db as never, 'child')).toBe('a1')
  })

  it('keeps an assistant tip as-is', async () => {
    const { handlers, ctx, db } = await fixture()
    await seedChain(db as never)
    await makeParent(db as never, 'a1')
    await handlers.fork!({ id: 'parent', name: 'child' } as never, ctx)
    expect(await childTip(db as never, 'child')).toBe('a1')
  })

  it('returns null when the user tip is the chain start', async () => {
    const { handlers, ctx, db } = await fixture()
    await Messages.insertWithId(db as never, T, 'u1', 'user', null)
    await makeParent(db as never, 'u1')
    await handlers.fork!({ id: 'parent', name: 'child' } as never, ctx)
    expect(await childTip(db as never, 'child')).toBe(null)
  })

  it('an explicit messageId always wins (fork-at-a-message)', async () => {
    const { handlers, ctx, db } = await fixture()
    await seedChain(db as never)
    await makeParent(db as never, 'u2')
    await handlers.fork!(
      { id: 'parent', name: 'child', messageId: 'u2' } as never,
      ctx,
    )
    expect(await childTip(db as never, 'child')).toBe('u2')
  })

  it('drops the parent system prompt when the fork CHANGES preset', async () => {
    const { handlers, ctx, db } = await fixture()
    await makeParent(db as never, null, {
      preset: 'maintainer',
      systemPrompt: 'MAINTAINER ONLY',
    })
    await handlers.fork!(
      { id: 'parent', name: 'child', preset: 'developer' } as never,
      ctx,
    )
    expect(await childPrompt(db as never, 'child')).toBe('')
  })

  it('carries the parent system prompt when the preset is unchanged', async () => {
    const { handlers, ctx, db } = await fixture()
    await makeParent(db as never, null, {
      preset: 'maintainer',
      systemPrompt: 'SHARED',
    })
    await handlers.fork!({ id: 'parent', name: 'child' } as never, ctx)
    expect(await childPrompt(db as never, 'child')).toBe('SHARED')
  })
})
