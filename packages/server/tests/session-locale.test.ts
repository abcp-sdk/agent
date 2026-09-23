import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { AgentDeps, Bus, LlmRegistry } from '@abcp-agent/agent'
import { connectDb, Presets, Sessions } from '@abcp-agent/agent'
import { createContextValues, type HandlerContext } from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'
import { kIdentity } from '../src/auth.js'
import { sessionsHandlers } from '../src/handlers/sessions.js'

/**
 * createSession must PIN the request locale, but an EMPTY request means "follow
 * the tenant default" and must stay UNSET. Regression: normalizeLocale('')
 * returns 'en', so routing an empty locale through it pinned every session to
 * English, which then won over a zh tenant config in resolveLocale.
 */
const T = 'default'

function fakeBus() {
  const store: Record<string, string> = {}
  const bus = {
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
    publishLifecycle: async () => {},
  } as unknown as Bus
  return bus
}

const dbs: Array<{ $client?: { close?: () => void } }> = []
afterEach(() => {
  for (const db of dbs) db.$client?.close?.()
  dbs.length = 0
})

async function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'sess-locale-'))
  const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
  if (r.isErr()) throw new Error(r.error)
  dbs.push(r.value as never)
  const bus = fakeBus()
  await Presets.seedDefaults(bus, T)
  const deps = {
    db: r.value,
    bus,
    llm: {
      resolve: async () => ({ isErr: () => true, error: 'x' }),
    } as unknown as LlmRegistry,
  } as unknown as AgentDeps
  const handlers = sessionsHandlers(deps)
  const values = createContextValues()
  values.set(kIdentity, { role: 'tenant', tenant: T })
  const ctx = { values } as unknown as HandlerContext
  return { handlers, ctx, db: r.value }
}

async function createdLocale(db: never, name: string): Promise<string> {
  const r = await Sessions.get(db, T, name)
  if (r.isErr()) throw new Error(r.error)
  return r.value?.locale ?? '<missing>'
}

describe('createSession locale pinning', () => {
  it('an EMPTY request locale stays UNSET (not pinned to "en")', async () => {
    const { handlers, ctx, db } = await fixture()
    await handlers.createSession!({ name: 's-empty', locale: '' } as never, ctx)
    expect(await createdLocale(db as never, 's-empty')).toBe('')
  })

  it('an omitted request locale stays UNSET', async () => {
    const { handlers, ctx, db } = await fixture()
    await handlers.createSession!({ name: 's-omit' } as never, ctx)
    expect(await createdLocale(db as never, 's-omit')).toBe('')
  })

  it('a non-empty request locale is normalized and pinned', async () => {
    const { handlers, ctx, db } = await fixture()
    await handlers.createSession!(
      { name: 's-zh', locale: 'ZH_CN' } as never,
      ctx,
    )
    expect(await createdLocale(db as never, 's-zh')).toBe('zh-cn')

    await handlers.createSession!({ name: 's-en', locale: 'en' } as never, ctx)
    expect(await createdLocale(db as never, 's-en')).toBe('en')
  })
})
