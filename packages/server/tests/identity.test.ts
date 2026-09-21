import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { connectDb, type Db, Tenants } from '@abcp-agent/agent'
import { createContextValues, type HandlerContext } from '@connectrpc/connect'
import { afterEach, describe, expect, it } from 'vitest'
import { kIdentity } from '../src/auth.js'
import { identityHandlers } from '../src/handlers/identity.js'

function ctx(role: 'tenant' | 'admin', tenant: string): HandlerContext {
  const values = createContextValues()
  values.set(kIdentity, { role, tenant })
  return { values } as unknown as HandlerContext
}

describe('GetIdentity', () => {
  const dbs: Db[] = []
  afterEach(() => {
    for (const db of dbs) {
      const c = db.$client as { close?: () => void }
      c.close?.()
    }
    dbs.length = 0
  })

  async function handlers() {
    const dir = mkdtempSync(join(tmpdir(), 'identity-'))
    const r = await connectDb('sqlite', `sqlite://${join(dir, 'a.db')}`)
    if (r.isErr()) throw new Error(r.error)
    dbs.push(r.value)
    await Tenants.create(r.value, 'acme', 'Acme Corp')
    return identityHandlers({ db: r.value } as never)
  }

  it('resolves a tenant token to its id, human name and role', async () => {
    const h = await handlers()
    const out = await h.getIdentity!({} as never, ctx('tenant', 'acme'))
    expect(out).toEqual({
      tenant: 'acme',
      tenantName: 'Acme Corp',
      role: 'tenant',
    })
  })

  it('falls back to the tenant id when no name is stored', async () => {
    const h = await handlers()
    const out = await h.getIdentity!({} as never, ctx('tenant', 'ghost'))
    expect(out).toEqual({
      tenant: 'ghost',
      tenantName: 'ghost',
      role: 'tenant',
    })
  })

  it('resolves an admin token to role=admin with empty tenant fields', async () => {
    const h = await handlers()
    const out = await h.getIdentity!({} as never, ctx('admin', ''))
    expect(out).toEqual({ tenant: '', tenantName: '', role: 'admin' })
  })
})
