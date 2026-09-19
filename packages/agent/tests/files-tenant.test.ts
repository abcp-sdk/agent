import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { connectDb, type Db } from '../src/db-client.js'
import { FilesDb } from '../src/db-files.js'
import type { FileRecord } from '../src/files.js'

/**
 * s3-mode file metadata is TENANT-SCOPED (`agent_files`, UNIQUE(tenant,
 * sha256)). The same bytes under two tenants must resolve to two independent
 * codes, so a cross-tenant upload can never resolve to another tenant's bytes
 * (which would break GetFile: the byte object is stored under t.<tenant>.<code>).
 */
describe('FilesDb is tenant-scoped', () => {
  const dbs: Db[] = []
  afterEach(() => {
    for (const db of dbs) {
      const c = db.$client as { close?: () => void }
      c.close?.()
    }
    dbs.length = 0
  })

  async function db(): Promise<Db> {
    const dir = mkdtempSync(join(tmpdir(), 'files-tenant-'))
    const res = await connectDb('sqlite', join(dir, 'agent.db'))
    if (res.isErr()) throw new Error(res.error)
    dbs.push(res.value)
    return res.value
  }

  const rec = (code: string, sha: string): FileRecord => ({
    code,
    sha256: sha,
    name: 'a.bin',
    mime: 'application/octet-stream',
    size: 3,
    uploader_session: '',
    created_at: '',
  })

  it('dedups per (tenant, sha) and isolates lookups', async () => {
    const d = await db()
    const sha = 'deadbeef'

    const a = await FilesDb.upsert(d, 'tenant-a', rec('code-a', sha))
    expect(a.isOk()).toBe(true)
    expect(a._unsafeUnwrap().code).toBe('code-a')

    // Same bytes under tenant-b: a DISTINCT code (not tenant-a's).
    const b = await FilesDb.upsert(d, 'tenant-b', rec('code-b', sha))
    expect(b.isOk()).toBe(true)
    expect(b._unsafeUnwrap().code).toBe('code-b')

    // Re-upload under tenant-a returns the stored canonical code.
    const a2 = await FilesDb.upsert(d, 'tenant-a', rec('code-a2', sha))
    expect(a2._unsafeUnwrap().code).toBe('code-a')

    // bySha is tenant-scoped.
    expect((await FilesDb.bySha(d, 'tenant-a', sha))._unsafeUnwrap()?.code).toBe(
      'code-a',
    )
    expect((await FilesDb.bySha(d, 'tenant-b', sha))._unsafeUnwrap()?.code).toBe(
      'code-b',
    )
    // A tenant that never uploaded these bytes sees nothing.
    expect(
      (await FilesDb.bySha(d, 'tenant-c', sha))._unsafeUnwrap(),
    ).toBeNull()

    // byCode is tenant-scoped: tenant-b cannot resolve tenant-a's code.
    expect(
      (await FilesDb.byCode(d, 'tenant-a', 'code-a'))._unsafeUnwrap()?.code,
    ).toBe('code-a')
    expect(
      (await FilesDb.byCode(d, 'tenant-b', 'code-a'))._unsafeUnwrap(),
    ).toBeNull()
  })
})
