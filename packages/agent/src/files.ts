import { createHash, randomUUID } from 'node:crypto'
import { err, ok, type Result, ResultAsync } from 'neverthrow'
import type { Bus } from './bus.js'
import { BUCKET_FILES_META, tenantKVKey, tenantObjectName } from './bus.js'
import type { Db } from './db-client.js'
import { FilesDb } from './db-files.js'

/** Metadata for a stored file; the NATS KV bucket is the single source of
 *  truth, colocated with the bytes in the persistent object store. */
export interface FileRecord {
  code: string
  sha256: string
  name: string
  mime: string
  size: number
  uploader_session: string
  created_at: string
}

const NO_TTL = 0
const META_PREFIX = 'f.'
const SHA_PREFIX = 'sha.'

/**
 * Write-through cache closing the KV read-after-write propagation window:
 * an upload followed by a cross-service meta lookup (e.g. the platform's
 * attachment refs) must never miss its own just-written entry. Keys are
 * tenant-scoped (`<tenant>\x00<code>` / `<tenant>\x00<sha>`) so identical
 * file codes/bytes under different tenants never collide.
 */
const CACHE_CAP = 4096
const metaCache = new Map<string, FileRecord>()
const shaCache = new Map<string, string>()

function metaCacheKey(tenant: string, code: string): string {
  return `${tenant}\x00${code}`
}
function shaCacheKey(tenant: string, sha256: string): string {
  return `${tenant}\x00${sha256}`
}

function cachePut(tenant: string, record: FileRecord): void {
  metaCache.set(metaCacheKey(tenant, record.code), record)
  if (record.sha256 !== '') {
    shaCache.set(shaCacheKey(tenant, record.sha256), record.code)
  }
  while (metaCache.size > CACHE_CAP) {
    const oldest = metaCache.keys().next().value
    if (oldest === undefined) break
    metaCache.delete(oldest)
  }
  while (shaCache.size > CACHE_CAP) {
    const oldest = shaCache.keys().next().value
    if (oldest === undefined) break
    shaCache.delete(oldest)
  }
}

/** Tenant-scoped KV key for a file's meta record. */
function fileMetaKey(tenant: string, code: string): string {
  return tenantKVKey(tenant, META_PREFIX + code)
}

/** Tenant-scoped KV key for a file's sha256 dedup index. */
function fileShaKey(tenant: string, sha256: string): string {
  return tenantKVKey(tenant, SHA_PREFIX + sha256)
}

function parseMeta(raw: string): FileRecord | null {
  try {
    const v = JSON.parse(raw) as Record<string, unknown>
    const code = String(v.code ?? '')
    if (code === '') return null
    return {
      code,
      sha256: String(v.sha256 ?? ''),
      name: String(v.name ?? ''),
      mime: String(v.mime ?? ''),
      size: Number(v.size ?? 0),
      uploader_session: String(v.uploader_session ?? ''),
      created_at: String(v.created_at ?? ''),
    }
  } catch {
    return null
  }
}

function ra<T>(op: Promise<T>, context: string): ResultAsync<T, string> {
  return ResultAsync.fromPromise(op, e => `${context}: ${String(e)}`)
}

/**
 * Persist a file mapping: meta key first, then the dedup index. A crash
 * between the two only loses dedup (harmless re-upload), never a dangling
 * index pointing at missing meta.
 */
export async function upsertFile(
  bus: Bus,
  tenant: string,
  record: FileRecord,
): Promise<Result<FileRecord, string>> {
  if (metaBackend === 'db') {
    const db = metaDb
    if (db === null) return err('file meta db not configured')
    const r = await FilesDb.upsert(db, tenant, record)
    return r.isErr() ? err(r.error) : ok(r.value)
  }
  return ra(
    (async () => {
      await bus.kvPut(
        BUCKET_FILES_META,
        fileMetaKey(tenant, record.code),
        JSON.stringify(record),
        NO_TTL,
      )
      if (record.sha256 !== '') {
        // Atomic when absent; a lost race means identical bytes, harmless.
        const created = await bus.kvCreate(
          BUCKET_FILES_META,
          fileShaKey(tenant, record.sha256),
          record.code,
          NO_TTL,
        )
        if (created === null) {
          await bus.kvPut(
            BUCKET_FILES_META,
            fileShaKey(tenant, record.sha256),
            record.code,
            NO_TTL,
          )
        }
        shaCache.set(shaCacheKey(tenant, record.sha256), record.code)
      }
      cachePut(tenant, record)
      return record
    })(),
    'upsert file failed',
  )
}

/** Look up the file record (if any) that already stores this sha256 (dedup,
 *  per tenant). */
export async function fileBySha(
  bus: Bus,
  tenant: string,
  sha256: string,
): Promise<Result<FileRecord | null, string>> {
  if (metaBackend === 'db') {
    const db = metaDb
    if (db === null || sha256 === '') return ok(null)
    const r = await FilesDb.bySha(db, tenant, sha256)
    return r.isErr() ? err(r.error) : ok(r.value)
  }
  return ra(
    (async () => {
      if (sha256 === '') return null
      const cached = shaCache.get(shaCacheKey(tenant, sha256))
      if (cached !== undefined) {
        const hit = metaCache.get(metaCacheKey(tenant, cached))
        if (hit !== undefined) return hit
      }
      const code = await bus.kvGet(
        BUCKET_FILES_META,
        fileShaKey(tenant, sha256),
      )
      if (code === null) return null
      const raw = await bus.kvGet(BUCKET_FILES_META, fileMetaKey(tenant, code))
      if (raw === null) return null
      const record = parseMeta(raw)
      if (record !== null) cachePut(tenant, record)
      return record
    })(),
    'fileBySha failed',
  )
}

/** Fetch a file record by code within a tenant. */
export async function fileByCode(
  bus: Bus,
  tenant: string,
  code: string,
): Promise<Result<FileRecord | null, string>> {
  if (metaBackend === 'db') {
    const db = metaDb
    if (db === null) return ok(null)
    const r = await FilesDb.byCode(db, tenant, code)
    return r.isErr() ? err(r.error) : ok(r.value)
  }
  return ra(
    (async () => {
      const cached = metaCache.get(metaCacheKey(tenant, code))
      if (cached !== undefined) return cached
      const raw = await bus.kvGet(BUCKET_FILES_META, fileMetaKey(tenant, code))
      if (raw === null) return null
      const record = parseMeta(raw)
      if (record !== null) cachePut(tenant, record)
      return record
    })(),
    'fileByCode failed',
  )
}

export function sha256Hex(data: Uint8Array): string {
  return createHash('sha256').update(data).digest('hex')
}

/** Mint a short, unguessable, collision-resistant object key. */
export function randomCode(): string {
  return randomUUID().replace(/-/g, '').slice(0, 16)
}

/**
 * BlobStore stores file bytes addressed by `code` within a tenant. Metadata
 * lives in the NATS KV bucket; durable file bytes use the persistent
 * (no-TTL) object bucket, so they never expire. Object names and meta keys
 * are tenant-scoped (`t.<tenant>.<code>`).
 */
export interface BlobStore {
  put(
    tenant: string,
    code: string,
    meta: FileRecord,
    data: Uint8Array,
  ): Promise<void>
  get(
    tenant: string,
    code: string,
  ): Promise<{ meta: FileRecord; data: Uint8Array }>
  stat(tenant: string, code: string): Promise<FileRecord | null>
}

/**
 * File-metadata backend. `nats` = KV buckets (default); `db` = the
 * `agent_files` table (used with the S3 object backend so file state leaves
 * NATS). Selected once at boot via [configureFileMetaStore].
 */
export type FileMetaBackend = 'nats' | 'db'

let metaBackend: FileMetaBackend = 'nats'
let metaDb: Db | null = null

/**
 * Select the metadata backend. `db` requires the database handle (the
 * `agent_files` table). Must be called before any file operation.
 */
export function configureFileMetaStore(
  backend: FileMetaBackend,
  db?: Db,
): void {
  metaBackend = backend
  metaDb = backend === 'db' ? (db ?? null) : null
}

/** NATS JetStream backend: bytes → persistent object bucket, meta → KV. */
function makeNatsStore(bus: Bus): BlobStore {
  return {
    async put(tenant, code, _meta, data) {
      await bus.objectPutPersistent(
        tenantObjectName(tenant, code),
        Uint8Array.from(data),
      )
    },
    async get(tenant, code) {
      const data = await bus.objectGetPersistent(tenantObjectName(tenant, code))
      if (data === null) throw new Error(`file not found: ${code}`)
      const meta = await fileByCode(bus, tenant, code)
      return {
        meta:
          meta.isOk() && meta.value !== null
            ? meta.value
            : ({ code } as FileRecord),
        data: Uint8Array.from(data),
      }
    },
    async stat(tenant, code) {
      const data = await bus.objectGetPersistent(tenantObjectName(tenant, code))
      if (data === null) return null
      const meta = await fileByCode(bus, tenant, code)
      return meta.isOk() && meta.value !== null
        ? meta.value
        : ({ code } as FileRecord)
    },
  }
}

/** Build the file blob backend (durable bytes via the bus's object store;
 *  metadata via the configured meta backend — see [configureFileMetaStore]). */
export function makeBlobStore(bus: Bus): BlobStore {
  return makeNatsStore(bus)
}
