import { createHash, randomUUID } from 'node:crypto'
import { err, ok, type Result, ResultAsync } from 'neverthrow'
import type { Bus } from './bus.js'
import { BUCKET_FILES_META, tenantKVKey, tenantObjectName } from './bus.js'
import type { Db } from './db-client.js'
import { FilesDb } from './db-files.js'

/** Metadata for a stored file; the NATS KV bucket is the single source of
 *  truth, colocated with the bytes in the persistent object store.
 *
 *  The optional media fields are DERIVED, server-side, by the media probe
 *  (ffprobe/ffmpeg) when the mime is a supported image/video/audio type. They
 *  are filled in asynchronously after the bytes land (so an upload is never
 *  blocked on decoding) and are absent for non-media files or files probed
 *  before the feature existed. `thumb_code` is itself a canonical file code:
 *  the thumbnail is a content-addressed file stored like any other, so a
 *  client fetches it through the normal GetFile/GetFileStream path. */
export interface FileRecord {
  code: string
  sha256: string
  name: string
  mime: string
  size: number
  uploader_session: string
  created_at: string
  width?: number | null
  height?: number | null
  duration_ms?: number | null
  thumb_code?: string | null
  thumbhash?: string | null
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
      width: optNum(v.width),
      height: optNum(v.height),
      duration_ms: optNum(v.duration_ms),
      thumb_code: optStr(v.thumb_code),
      thumbhash: optStr(v.thumbhash),
    }
  } catch {
    return null
  }
}

function optNum(v: unknown): number | null {
  return v === undefined || v === null || v === '' ? null : Number(v)
}

function optStr(v: unknown): string | null {
  return v === undefined || v === null ? null : String(v)
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

/** Patch a file record's derived media fields in place. Only the provided
 *  keys are written; `undefined` keys are left untouched so a probe that only
 *  learned dimensions never clobbers an earlier thumbnail. Best-effort: a
 *  failure here must never surface to the caller that stored the bytes. */
export async function updateFileMedia(
  bus: Bus,
  tenant: string,
  code: string,
  patch: Pick<
    FileRecord,
    'width' | 'height' | 'duration_ms' | 'thumb_code' | 'thumbhash'
  >,
): Promise<void> {
  if (metaBackend === 'db') {
    const db = metaDb
    if (db !== null) await FilesDb.updateMedia(db, tenant, code, patch)
    return
  }
  const existing = await fileByCode(bus, tenant, code)
  if (existing.isErr() || existing.value === null) return
  const merged: FileRecord = { ...existing.value }
  for (const [k, v] of Object.entries(patch)) {
    if (v !== undefined) (merged as unknown as Record<string, unknown>)[k] = v
  }
  cachePut(tenant, merged)
  await bus.kvPut(
    BUCKET_FILES_META,
    fileMetaKey(tenant, code),
    JSON.stringify(merged),
    NO_TTL,
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
  /**
   * Yield a file's bytes in order as chunks. The streaming counterpart of
   * [get]: callers can start rendering before the whole object has been read.
   * Implementations may read the object whole and slice it (the NATS object
   * store exposes no forward reader, so this is the portable behaviour) — the
   * wire contract is identical either way.
   */
  getStream(
    tenant: string,
    code: string,
    chunkSize?: number,
  ): AsyncGenerator<Uint8Array>
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

/** Default chunk size for [BlobStore.getStream] (bytes). */
export const FILE_STREAM_CHUNK = 256 * 1024

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
    async *getStream(tenant, code, chunkSize = FILE_STREAM_CHUNK) {
      // The NATS object store has no forward reader exposed by the SDK, so
      // read the object whole and slice it. The S3 backend streams natively;
      // this path is the documented fallback.
      const data = await bus.objectGetPersistent(tenantObjectName(tenant, code))
      if (data === null) return
      const bytes = Uint8Array.from(data)
      for (let off = 0; off < bytes.length; off += chunkSize) {
        yield bytes.subarray(off, Math.min(off + chunkSize, bytes.length))
      }
    },
  }
}

/** Optional durable-stream reader. The bus's object store (NATS) has no
 *  forward reader, so when an S3 backend is configured its native streaming
 *  reader is passed here and used by [BlobStore.getStream]; otherwise the
 *  whole-object fallback in [makeNatsStore] applies. */
export interface DurableStreamReader {
  objectGetStream(name: string): Promise<AsyncIterable<Uint8Array> | null>
}

/** Build the file blob backend (durable bytes via the bus's object store;
 *  metadata via the configured meta backend — see [configureFileMetaStore]).
 *  `streamer`, when provided, enables true streaming reads (S3); otherwise
 *  [BlobStore.getStream] reads whole and slices. */
export function makeBlobStore(
  bus: Bus,
  streamer?: DurableStreamReader,
): BlobStore {
  const base = makeNatsStore(bus)
  if (streamer === undefined) return base
  const fallback = base.getStream.bind(base)
  return {
    ...base,
    async *getStream(tenant, code, chunkSize = FILE_STREAM_CHUNK) {
      const source = await streamer.objectGetStream(
        tenantObjectName(tenant, code),
      )
      if (source === null) {
        // Missing from the stream reader: fall back (also covers a transient
        // S3 error surfacing as null).
        yield* fallback(tenant, code, chunkSize)
        return
      }
      let acc: Uint8Array[] = []
      let accLen = 0
      for await (const part of source) {
        let chunk = part
        // Re-chunk to the requested size so the wire chunking is stable even
        // when S3 hands back larger/smaller pieces.
        while (chunk.length > 0) {
          const want = chunkSize - accLen
          if (chunk.length >= want) {
            acc.push(chunk.subarray(0, want))
            accLen += want
            chunk = chunk.subarray(want)
            yield joinChunks(acc, accLen)
            acc = []
            accLen = 0
          } else {
            acc.push(chunk)
            accLen += chunk.length
            chunk = chunk.subarray(chunk.length)
          }
        }
      }
      if (accLen > 0) yield joinChunks(acc, accLen)
    },
  }
}

function joinChunks(parts: Uint8Array[], total: number): Uint8Array {
  if (parts.length === 1) return parts[0]!
  const out = new Uint8Array(total)
  let off = 0
  for (const p of parts) {
    out.set(p, off)
    off += p.length
  }
  return out
}
