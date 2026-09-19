import { extname } from 'node:path'
import type { Bus } from './bus.js'
import {
  type BlobStore,
  type FileRecord,
  fileBySha,
  randomCode,
  sha256Hex,
  upsertFile,
} from './files.js'
import { scheduleMediaProbe } from './media.js'
import { extForMime, sniffMime } from './mime.js'

/**
 * THE single write path for stored file bytes.
 *
 * Every ingest (HTTP UploadFile/IngestFile, the bus file.ingest RPC, streamed
 * `file` parts, and the bundled extension's generated media) funnels through
 * here so a file is stored, deduped, content-typed and media-probed IDENTICALLY
 * regardless of origin. Callers never supply a mime: the type is DERIVED from
 * the bytes (see mime.ts), which closes the class of bugs where a caller's
 * guess (empty from a browser, hard-coded by a tool, absent in a sandbox
 * upload) silently disabled thumbnails or rendered the wrong viewer.
 */
export interface StoreFileDeps {
  bus: Bus
  files: BlobStore
}

export interface StoreFileInput {
  tenant: string
  data: Uint8Array
  /** Display name. An extension is appended when absent and derivable. */
  name: string
  uploaderSession?: string
  /** Optional caller-minted code (the bus ingest RPC allows one). */
  code?: string
}

/** Persist bytes: sniff the mime, dedup by (tenant, sha256), store, probe.
 *  Returns the canonical stored record (its `mime` is authoritative). */
export async function storeFile(
  deps: StoreFileDeps,
  input: StoreFileInput,
): Promise<FileRecord> {
  const { bus, files } = deps
  const { tenant, data } = input
  const { mime, ext } = await sniffMime(data, input.name)

  // Complete a missing extension from the derived type so the name the UI
  // shows (and the download filename) is coherent.
  let name = input.name.trim()
  if (name === '') name = ext === '' ? 'file' : `file.${ext}`
  else if (extname(name) === '' && mime !== 'application/octet-stream') {
    const e = ext !== '' ? ext : extForMime(mime)
    if (e !== '') name = `${name}.${e}`
  }

  const sha = sha256Hex(data)
  const existing = await fileBySha(bus, tenant, sha)
  if (existing.isOk() && existing.value !== null) return existing.value

  const code = input.code?.trim() || randomCode()
  const record: FileRecord = {
    code,
    sha256: sha,
    name,
    mime,
    size: data.length,
    uploader_session: input.uploaderSession ?? '',
    created_at: new Date().toISOString(),
  }
  await files.put(tenant, code, record, data)
  await upsertFile(bus, tenant, record)
  // Media facts are derived asynchronously (never block the store). The probe
  // also refines the mime when the magic-byte sniff was inconclusive.
  scheduleMediaProbe({ bus, files }, tenant, record)
  return record
}
