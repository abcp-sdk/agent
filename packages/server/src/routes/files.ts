import {
  type FileRecord,
  fileByCode,
  fileBySha,
  randomCode,
  sha256Hex,
  upsertFile,
} from '@easylab-agent/agent'
import type { AgentDeps } from '@easylab-agent/agent'
import { type Router } from '../http.js'

function fileToJson(f: FileRecord) {
  return {
    code: f.code,
    sha256: f.sha256,
    name: f.name,
    mime: f.mime,
    size: f.size,
    uploader_session: f.uploader_session,
    created_at: f.created_at,
  }
}

/** Dedup + store a single file. Shared by upload (multipart) and ingest (tool bytes). */
async function storeBytes(
  deps: AgentDeps,
  data: Uint8Array,
  name: string,
  mime: string,
  uploader: string,
): Promise<FileRecord> {
  const sha = sha256Hex(data)
  // Content dedup: reuse the existing code for identical bytes.
  const existing = await fileBySha(deps.bus, sha)
  if (existing.isOk() && existing.value !== null) {
    return existing.value
  }
  const code = randomCode()
  const record: FileRecord = {
    code,
    sha256: sha,
    name,
    mime,
    size: data.length,
    uploader_session: uploader,
    created_at: new Date().toISOString(),
  }
  await deps.files.put(code, record, data)
  await upsertFile(deps.bus, record)
  return record
}

export function fileRoutes(r: Router): void {
  r.post('/files', async c => {
    const deps = c.deps
    const body = await c.req.raw.formData()
    const file = body.get('file')
    const uploader = (body.get('uploader_session') as string) || ''
    if (!(file instanceof File)) {
      return c.json(
        { ok: false, error: 'file field required (multipart)' },
        400,
      )
    }
    const data = new Uint8Array(await file.arrayBuffer())
    if (data.length === 0)
      return c.json({ ok: false, error: 'empty file' }, 400)
    const record = await storeBytes(
      deps,
      data,
      file.name,
      file.type || 'application/octet-stream',
      uploader,
    )
    return c.json(fileToJson(record), 200)
  })

  r.post('/files/ingest', async c => {
    const deps = c.deps
    const name = c.req.query.get('name') || 'artifact'
    const mime = c.req.query.get('content_type') || 'application/octet-stream'
    const uploader = c.req.query.get('uploader_session') || ''
    const data = new Uint8Array(await c.req.raw.arrayBuffer())
    if (data.length === 0)
      return c.json({ ok: false, error: 'empty body' }, 400)
    const record = await storeBytes(deps, data, name, mime, uploader)
    return c.json(fileToJson(record), 200)
  })

  r.get('/files/:code', async c => {
    const deps = c.deps
    const code = c.req.params['code'] ?? ''
    const row = await fileByCode(deps.bus, code)
    if (row.isErr()) return c.json({ ok: false, error: row.error }, 500)
    if (row.value === null)
      return c.json({ ok: false, error: 'file not found' }, 404)
    let meta = row.value
    let data: Uint8Array
    try {
      const got = await deps.files.get(code)
      data = got.data
      meta = got.meta.code ? got.meta : meta
    } catch {
      return c.json({ ok: false, error: 'file not found' }, 404)
    }
    const ct = meta.mime || 'application/octet-stream'
    return new Response(new Uint8Array(data), {
      status: 200,
      headers: {
        'content-type': ct,
        'content-length': String(data.length),
        'content-disposition': `inline; filename="${meta.name || 'file'}"`,
      },
    })
  })

  r.get('/files/:code/meta', async c => {
    const deps = c.deps
    const code = c.req.params['code'] ?? ''
    const row = await fileByCode(deps.bus, code)
    if (row.isErr()) return c.json({ ok: false, error: row.error }, 500)
    if (row.value === null)
      return c.json({ ok: false, error: 'file not found' }, 404)
    return c.json(fileToJson(row.value), 200)
  })
}
