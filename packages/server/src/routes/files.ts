import { createRoute, OpenAPIHono } from '@hono/zod-openapi'
import {
  type FileRecord,
  fileByCode,
  fileBySha,
  randomCode,
  sha256Hex,
  upsertFile,
} from '@easylab-agent/agent'
import { z } from 'zod'
import type { AppEnv } from '../context.js'

const ErrorSchema = z.object({ ok: z.boolean(), error: z.string() })

const fileJsonSchema = z.object({
  code: z.string(),
  sha256: z.string(),
  name: z.string(),
  mime: z.string(),
  size: z.number(),
  uploader_session: z.string(),
  created_at: z.string(),
})

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
  deps: AppEnv['Variables']['deps'],
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

const uploadFileRoute = createRoute({
  method: 'post',
  path: '/files',
  summary: 'Upload a file (multipart)',
  request: {},
  responses: {
    200: {
      description: 'File',
      content: { 'application/json': { schema: fileJsonSchema } },
    },
    400: {
      description: 'Bad request',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const ingestFileRoute = createRoute({
  method: 'post',
  path: '/files/ingest',
  summary: 'Ingest raw bytes (tool-generated, e.g. screenshots)',
  description:
    'Stores raw body bytes under a new code, deduplicating by sha256. Used by tools that produce binary artifacts (browser screenshots, PDFs) so they can hand the model a `file:<code>` reference instead of embedding bytes.',
  responses: {
    200: {
      description: 'File',
      content: { 'application/json': { schema: fileJsonSchema } },
    },
    400: {
      description: 'Bad request',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const getFileRoute = createRoute({
  method: 'get',
  path: '/files/{code}',
  summary: 'Download a file',
  responses: {
    200: { description: 'File bytes' },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const getFileMetaRoute = createRoute({
  method: 'get',
  path: '/files/{code}/meta',
  summary: 'Get file metadata',
  responses: {
    200: {
      description: 'Meta',
      content: { 'application/json': { schema: fileJsonSchema } },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

export const fileRoutes = new OpenAPIHono<AppEnv>()
  .openapi(uploadFileRoute, async c => {
    const deps = c.get('deps')
    const body = await c.req.parseBody()
    const file = body.file
    const uploader = (body.uploader_session as string) || ''
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
  .openapi(ingestFileRoute, async c => {
    const deps = c.get('deps')
    const name = c.req.query('name') || 'artifact'
    const mime = c.req.query('content_type') || 'application/octet-stream'
    const uploader = c.req.query('uploader_session') || ''
    const data = new Uint8Array(await c.req.raw.arrayBuffer())
    if (data.length === 0)
      return c.json({ ok: false, error: 'empty body' }, 400)
    const record = await storeBytes(deps, data, name, mime, uploader)
    return c.json(fileToJson(record), 200)
  })
  .openapi(getFileRoute, async c => {
    const deps = c.get('deps')
    const code = c.req.param('code')
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
    c.header('Content-Type', ct)
    c.header('Content-Length', String(data.length))
    c.header('Content-Disposition', `inline; filename="${meta.name || 'file'}"`)
    return c.body(new Uint8Array(data), 200)
  })
  .openapi(getFileMetaRoute, async c => {
    const deps = c.get('deps')
    const code = c.req.param('code')
    const row = await fileByCode(deps.bus, code)
    if (row.isErr()) return c.json({ ok: false, error: row.error }, 500)
    if (row.value === null)
      return c.json({ ok: false, error: 'file not found' }, 404)
    return c.json(fileToJson(row.value), 200)
  })
