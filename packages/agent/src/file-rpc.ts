import { randomUUID } from 'node:crypto'
import { FILE_GET_WILDCARD, FILE_INGEST_WILDCARD } from '@abc-protocol/sdk'
import { z } from 'zod'
import type { Bus } from './bus.js'
import { tenantObjectName } from './bus.js'
import type { BlobStore } from './files.js'
import { logger } from './logger.js'
import { storeFile } from './store-file.js'

/**
 * Agent-served file RPCs (`abc.<tenant>.file.ingest` / `.file.get`).
 *
 * A DB-less, store-less extension (no S3 credentials, no agent DB) cannot mint
 * a canonical `file:<code>` on its own, so it asks the agent to persist bytes +
 * metadata via the SAME path as an in-process ingest. The tenant rides the
 * request subject (and envelope), so no separate credential is introduced —
 * the extension is a trusted bus peer that already serves every tenant.
 *
 * BYTES NEVER RIDE THE MESSAGE. The extension first puts the bytes in the
 * transient object store (`tenantObjectName(tenant, object)`; the transport
 * chunks them, so any size works and the broker max_payload is irrelevant) and
 * sends only the object reference. Symmetrically, `file.get` writes the bytes
 * to the transient object store and returns the reference.
 *
 * NO MIME ON THE WIRE: the agent DERIVES the content type from the bytes, so
 * the caller cannot mislabel a file (nor needs a content-type library). The
 * derived mime is returned in the reply.
 *
 * This mirrors the HTTP `IngestFile` / `GetFile` handlers but over the bus, so
 * remote extension servers use one code path regardless of blob backend
 * (`nats` object store or `s3`).
 */

const IngestRequestSchema = z.object({
  code: z.string().optional(),
  name: z.string(),
  object: z.string(),
  session_name: z.string().optional(),
})

const GetRequestSchema = z.object({ code: z.string() })

export interface FileRpcDeps {
  bus: Bus
  files: BlobStore
}

/** Serve the file RPCs; returns a stop function. */
export function serveFileRpc(deps: FileRpcDeps): () => void {
  const { bus, files } = deps
  let closed = false
  const stops: Array<() => Promise<void>> = []

  const start = async (): Promise<void> => {
    const ingestSub = await bus.subscribe(FILE_INGEST_WILDCARD, {
      queue: 'agent.files',
    })
    stops.push(() => ingestSub.close())
    void (async () => {
      for await (const env of ingestSub) {
        if (closed) return
        const replyTo = env.reply_to
        if (replyTo === undefined || replyTo === '') continue
        const tenant = env.tenant
        const parsed = IngestRequestSchema.safeParse(env.payload)
        if (!parsed.success) {
          await bus.publish(
            replyTo,
            {
              ok: false,
              error: {
                code: 'invalid_argument',
                message: 'invalid ingest request',
              },
            },
            { tenant },
          )
          continue
        }
        try {
          const { name, session_name, object } = parsed.data
          const raw = await bus.objectGet(tenantObjectName(tenant, object))
          if (raw === null) {
            await bus.publish(
              replyTo,
              {
                ok: false,
                error: { code: 'not_found', message: 'ingest object missing' },
              },
              { tenant },
            )
            continue
          }
          const bytes = new Uint8Array(raw)
          if (bytes.length === 0) {
            await bus.publish(
              replyTo,
              {
                ok: false,
                error: {
                  code: 'invalid_argument',
                  message: 'file data is empty',
                },
              },
              { tenant },
            )
            continue
          }
          const cleanName = name.trim()
          if (cleanName === '') {
            await bus.publish(
              replyTo,
              {
                ok: false,
                error: {
                  code: 'invalid_argument',
                  message: 'file name is required',
                },
              },
              { tenant },
            )
            continue
          }
          // Single canonical store path: the agent derives the mime from the
          // bytes (the caller sends none), dedups by (tenant, sha) and probes.
          const record = await storeFile(
            { bus, files },
            {
              tenant,
              data: bytes,
              name: cleanName,
              uploaderSession: session_name ?? '',
              ...(parsed.data.code !== undefined
                ? { code: parsed.data.code }
                : {}),
            },
          )
          await bus.publish(
            replyTo,
            { ok: true, code: record.code, mime: record.mime },
            { tenant },
          )
        } catch (e) {
          logger.warn({ err: String(e), tenant }, 'file.ingest failed')
          await bus.publish(
            replyTo,
            { ok: false, error: { code: 'internal', message: String(e) } },
            { tenant },
          )
        }
      }
    })()

    const getSub = await bus.subscribe(FILE_GET_WILDCARD, {
      queue: 'agent.files',
    })
    stops.push(() => getSub.close())
    void (async () => {
      for await (const env of getSub) {
        if (closed) return
        const replyTo = env.reply_to
        if (replyTo === undefined || replyTo === '') continue
        const tenant = env.tenant
        const parsed = GetRequestSchema.safeParse(env.payload)
        if (!parsed.success) {
          await bus.publish(
            replyTo,
            {
              ok: false,
              error: {
                code: 'invalid_argument',
                message: 'invalid get request',
              },
            },
            { tenant },
          )
          continue
        }
        try {
          const got = await files.get(tenant, parsed.data.code)
          // Bytes go to the transient object store; only the reference rides
          // the reply (arbitrary size, no broker max_payload concern).
          const object = `${randomUUID()}.get`
          await bus.objectPut(tenantObjectName(tenant, object), got.data)
          await bus.publish(
            replyTo,
            {
              ok: true,
              meta: {
                code: got.meta.code,
                sha256: got.meta.sha256,
                name: got.meta.name,
                mime: got.meta.mime,
                size: got.meta.size,
                uploader_session: got.meta.uploader_session,
                created_at: got.meta.created_at,
                ...(got.meta.width != null ? { width: got.meta.width } : {}),
                ...(got.meta.height != null ? { height: got.meta.height } : {}),
                ...(got.meta.duration_ms != null
                  ? { duration_ms: got.meta.duration_ms }
                  : {}),
                ...(got.meta.thumb_code != null
                  ? { thumb_code: got.meta.thumb_code }
                  : {}),
                ...(got.meta.thumbhash != null
                  ? { thumbhash: got.meta.thumbhash }
                  : {}),
              },
              object,
            },
            { tenant },
          )
        } catch (e) {
          await bus.publish(
            replyTo,
            { ok: false, error: { code: 'not_found', message: String(e) } },
            { tenant },
          )
        }
      }
    })()
  }

  void start().catch(e =>
    logger.error({ err: String(e) }, 'file rpc serve failed'),
  )

  return () => {
    closed = true
    for (const stop of stops) void stop().catch(() => {})
  }
}
