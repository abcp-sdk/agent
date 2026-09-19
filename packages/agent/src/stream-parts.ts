/**
 * Sanitize one AI SDK `fullStream` part into a JSON-safe payload for the
 * durable session-event bus.
 *
 * The event bus frame is `JSON.stringify`'d (NATS envelope) and then wrapped
 * into a `google.protobuf.Struct` by the Connect watch handler, so every value
 * must be a JSON primitive / array / object. A raw `TextStreamPart` is NOT:
 *
 *   - `file` / `reasoning-file` carry a `GeneratedFile` whose `uint8Array` is a
 *     `Uint8Array` (JSON.stringify turns it into `{"0":..,"1":..}` — a huge,
 *     useless object) and whose `base64` can be megabytes;
 *   - `error.error` and `raw.rawValue` are `unknown` (an `Error` serializes to
 *     `{}`);
 *   - `start-step.request` / `finish-step.response` embed request/response
 *     bodies (base64 images, full messages).
 *
 * The policy is: pass the AI SDK event name and its payload through VERBATIM
 * for anything JSON-safe, and normalise the few unsafe fields. `file` /
 * `reasoning-file` bytes are NEVER inlined — every such part is stored in the
 * blob store and the event carries `file:<code>` (see `sanitizeStreamPart`).
 */
import type { BlobStore, FileRecord } from './files.js'

export interface SanitizeDeps {
  files: BlobStore
  bus: unknown
  tenant: string
  session: string
  /** Injected so this module never imports the code generator directly. */
  mintCode: () => string
  sha256Hex: (data: Uint8Array) => string
  upsertFile: (
    bus: unknown,
    tenant: string,
    record: FileRecord,
  ) => Promise<unknown>
}

/** Recursively coerce an unknown value into a JSON-safe one. */
function jsonSafe(v: unknown, depth = 0): unknown {
  if (v === null || v === undefined) return null
  const t = typeof v
  if (t === 'string' || t === 'boolean') return v
  if (t === 'number') return Number.isFinite(v as number) ? v : String(v)
  if (t === 'bigint') return (v as bigint).toString()
  if (t === 'function' || t === 'symbol') return String(v)
  if (v instanceof Uint8Array) return Buffer.from(v).toString('base64')
  if (v instanceof Error) return { name: v.name, message: v.message }
  if (Array.isArray(v)) {
    if (depth > 24) return '[max-depth]'
    return v.map(x => jsonSafe(x, depth + 1))
  }
  if (t === 'object') {
    if (depth > 24) return '[max-depth]'
    const out: Record<string, unknown> = {}
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      // Drop the raw byte view: it is never useful on the wire and explodes
      // into a keyed object. `base64` (kept below) carries the bytes.
      if (k === 'uint8Array') continue
      out[k] = jsonSafe(val, depth + 1)
    }
    return out
  }
  return String(v)
}

/**
 * Normalise one `fullStream` part. `part` is the raw AI SDK part (typed
 * `unknown` on purpose — the union is huge and evolves with the SDK; we never
 * want a new part type to fail compilation or silently crash the turn).
 *
 * Returns the payload to publish under `part.type`. For `file` /
 * `reasoning-file` this may await blob storage.
 */
export async function sanitizeStreamPart(
  part: { type: string } & Record<string, unknown>,
  deps: SanitizeDeps,
): Promise<Record<string, unknown>> {
  // Media parts: bytes NEVER ride the event. Store every `file` /
  // `reasoning-file` in the blob store (any size) and emit `file:<code>`.
  if (part.type === 'file' || part.type === 'reasoning-file') {
    const file = part.file as
      | { base64?: string; mediaType?: string; uint8Array?: Uint8Array }
      | undefined
    const base64 = typeof file?.base64 === 'string' ? file.base64 : ''
    const mediaType = file?.mediaType ?? 'application/octet-stream'
    const bytes =
      file?.uint8Array !== undefined
        ? file.uint8Array
        : base64.length > 0
          ? Buffer.from(base64, 'base64')
          : Buffer.alloc(0)
    const code = deps.mintCode()
    const name = `model-${part.type}-${Date.now()}`
    const record: FileRecord = {
      code,
      sha256: deps.sha256Hex(bytes),
      name,
      mime: mediaType,
      size: bytes.length,
      uploader_session: deps.session,
      created_at: new Date().toISOString(),
    }
    await deps.files.put(deps.tenant, code, record, bytes)
    await deps.upsertFile(deps.bus, deps.tenant, record)
    return {
      type: part.type,
      mediaType,
      file: `file:${code}`,
      code,
      name,
      size: bytes.length,
      ...(part.providerMetadata !== undefined
        ? { providerMetadata: jsonSafe(part.providerMetadata) }
        : {}),
    }
  }

  // `start-step` / `finish-step` embed request/response bodies; keep only the
  // fields clients/tests consume.
  if (part.type === 'start-step') {
    return {
      type: 'start-step',
      ...(part.warnings !== undefined ? { warnings: jsonSafe(part.warnings) } : {}),
    }
  }
  if (part.type === 'finish-step') {
    const out: Record<string, unknown> = { type: 'finish-step' }
    for (const k of [
      'usage',
      'finishReason',
      'rawFinishReason',
      'providerMetadata',
    ] as const) {
      if (part[k] !== undefined) out[k] = jsonSafe(part[k])
    }
    return out
  }

  // `tool-result` / `tool-error` / `tool-output-denied`: pass the part through
  // verbatim, but ALSO add the derived convenience fields the existing clients
  // read (they predate transparent pass-through and would otherwise regress).
  if (part.type === 'tool-result') {
    const out = jsonSafe(part) as Record<string, unknown>
    const output = part.output as { content?: unknown; metadata?: unknown } | undefined
    const metadata = output?.metadata as Record<string, unknown> | null | undefined
    out['formatted'] = typeof output?.content === 'string' ? output.content : ''
    out['data'] = jsonSafe(output?.metadata ?? null)
    if (metadata && typeof metadata['change_id'] === 'string') {
      out['change_id'] = metadata['change_id']
    }
    return out
  }
  if (part.type === 'tool-error') {
    const out = jsonSafe(part) as Record<string, unknown>
    out['error'] = String(part.error ?? '')
    out['message'] = String(part.error ?? '')
    return out
  }
  if (part.type === 'tool-output-denied') {
    const out = jsonSafe(part) as Record<string, unknown>
    out['error'] = 'denied'
    return out
  }

  // Everything else: verbatim, recursively JSON-safe.
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(part)) {
    out[k] = jsonSafe(v)
  }
  return out
}
