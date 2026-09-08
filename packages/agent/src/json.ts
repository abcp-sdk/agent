import { err, ok, type Result } from 'neverthrow'
import { z } from 'zod'

/**
 * A single source of truth for JSON deserialization.
 *
 * Never call `JSON.parse` directly — every inbound boundary uses `parse`
 * so malformed input is reported via a `neverthrow` Result rather
 * than throwing or silently nulling.
 *
 * `JSON.stringify` is safe for JSON-encodable values and IS permitted for
 * serialization (outbound only).
 */

export type Json = unknown

/** Parse raw text (or Uint8Array) into JSON with a shape-validating schema. */
export function parse<T extends z.ZodType>(
  schema: T,
  raw: string | Uint8Array | null | undefined,
): Result<z.infer<T>, string> {
  if (raw === null || raw === undefined) {
    return err('parse: input is null/undefined')
  }
  const text =
    raw instanceof Uint8Array ? Buffer.from(raw).toString('utf8') : raw
  let data: unknown
  try {
    data = JSON.parse(text)
  } catch (e) {
    return err(`parse: invalid JSON: ${String(e)}`)
  }
  const result = schema.safeParse(data)
  if (result.success) return ok(result.data)
  return err(`parse: schema mismatch: ${z.treeifyError(result.error)}`)
}

/** Serialize a value to a JSON string (outbound only). */
export function stringify(value: unknown): string {
  return JSON.stringify(value ?? null)
}

// ---- shared payload / envelope schemas (kept here to avoid a schema→db dep) ----

export const WakePayloadSchema = z.object({
  session_name: z.string(),
  type: z.enum(['user_prompt', 'interrupt', 'event']),
})
export type WakePayload = z.infer<typeof WakePayloadSchema>

/**
 * The durable mailbox message carried end-to-end over NATS: extensions and
 * the HTTP prompt route publish it; the agent's mailbox consumer parses it,
 * persists a row in the `mailbox` table, and acks. `id` is the producer-
 * generated row key (also used as the publisher-side msg-id) so JetStream
 * redeliveries are idempotent at the PG layer.
 */
export const MailboxEnvelopeSchema = z.object({
  id: z.string(),
  session_name: z.string(),
  type: z.string(),
  payload: z.unknown(),
})
export type MailboxEnvelope = z.infer<typeof MailboxEnvelopeSchema>

export const ContentPayloadSchema = z.object({
  content: z.string().optional(),
  text: z.string().optional(),
  prompt: z.string().optional(),
})
export type ContentPayload = z.infer<typeof ContentPayloadSchema>

export const ToolPartDataSchema = z.object({
  id: z.string(),
  name: z.string(),
  input: z.unknown(),
})
export type ToolPartData = z.infer<typeof ToolPartDataSchema>

export const ToolResultPartDataSchema = z.object({
  tool_use_id: z.string(),
  content: z.string(),
  metadata: z.unknown(),
})
export type ToolResultPartData = z.infer<typeof ToolResultPartDataSchema>

export const TextPartDataSchema = z.object({
  text: z.string(),
})
export type TextPartData = z.infer<typeof TextPartDataSchema>

/**
 * A user-attached file reference, persisted as its OWN part type (`file`)
 * so history and the UI can render it as a distinct attachment (not padded
 * into the prompt text). `code` is the blob key (`file:<code>`), always a
 * 16-char hex id minted by the file store.
 */
export const FilePartDataSchema = z.object({
  code: z.string(),
  name: z.string().optional(),
  mime: z.string().optional(),
  size: z.number().int().optional(),
})
export type FilePartData = z.infer<typeof FilePartDataSchema>

export const SummaryPartDataSchema = z.object({
  summary: z.string(),
  /** First message id of the verbatim tail kept after this checkpoint. */
  tail_from: z.string().nullish(),
})
export type SummaryPartData = z.infer<typeof SummaryPartDataSchema>

export const ToolResultEnvelopeSchema = z.object({
  call_id: z.string(),
  tool: z.string(),
  content: z.string(),
  content_object: z.string().optional(),
  metadata: z.unknown().optional(),
})
export type ToolResultEnvelope = z.infer<typeof ToolResultEnvelopeSchema>

/**
 * The canonical tool result: `content` is the natural-language text fed back
 * to the model; `metadata` is an opaque custom JSON the agent persists and
 * forwards verbatim (never parsed). Shared across the NATS boundary and the
 * persisted part data.
 */
export const ToolResultSchema = z.object({
  content: z.string(),
  metadata: z.unknown(),
})
export type ToolResult = z.infer<typeof ToolResultSchema>
