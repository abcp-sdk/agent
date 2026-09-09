import { err, ok, type Result } from 'neverthrow'
import { z } from 'zod'

/**
 * Single source of truth for JSON deserialization at the schema boundary.
 * Never call `JSON.parse` directly — decode raw input with a shape-validating
 * schema so malformed payloads surface as a `neverthrow` Result instead of
 * throwing or silently nulling.
 */
export function parse<T extends z.ZodType>(
  schema: T,
  raw: string | Uint8Array | null | undefined,
): Result<z.infer<T>, string> {
  if (raw === null || raw === undefined) {
    return err('parse: input is null/undefined')
  }
  let data: unknown
  try {
    data = JSON.parse(
      raw instanceof Uint8Array ? Buffer.from(raw).toString('utf8') : raw,
    )
  } catch (e) {
    return err(`parse: invalid JSON: ${String(e)}`)
  }
  const result = schema.safeParse(data)
  if (result.success) return ok(result.data)
  return err(`parse: schema mismatch: ${z.treeifyError(result.error)}`)
}

// ---- zod schemas (API contract, shared with server) ----------------

export const SessionRowSchema = z.object({
  name: z.string(),
  model: z.string(),
  preset: z.string(),
  tip_id: z.string().nullable(),
  max_turns: z.number().int(),
  system_prompt: z.string(),
  input_tokens: z.number().int(),
  output_tokens: z.number().int(),
  total_tokens: z.number().int(),
  last_input_tokens: z.number().int(),
  last_output_tokens: z.number().int(),
  created_at: z.string(),
  updated_at: z.string(),
  last_used_at: z.string().nullable(),
  locale: z.string().optional(),
})
export type SessionRow = z.infer<typeof SessionRowSchema>

export const MessageRowSchema = z.object({
  id: z.string(),
  role: z.string(),
  prev_id: z.string().nullable(),
  created_at: z.string(),
})
export type MessageRow = z.infer<typeof MessageRowSchema>

export const PartRowSchema = z.object({
  id: z.string(),
  message_id: z.string(),
  type: z.string(),
  seq: z.number().int(),
  data: z.string(),
})
export type PartRow = z.infer<typeof PartRowSchema>

export const MailboxRowSchema = z.object({
  id: z.string(),
  session_name: z.string(),
  msg_type: z.string(),
  payload: z.string(),
  effective_at: z.string().nullable(),
  status: z.string(),
  created_at: z.string(),
  consumed_at: z.string().nullable(),
  seq: z.number().int().nullable(),
})
export type MailboxRow = z.infer<typeof MailboxRowSchema>

export const PresetRowSchema = z.object({
  id: z.string(),
  system_prompt: z.string(),
  system_prompt_i18n: z.string().optional(),
  tools: z.string(),
  max_turns: z.number().int(),
  /** True when this is an immutable system preset (cannot be edited/deleted). */
  is_system: z.boolean().optional(),
})
export type PresetRow = z.infer<typeof PresetRowSchema>

export const ProviderRowSchema = z.object({
  provider_id: z.string(),
  api_type: z.string(),
  base_url: z.string(),
  api_key: z.string(),
  headers: z.string(),
  models: z.string(),
  updated_at: z.string(),
})
export type ProviderRow = z.infer<typeof ProviderRowSchema>

// ---- request bodies ----------------

export const CreateSessionBodySchema = z.object({
  name: z.string().min(1),
  model: z.string().optional(),
  preset: z.string().optional(),
})
export type CreateSessionBody = z.infer<typeof CreateSessionBodySchema>

export const PromptBodySchema = z.object({
  prompt: z.string(),
  /** Structured attachments, persisted as their own `file` parts. */
  attachments: z
    .array(
      z.object({
        code: z.string(),
        name: z.string().optional(),
        mime: z.string().optional(),
        size: z.number().int().optional(),
      }),
    )
    .optional(),
})
export type PromptBody = z.infer<typeof PromptBodySchema>

export interface FileAttachment {
  code: string
  name?: string
  mime?: string
  size?: number
}

/** Structured payload of a `file` part (user attachment ref). */
export interface FilePartData {
  code: string
  name?: string | undefined
  mime?: string | undefined
  size?: number | undefined
}

export const ForkBodySchema = z.object({
  name: z.string().min(1),
  /**
   * Optional fork point: a message id on the parent's chain. Absent forks
   * from the parent's current tip (legacy behavior); present forks from that
   * exact message, letting callers pin the fork to the moment they captured
   * the id. Validated with a chain-membership walk (see the undo route).
   */
  message_id: z.string().optional(),
  /**
   * Explicit preset for the forked session. When omitted the fork inherits
   * the parent's full config (model/preset/system_prompt/max_turns/locale).
   */
  preset: z.string().optional(),
})
export type ForkBody = z.infer<typeof ForkBodySchema>

export const RenameBodySchema = z.object({
  name: z.string().min(1),
})
export type RenameBody = z.infer<typeof RenameBodySchema>

export const ModelBodySchema = z.object({ model: z.string().min(1) })
export type ModelBody = z.infer<typeof ModelBodySchema>

export const UndoBodySchema = z.object({ message_id: z.string().optional() })
export type UndoBody = z.infer<typeof UndoBodySchema>

export const SessionSettingsBodySchema = z.object({
  model: z.string().optional(),
  preset: z.string().optional(),
  max_turns: z.number().int().optional(),
  system_prompt: z.string().optional(),
  locale: z.string().optional(),
})
export type SessionSettingsBody = z.infer<typeof SessionSettingsBodySchema>

export const PresetBodySchema = z.object({
  id: z.string().min(1),
  system_prompt: z.string().optional(),
  system_prompt_i18n: z.any().optional(),
  tools: z.unknown().optional(),
  max_turns: z.number().int().optional(),
})
export type PresetBody = z.infer<typeof PresetBodySchema>

export const ConfigBodySchema = z.object({
  key: z.string().min(1),
  value: z.string(),
})
export type ConfigBody = z.infer<typeof ConfigBodySchema>

export const ProviderModelBodySchema = z.object({
  id: z.string(),
  name: z.string().optional(),
})
export type ProviderModelBody = z.infer<typeof ProviderModelBodySchema>

export const ProviderBodySchema = z.object({
  provider_id: z.string().min(1),
  api_type: z.string().min(1),
  base_url: z.string().url(),
  api_key: z.string().optional(),
  headers: z.record(z.string(), z.unknown()).optional(),
  models: z.array(ProviderModelBodySchema).optional(),
})
export type ProviderBody = z.infer<typeof ProviderBodySchema>

export const ProviderTestBodySchema = z.object({
  api_type: z.string(),
  base_url: z.string().url(),
  api_key: z.string().optional(),
  /** When set, the test performs a real 1-turn text generation (proving the
   * model works), instead of only probing `GET /models`. */
  model: z.string().optional(),
})
export type ProviderTestBody = z.infer<typeof ProviderTestBodySchema>

// ---- SSE events ----------------

export const SSE_EVENT_NAMES = [
  'status',
  'text-delta',
  'tool-call',
  'tool-result',
  'error',
  'turn-complete',
] as const
export type SSEEventName = (typeof SSE_EVENT_NAMES)[number]

/** Parsed SSE delta params, discriminated by event type for the UI. */
export const SseTextDeltaParamsSchema = z.object({ text: z.string() })
export const SseErrorParamsSchema = z.object({ message: z.string() })
export const SseParamsSchema = z.union([
  z.object({ type: z.string() }),
  z.object({ text: z.string() }),
  z.object({ content: z.string() }),
  z.object({ message: z.string() }),
  z.object({ reason: z.string() }),
  z.object({ tool_use_id: z.string(), content: z.string() }),
  z.object({
    toolCallId: z.string(),
    toolName: z.string(),
    content: z.string(),
  }),
])
export const SSEEnvelopeSchema = z.object({
  event: z.string(),
  params: z.record(z.string(), z.unknown()).optional(),
  eid: z.string().optional(),
})
export type SSEEnvelope = z.infer<typeof SSEEnvelopeSchema>

// ---- API error envelope ----------------

export interface ApiErrorBody {
  ok: false
  error: string
}

// ---- typed API contract ----------------
//
// The single source of truth for the typed API surface is the server router:
// the UI derives its Hono client with `hc<AppType>()` from
// @easylab-agent/server (`AppType = typeof app`). No hand-written contract.

export type SessionJson = SessionRow

export const ProviderJsonSchema = z.object({
  provider_id: z.string(),
  api_type: z.string(),
  base_url: z.string(),
  api_key: z.string(),
  headers: z.record(z.string(), z.string()),
  models: z.array(z.string()),
})
export type ProviderJson = z.infer<typeof ProviderJsonSchema>

/** A single provider entry from the models.dev catalog (prefill hint). */
export const CatalogProviderSchema = z.object({
  id: z.string(),
  name: z.string(),
  api: z.string().optional(),
  npm: z.string(),
  env: z.array(z.string()),
  models: z.record(z.string(), z.unknown()),
})
export type CatalogProvider = z.infer<typeof CatalogProviderSchema>

export const CatalogProvidersSchema = z.record(
  z.string(),
  CatalogProviderSchema,
)
export type CatalogProviders = z.infer<typeof CatalogProvidersSchema>

// ---- extension server protocol ----------------
//
// The single source of truth for the agent ↔ extension-server contract. TS
// clients import these zod schemas directly; Go/Rust clients are generated
// from `z.toJSONSchema()` (see scripts/gen-extension-schema.mjs).

/** A tool manifest entry (identical shape to the existing tool servers). */
export const ExtensionToolSchema = z.object({
  name: z.string(),
  description: z.string(),
  descriptions: z.record(z.string(), z.string()).optional(),
  input_schema: z.record(z.string(), z.unknown()).optional(),
  /** Config names whose value this tool requires to run (may be shared). */
  required_config: z.array(z.string()).optional(),
})
export type ExtensionTool = z.infer<typeof ExtensionToolSchema>

/** A named template variable an extension can resolve (e.g. `org`, `repo`). */
export const ExtensionVariableSchema = z.object({
  name: z.string(),
  description: z.string().optional(),
})
export type ExtensionVariable = z.infer<typeof ExtensionVariableSchema>

/** A declared extension config knob (mirrors @abc-protocol ExtensionConfigItem). */
export const ExtensionConfigItemSchema = z.object({
  name: z.string(),
  type: z.enum(['string', 'number', 'boolean', 'enum', 'json']),
  enum_values: z.array(z.string()).optional(),
  default: z.unknown().optional(),
  description: z.string().optional(),
  // Localized config descriptions (locale → text); `description` is the
  // fallback, same convention as tool descriptions.
  descriptions: z.record(z.string(), z.string()).optional(),
  scope: z.enum(['global', 'session']).default('global'),
})
export type ExtensionConfigItem = z.infer<typeof ExtensionConfigItemSchema>

/** The manifest an extension serves at GET /api/v1/extension. */
export const ExtensionManifestSchema = z.object({
  id: z.string(),
  version: z.string(),
  capabilities: z.array(z.enum(['tools', 'prompt'])),
  tools: z.array(ExtensionToolSchema).optional(),
  prompt: z
    .object({
      variables: z.array(ExtensionVariableSchema).optional(),
    })
    .optional(),
  config: z.array(ExtensionConfigItemSchema).optional(),
})
export type ExtensionManifest = z.infer<typeof ExtensionManifestSchema>

/** A resolved template variable value (request/reply body). */
export const ExtensionVariableValueSchema = z.object({
  name: z.string(),
  value: z.string(),
})
export type ExtensionVariableValue = z.infer<
  typeof ExtensionVariableValueSchema
>

// Generated Connect types (strong-typed RPC contract from .proto).

// Agent.v1 RPC contract (locally generated from proto/agent/v1/agent.proto;
// decoupled from @easylab/sdk). agent_pb.ts carries the message types + the
// GenService descriptor (AgentService) the Connect router consumes.
export * from "./gen/agent/v1/agent_pb.js";
