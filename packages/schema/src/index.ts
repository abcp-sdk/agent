import {
  type MailboxEntry,
  MailboxEntrySchema as MailboxEntryDesc,
  type Message,
  MessageSchema as MessageDesc,
  type Part,
  PartSchema as PartDesc,
  type Preset,
  PresetSchema as PresetDesc,
  type Provider,
  ProviderSchema as ProviderDesc,
  type Session,
  SessionSchema as SessionDesc,
} from '@abcp/agent-sdk'
import {
  type DescField,
  type DescMessage,
  ScalarType,
} from '@bufbuild/protobuf'
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

// ---- database-row schemas -----------------------------------------------
//
// These validate the SERVER'S OWN database rows (sqlite/postgres columns),
// not the RPC wire messages — a different concern from the generated
// `agent.v1` messages. The shapes are derived from the SAME `.proto` message
// descriptors (single source of truth) rather than hand-maintained copies:
// each field is materialized as its JSON-realm primitive (int64 -> number,
// enums -> string, bytes -> string, sub-messages -> their scalar shape). This
// kills the drift between the DB row type and the proto message it mirrors.
//
// `ProtoRow` produces the snake_case JSON shape (the DB column naming, which
// equals the proto `name`), while `ProtoMsg` names the spec by the generated
// message's camelCase property names.

type ZodField = z.ZodTypeAny

/** A proto message -> zod object over its fields' JSON-realm primitives. */
function protoMessageSchema(desc: DescMessage): z.ZodObject<z.ZodRawShape> {
  const shape: Record<string, z.ZodType> = {}
  for (const f of desc.fields) shape[f.name] = zField(f)
  return z.object(shape)
}
function zField(f: DescField): ZodField {
  switch (f.fieldKind) {
    case 'scalar':
      return zScalar(f.scalar, f.name)
    case 'enum':
      return z.string()
    case 'list':
      return z.array(
        f.scalar !== undefined ? zScalar(f.scalar, f.name) : z.string(),
      )
    case 'map':
      return z.record(z.string(), z.string())
    case 'message':
      // Sub-messages do not appear in the flat DB rows; validate them permissively.
      return z.unknown()
    default:
      return z.unknown()
  }
}

/** enum -> string; int64 -> number; bytes -> string; everything else as-is. */
function zScalar(scalar: ScalarType, field: string): ZodField {
  switch (scalar) {
    case ScalarType.INT64:
    case ScalarType.UINT64:
    case ScalarType.SINT64:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
      // Postgres bigints are read/JSON-shaped as numbers by the server.
      return z.number().int().nullable()
    case ScalarType.INT32:
    case ScalarType.UINT32:
    case ScalarType.SINT32:
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
    case ScalarType.FLOAT:
    case ScalarType.DOUBLE:
      // NUMERIC/INTEGER DB columns are NULL-able.
      return z.number().nullable()
    case ScalarType.BOOL:
      return z.boolean().nullable()
    case ScalarType.BYTES:
      return z.string().nullable()
    case ScalarType.STRING:
      // MOST columns are NOT NULL, but a few nullable text columns exist
      // (tip_id / prev_id / effective_at / consumed_at / last_used_at); the
      // proto message has no presence bit for proto3 scalars, so the schema
      // accepts null and the row types are narrowed by their consumers.
      return z.string().nullable()
    default:
      throw new Error(`proto row field ${field}: unsupported scalar ${scalar}`)
  }
}

/** camelCase -> snake_case at the type level (protobuf field naming). */
type Snake<S extends string> = S extends `${infer H}${infer T}`
  ? H extends Uppercase<H>
    ? H extends Lowercase<H>
      ? `${H}${Snake<T>}`
      : `_${Lowercase<H>}${Snake<T>}`
    : `${H}${Snake<T>}`
  : S

/** The generated runtime shape carried on a `GenMessage` descriptor. */
type GenShape<G> = G extends { $codegenv2: { a: infer A } } ? A : never

/** Drops the protobuf runtime `$typeName` brand from a message shape. */
type Clean<T> = T extends { $typeName: string } ? Omit<T, '$typeName'> : T

/** Precise JSON-realm row shape for a generated message `G`, re-keyed by the
 *  snake_case proto field names (== the DB column naming). The protobuf
 *  runtime properties (`$typeName` / `$unknown`) are excluded. */
type DerivedRow<G> = {
  [K in keyof GenShape<G> as K extends string
    ? K extends `$${string}`
      ? never
      : Snake<K>
    : never]: Clean<GenShape<G>[K]>
}

/** DerivedRow with DB-representation overrides (a column stored as a JSON
 *  string while the proto field is a message/map/list) and proto-only fields
 *  dropped (UI aggregates the DB row does not carry). */
type RowOf<
  G,
  Overrides extends Record<string, unknown>,
  Drop extends keyof DerivedRow<G>,
> = Omit<DerivedRow<G>, Drop | keyof Overrides> & Overrides

/** Public row types: the proto message's snake_case JSON shape (the DB column
 *  naming), with the per-column DB representation/nullability applied. These
 *  are DERIVED from the `.proto` descriptors — no hand-maintained field list —
 *  so a proto change can never silently drift from the DB row type. */
export type SessionRow = RowOf<
  typeof SessionDesc,
  { tip_id: string | null; last_used_at: string | null },
  | 'tip_id'
  | 'last_used_at'
  | 'org'
  | 'repo'
  | 'branch'
  | 'unread_count'
  | 'last_message_at'
  | 'last_message_preview'
  | 'message_seq'
>
export type MessageRow = RowOf<
  typeof MessageDesc,
  { prev_id: string | null },
  'prev_id' | 'parts'
>
export type PartRow = DerivedRow<typeof PartDesc>
export type MailboxRow = RowOf<
  typeof MailboxEntryDesc,
  {
    effective_at: string | null
    consumed_at: string | null
    seq: number | null // int64 in proto; the DB/JSON realm exposes a number
  },
  'seq' | 'effective_at' | 'consumed_at'
>
export type PresetRow = RowOf<
  typeof PresetDesc,
  // JSON-encoded text columns (the DB stores these as strings).
  { system_prompt_i18n: string; tools: string },
  'system_prompt_i18n' | 'tools'
>
export type ProviderRow = RowOf<
  typeof ProviderDesc,
  { headers: string; models: string },
  'headers' | 'models'
>

export const SessionRowSchema = protoMessageSchema(SessionDesc)
export const MessageRowSchema = protoMessageSchema(MessageDesc)
export const PartRowSchema = protoMessageSchema(PartDesc)
export const MailboxRowSchema = protoMessageSchema(MailboxEntryDesc)
export const PresetRowSchema = protoMessageSchema(PresetDesc)
export const ProviderRowSchema = protoMessageSchema(ProviderDesc)

export type { MailboxEntry, Message, Part, Preset, Provider, Session }

// ---- extension server protocol ------------------------------------------
//
// Re-exported from @abc-protocol/sdk (the single source of truth for the
// agent <-> extension-server contract). Previously hand-copied here, which
// had already drifted (capability charset vs enum).

export type {
  ExtensionConfigItem,
  ExtensionManifest,
  ExtensionTool,
  ExtensionVariable,
  ExtensionVariableValue,
} from '@abc-protocol/sdk'
export {
  ExtensionConfigItemSchema,
  ExtensionManifestSchema,
  ExtensionToolSchema,
  ExtensionVariableSchema,
  ExtensionVariableValueSchema,
} from '@abc-protocol/sdk'

// Generated Connect types (strong-typed RPC contract from .proto).
//
// The TS generation lives in agent-sdk-typescript (@abcp/agent-sdk) — the
// single copy in this monorepo — and is re-exported here so this package
// stays the agent's single schema entry point. Do NOT keep a second copy of
// `agent_pb.ts` in this repo; update via agent-proto/scripts/sync-agent-sdks.sh.
export * from '@abcp/agent-sdk'
