import type { JsonObject, JsonValue } from '@bufbuild/protobuf'
import { create, fromJson, toJson } from '@bufbuild/protobuf'
import { type Value, ValueSchema } from '@bufbuild/protobuf/wkt'

/**
 * Proto JSON/Value conversion helpers shared by the Connect handler modules.
 * All google.protobuf.Value / Struct wrapping goes through here.
 */

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null
}

/** Read an optional string field off an opaque object (no casts). */
export function fieldString(o: unknown, key: string): string | undefined {
  if (!isRecord(o)) return undefined
  const v: unknown = o[key]
  return typeof v === 'string' ? v : undefined
}

/** Wrap a raw JSON value into a google.protobuf.Value message. */
export function toValue(v: unknown): Value {
  return fromJson(ValueSchema, v as JsonValue)
}

/** Unwrap a google.protobuf.Value message into a raw JSON value. */
export function valueToRaw(value: Value | undefined | null): unknown {
  if (value === null || value === undefined) return null
  return toJson(ValueSchema, value)
}

/** Convert an unknown JSON-ish value into a typed JsonValue (no casts). */
export function toJsonValue(v: unknown): JsonValue {
  if (
    v === null ||
    typeof v === 'string' ||
    typeof v === 'number' ||
    typeof v === 'boolean'
  ) {
    return v
  }
  if (Array.isArray(v)) return v.map(toJsonValue)
  if (isRecord(v)) return toJsonObject(v)
  return String(v)
}

/** Convert a record of unknowns into a typed JsonObject field by field. */
export function toJsonObject(v: Record<string, unknown>): JsonObject {
  const out: JsonObject = {}
  for (const [key, value] of Object.entries(v)) {
    out[key] = toJsonValue(value)
  }
  return out
}

// ---- Struct helpers (google.protobuf.Value wrapping) ----

export function toStructValue(v: unknown): Record<string, unknown> {
  if (v === null || v === undefined) return { case: 'nullValue', value: 0 }
  if (typeof v === 'string') return { case: 'stringValue', value: v }
  if (typeof v === 'number') return { case: 'numberValue', value: v }
  if (typeof v === 'boolean') return { case: 'boolValue', value: v }
  if (Array.isArray(v))
    return { case: 'listValue', value: { values: v.map(toStructValue) } }
  if (isRecord(v))
    return {
      case: 'structValue',
      value: { fields: toStructFields(v) },
    }
  return { case: 'stringValue', value: String(v) }
}

export function toStructFields(
  o: Record<string, unknown>,
): Record<string, unknown> {
  const fields: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(o)) fields[k] = toStructValue(v)
  return fields
}
