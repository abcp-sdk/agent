import type { ReasoningOption } from '@opencode-ai/models'
import { z } from 'zod'

/**
 * Reasoning-variant derivation from the models.dev catalog.
 *
 * A "variant" is a selectable reasoning configuration for a model (e.g.
 * low/medium/high/max reasoning effort, a fast mode, or a token budget). The
 * catalog is keyed provider-first (`catalog[providerId].models[modelId]`), so
 * variants are resolved STRICTLY by the provider+model reference — never by a
 * flat model-id scan (a model id can be offered by several providers with
 * different capabilities).
 *
 * The caller passes the selected variant id back on the session; at turn time
 * the fragment is merged into AI-SDK `providerOptions` (and request headers).
 * No variant selected ⇒ no providerOptions sent (provider defaults).
 */

/** JSON value (mirrors the AI-SDK provider-options contract). */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }
export interface JsonObject {
  [key: string]: JsonValue
}

/** A selectable variant as surfaced over the API (id/name/description). */
export interface ModelVariant {
  id: string
  name: string
  description: string
}

/** A variant plus the AI-SDK wiring needed to apply it. */
export interface VariantDef extends ModelVariant {
  providerOptions: Record<string, JsonObject>
  headers?: Record<string, string>
}

// ---- catalog shape (validated) ----

const ReasoningOptionSchema = z.union([
  z.object({ type: z.literal('toggle') }),
  z.object({
    type: z.literal('effort'),
    values: z.array(z.union([z.string(), z.null()])),
  }),
  z.object({
    type: z.literal('budget_tokens'),
    min: z.number().optional(),
    max: z.number().optional(),
  }),
])

const JsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(JsonValueSchema),
    z.record(z.string(), JsonValueSchema),
  ]),
)
const JsonObjectSchema = z.record(z.string(), JsonValueSchema)

const ExperimentalModeSchema = z.object({
  provider: z
    .object({
      body: JsonObjectSchema.optional(),
      headers: z.record(z.string(), z.string()).optional(),
    })
    .optional(),
})

const CatalogModelSchema = z.object({
  reasoning: z.boolean().optional(),
  reasoning_options: z.array(ReasoningOptionSchema).optional(),
  limit: z.object({ context: z.number().nonnegative().optional() }).optional(),
  experimental: z
    .object({ modes: z.record(z.string(), ExperimentalModeSchema).optional() })
    .optional(),
})

/** A raw record lookup (navigates unvalidated JSON). */
function asRecord(v: unknown): Record<string, unknown> | null {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : null
}

export interface CatalogModel {
  reasoning: boolean
  reasoning_options: ReasoningOption[]
  contextLimit: number | undefined
  modes: Record<
    string,
    { body: JsonObject | undefined; headers: Record<string, string> | undefined }
  >
}

/**
 * Look up a single model strictly under `providerId` (no flat scan). The
 * models.dev catalog is validated PER MODEL only — a single malformed entry
 * elsewhere (e.g. a video model with a 0 context) must never invalidate the
 * whole catalog, and parsing the multi-MB catalog on every lookup is wasteful.
 */
export function catalogModel(
  modelsDev: unknown,
  providerId: string,
  modelId: string,
): CatalogModel | null {
  if (providerId === '' || modelId === '') return null
  const catalog = asRecord(modelsDev)
  if (catalog === null) return null
  const provider = asRecord(catalog[providerId])
  if (provider === null) return null
  const models = asRecord(provider['models'])
  if (models === null) return null
  const parsed = CatalogModelSchema.safeParse(models[modelId])
  if (!parsed.success) return null
  const m = parsed.data
  const modes: CatalogModel['modes'] = {}
  for (const [id, mode] of Object.entries(m.experimental?.modes ?? {})) {
    modes[id] = { body: mode.provider?.body, headers: mode.provider?.headers }
  }
  return {
    reasoning: m.reasoning ?? false,
    reasoning_options: (m.reasoning_options ?? []) as ReasoningOption[],
    contextLimit: m.limit?.context,
    modes,
  }
}

/**
 * Resolve a model's context window strictly by provider+model; falls back
 * when the provider/model is absent from the catalog.
 */
export function resolveContextLimit(
  modelsDev: unknown,
  providerId: string,
  modelId: string,
  fallback: number,
): number {
  return catalogModel(modelsDev, providerId, modelId)?.contextLimit ?? fallback
}

// ---- provider-option mapping ----

/** Normalize the api type to the AI-SDK provider-options namespace. */
function apiNamespace(apiType: string): string {
  const t = apiType.toLowerCase()
  if (t === 'claude') return 'anthropic'
  if (t === 'gemini') return 'google'
  if (t === 'openai_compatible') return 'openai-compatible'
  return t
}

function isAnthropic(ns: string): boolean {
  return ns === 'anthropic'
}
function isGoogle(ns: string): boolean {
  return ns === 'google'
}
function isOpenAI(ns: string): boolean {
  return ns === 'openai'
}
function isDeepSeek(ns: string): boolean {
  return ns === 'deepseek'
}

/** Google thinkingLevel accepts only these; clamp the wider effort domain. */
function googleLevel(value: string | null): string {
  switch (value) {
    case 'minimal':
    case 'low':
    case 'medium':
    case 'high':
      return value
    case null:
    case 'none':
    case 'default':
      return 'minimal'
    default:
      // xhigh/max and anything else → the highest supported level.
      return 'high'
  }
}

/** Reasoning-enabled provider options with no explicit level. */
function enableOptions(ns: string): Record<string, JsonObject> {
  if (isAnthropic(ns)) return { anthropic: { thinking: { type: 'enabled' } } }
  if (isGoogle(ns))
    return { google: { thinkingConfig: { includeThoughts: true } } }
  if (isDeepSeek(ns)) return { deepseek: { thinking: { type: 'enabled' } } }
  // openai / openai-compatible / fallback: a moderate effort level.
  return { [ns]: { reasoningEffort: 'medium' } }
}

/** Effort-level provider options. */
function effortOptions(
  ns: string,
  value: string | null,
): Record<string, JsonObject> | null {
  if (isAnthropic(ns)) {
    // Anthropic effort has no null/none literal; skip unsupported values.
    if (value === null || value === 'none' || value === 'default') return null
    return { anthropic: { effort: value } }
  }
  if (isGoogle(ns)) {
    return {
      google: {
        thinkingConfig: {
          includeThoughts: true,
          thinkingLevel: googleLevel(value),
        },
      },
    }
  }
  const effort = value === null ? 'none' : value
  return { [ns]: { reasoningEffort: effort } }
}

/** Token-budget provider options (high/max budgets). */
function budgetOptions(
  ns: string,
  tokens: number,
): Record<string, JsonObject> | null {
  if (isAnthropic(ns)) {
    return { anthropic: { thinking: { type: 'enabled', budgetTokens: tokens } } }
  }
  if (isGoogle(ns)) {
    return {
      google: { thinkingConfig: { includeThoughts: true, thinkingBudget: tokens } },
    }
  }
  // Budget is not a portable concept for the other providers.
  return null
}

function variantName(value: string | null): string {
  return value === null ? 'none' : value
}

function mergeOptions(
  a: Record<string, JsonObject>,
  b: Record<string, JsonObject>,
): Record<string, JsonObject> {
  const out: Record<string, JsonObject> = { ...a }
  for (const [ns, opts] of Object.entries(b)) {
    out[ns] = { ...(out[ns] ?? {}), ...opts }
  }
  return out
}

/**
 * Derive the selectable variants for a model under a given api type.
 * Effort options win when present; otherwise a toggle emits a single
 * reasoning-on variant and budget options emit high/max budget variants.
 * Experimental modes (e.g. "fast") become variants with request body/headers.
 */
export function variantsForApiType(
  model: CatalogModel,
  apiType: string,
): VariantDef[] {
  const ns = apiNamespace(apiType)
  const out: VariantDef[] = []
  const options = model.reasoning_options

  const effort = options.find(o => o.type === 'effort')
  if (effort !== undefined) {
    for (const raw of effort.values) {
      const value = raw === undefined ? null : raw
      const opts = effortOptions(ns, value)
      if (opts === null) continue
      out.push({
        id: variantName(value),
        name: variantName(value),
        description: `reasoning effort ${variantName(value)}`,
        providerOptions: opts,
      })
    }
  } else {
    const toggle = options.some(o => o.type === 'toggle')
    const budget = options.find(o => o.type === 'budget_tokens')
    if (budget !== undefined) {
      const base = toggle ? enableOptions(ns) : {}
      const max = budget.max
      const min = budget.min
      const high = Math.floor(((max ?? min ?? 0) + (min ?? 0)) / 2)
      for (const [id, tokens] of [
        ['high', min !== undefined ? Math.max(high, min) : high],
        ['max', max ?? high],
      ] as const) {
        const opts = budgetOptions(ns, tokens)
        if (opts === null) continue
        out.push({
          id,
          name: id,
          description: `reasoning budget ${tokens}`,
          providerOptions: mergeOptions(base, opts),
        })
      }
    } else if (toggle) {
      out.push({
        id: 'on',
        name: 'on',
        description: 'reasoning on',
        providerOptions: enableOptions(ns),
      })
    }
  }

  for (const [id, mode] of Object.entries(model.modes)) {
    out.push({
      id,
      name: id,
      description: `${id} mode`,
      providerOptions: mode.body !== undefined ? { [ns]: mode.body } : {},
      ...(mode.headers !== undefined ? { headers: mode.headers } : {}),
    })
  }

  return out
}

/**
 * Resolve a selected variant id to its wiring. Unknown/empty id ⇒ null (no
 * providerOptions applied).
 */
export function findVariant(
  model: CatalogModel,
  apiType: string,
  variantId: string,
): VariantDef | null {
  if (variantId === '') return null
  return (
    variantsForApiType(model, apiType).find(v => v.id === variantId) ?? null
  )
}

/** Strip the wiring to the API-facing shape (id/name/description). */
export function toModelVariant(v: VariantDef): ModelVariant {
  return { id: v.id, name: v.name, description: v.description }
}
