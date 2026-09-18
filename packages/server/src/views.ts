import type { JsonObject } from '@bufbuild/protobuf'
import { create, fromJson, toJson } from '@bufbuild/protobuf'
import {
  type AgentDeps,
  type Bus,
  DEFAULT_PRESET,
  findVariant,
  getModelsDev,
  maskSecret,
  pickLocalized,
  toModelVariant,
} from '@abcp-agent/agent'
import {
  GetFileResponseSchema,
  ListToolsResponseSchema,
  type Session,
  type ToolInfo,
} from '@abcp-agent/schema'

export interface SessionRowView {
  name: string
  model?: string | null | undefined
  variant?: string | null | undefined
  preset?: string | null | undefined
  tip_id?: string | null | undefined
  max_turns?: number | null | undefined
  system_prompt?: string | null | undefined
  input_tokens?: number | null | undefined
  output_tokens?: number | null | undefined
  total_tokens?: number | null | undefined
  last_input_tokens?: number | null | undefined
  last_output_tokens?: number | null | undefined
  created_at?: string | null | undefined
  updated_at?: string | null | undefined
  last_used_at?: string | null | undefined
  locale?: string | null | undefined
  org?: string | null | undefined
  repo?: string | null | undefined
  branch?: string | null | undefined
  unread_count?: number | null | undefined
  last_message_at?: string | null | undefined
  last_message_preview?: string | null | undefined
  group?: string | null | undefined
}

export interface ProviderRowView {
  provider_id: string
  capability?: string | null | undefined
  api_type?: string | null | undefined
  base_url?: string | null | undefined
  api_key?: string | null | undefined
  headers?: string | null | undefined
  models?: string | null | undefined
  updated_at?: string | null | undefined
}

export interface PresetRowView {
  id: string
  system_prompt?: string | null | undefined
  system_prompt_i18n?: string | null | undefined
  tools?: string | null | undefined
  max_turns?: number | null | undefined
  is_system?: boolean | null | undefined
}

/** Runtime type guard: a plain string-keyed object (not an array). */

export function sessionToMsg(
  s: SessionRowView,
  fact?: {
    last_message_at: string
    last_message_preview: string
    message_seq: number
  },
) {
  return {
    name: s.name,
    model: s.model ?? '',
    variant: s.variant ?? '',
    preset: s.preset ?? '',
    tipId: s.tip_id ?? '',
    maxTurns: s.max_turns ?? 0,
    systemPrompt: s.system_prompt ?? '',
    inputTokens: s.input_tokens ?? 0,
    outputTokens: s.output_tokens ?? 0,
    totalTokens: s.total_tokens ?? 0,
    lastInputTokens: s.last_input_tokens ?? 0,
    lastOutputTokens: s.last_output_tokens ?? 0,
    createdAt: s.created_at ?? '',
    updatedAt: s.updated_at ?? '',
    lastUsedAt: s.last_used_at ?? '',
    locale: s.locale ?? '',
    group: s.group ?? '',
    org: s.org ?? '',
    repo: s.repo ?? '',
    branch: s.branch ?? '',
    unreadCount: 0,
    lastMessageAt: fact?.last_message_at ?? '',
    lastMessagePreview: fact?.last_message_preview ?? '',
    messageSeq: fact?.message_seq ?? 0,
  }
}

/**
 * Parse the stored provider `models` JSON (an array of
 * `{ id, name?, context_limit, model_type? }`) into the proto ProviderModel
 * shape. A bare string entry is also tolerated (name=id, context_limit=0) so a
 * malformed row never breaks listing. `model_type` is display-only kind
 * metadata from the gateway `/config` (empty when unknown / a text provider).
 */
export function parseProviderModels(raw: string | null | undefined): {
  id: string
  name: string
  contextLimit: bigint
  modelType: string
}[] {
  let arr: unknown = []
  try {
    arr = JSON.parse(raw ?? '[]') ?? []
  } catch {
    return []
  }
  if (!Array.isArray(arr)) return []
  const out: {
    id: string
    name: string
    contextLimit: bigint
    modelType: string
  }[] = []
  for (const item of arr) {
    if (typeof item === 'string') {
      if (item !== '')
        out.push({ id: item, name: item, contextLimit: 0n, modelType: '' })
      continue
    }
    if (item === null || typeof item !== 'object') continue
    const v = item as Record<string, unknown>
    const id = String(v['id'] ?? '')
    if (id === '') continue
    out.push({
      id,
      name: String(v['name'] ?? id),
      contextLimit: BigInt(Math.trunc(Number(v['context_limit'] ?? 0)) || 0),
      modelType: String(v['model_type'] ?? ''),
    })
  }
  return out
}

/** Provider row -> proto message. Exported for masking tests.
 * The api key is ALWAYS masked (see maskSecret). */
export function providerToMsg(p: ProviderRowView) {
  let headers: Record<string, string> = {}
  try {
    headers = JSON.parse(p.headers ?? '{}') ?? {}
  } catch {}
  return {
    providerId: p.provider_id ?? '',
    capability: p.capability ?? 'text',
    apiType: p.api_type ?? '',
    baseUrl: p.base_url ?? '',
    // Masked: the plaintext never leaves the server. The mask doubles as
    // the edit-time sentinel (see registerProvider).
    apiKey: maskSecret(p.api_key ?? ''),
    headers,
    models: parseProviderModels(p.models),
    updatedAt: p.updated_at ?? '',
  }
}

export function presetToMsg(p: PresetRowView, locale?: string) {
  let tools: string[] = []
  try {
    tools = JSON.parse(p.tools ?? '[]') ?? []
  } catch {}
  return {
    id: p.id ?? '',
    systemPrompt: presetPromptFor(p, locale ?? ''),
    tools,
    maxTurns: p.max_turns ?? 0,
    isSystem: p.is_system ?? false,
  }
}

/** Resolve a preset's system prompt for [locale]: parse `system_prompt_i18n`
 * as a { locale: template } map and pick the entry (exact → primary language
 * → fallback to the default `system_prompt`). */
export function presetPromptFor(p: PresetRowView, locale: string): string {
  const i18n = p.system_prompt_i18n
  if (i18n !== undefined && i18n !== null && i18n !== '{}' && i18n !== '') {
    try {
      const map = JSON.parse(i18n) as Record<string, unknown>
      const picked = pickLocalized(map as Record<string, string>, locale)
      if (picked !== null) return picked
    } catch {
      /* fall through to the default prompt */
    }
  }
  return p.system_prompt ?? ''
}

/**
 * Resolve a session's effective (preset, model), applying tenant defaults and
 * VALIDATING both. One gate for BOTH create and update so an API client can
 * neither blank a working setting nor write an unregistered one.
 *
 *  - preset: requested → (update: keep current) → tenant `default_preset` →
 *    built-in `default`. MUST exist for the tenant, else InvalidArgument.
 *  - model: requested → (update: keep current) → tenant `default_model` → ''.
 *    A non-empty model MUST be a resolvable `provider_id/model_id` (provider
 *    registered, model buildable), else InvalidArgument. Empty is allowed on
 *    create (a session may exist without a model until the first turn).
 */
