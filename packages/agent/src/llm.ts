import { createAnthropic } from '@ai-sdk/anthropic'
import { createDeepSeek } from '@ai-sdk/deepseek'
import { createGoogle } from '@ai-sdk/google'
import { createOpenAI } from '@ai-sdk/openai'
import { createOpenAICompatible } from '@ai-sdk/openai-compatible'
import type { ProviderRow } from '@easylab-agent/schema'
import type { LanguageModel } from 'ai'
import { err, ok, type Result } from 'neverthrow'
import { z } from 'zod'
import type { ServerConfig } from './config.js'
import type { Db } from './db-client.js'
import { findProviderForModel, Providers } from './db-providers.js'
import { parse } from './json.js'
import { logger } from './logger.js'

const HeadersSchema = z.record(z.string(), z.string())
const StringArraySchema = z.array(z.string())

export interface ProviderCredentials {
  apiType: string
  baseUrl: string
  apiKey: string
  headers: Record<string, string>
}

const KNOWN_API_TYPES = new Set([
  'anthropic',
  'claude',
  'openai',
  'openai-compatible',
  'openai_compatible',
  'deepseek',
  'google',
  'gemini',
])

export function validateApiType(apiType: string): Result<void, string> {
  return KNOWN_API_TYPES.has(apiType.toLowerCase())
    ? ok(undefined)
    : err(
        `unknown api type: ${apiType} (expected anthropic|openai|openai-compatible|deepseek|google)`,
      )
}

/** Pure model factory — mirrors the provider registry. */
export function buildModelForApiType(
  credentials: ProviderCredentials,
  modelId: string,
): Result<LanguageModel, string> {
  const apiType = credentials.apiType.toLowerCase()
  const { baseUrl, apiKey, headers } = credentials
  const baseURL = baseUrl ? { baseURL: baseUrl } : {}

  switch (apiType) {
    case 'deepseek':
      return ok(
        createDeepSeek({ ...baseURL, apiKey, headers }).languageModel(modelId),
      )
    case 'anthropic':
    case 'claude':
      return ok(
        createAnthropic({ ...baseURL, apiKey, headers }).languageModel(modelId),
      )
    case 'openai':
      return ok(
        createOpenAI({ ...baseURL, apiKey, headers }).languageModel(modelId),
      )
    case 'google':
    case 'gemini':
      return ok(
        createGoogle({ ...baseURL, apiKey, headers }).languageModel(modelId),
      )
    case 'openai-compatible':
    case 'openai_compatible':
      return ok(
        createOpenAICompatible({
          name: 'openai-compatible',
          baseURL: baseUrl,
          apiKey,
          headers,
          includeUsage: true,
        }).languageModel(modelId),
      )
    default:
      return err(`unknown api type: ${apiType}`)
  }
}

export interface ResolvedModel {
  model: LanguageModel
  modelId: string
}

/**
 * Parse a `provider/model` reference (the model string used by the exposed
 * single-turn LLM endpoint). Extensions pass `provider_id/model_id` so the
 * provider is picked explicitly instead of by model-id lookup.
 * Returns null for any malformed reference.
 */
export function parseProviderModelRef(
  ref: string,
): { providerId: string; modelId: string } | null {
  if (ref === '') return null
  const idx = ref.indexOf('/')
  if (idx <= 0 || idx === ref.length - 1) return null
  const providerId = ref.slice(0, idx)
  const modelId = ref.slice(idx + 1)
  if (providerId === '' || modelId === '') return null
  return { providerId, modelId }
}

const CatalogModelSchema = z.object({
  limit: z
    .object({
      context: z.number().positive().optional(),
    })
    .optional(),
})

const CatalogModelsSchema = z.record(z.string(), CatalogModelSchema)

const CatalogProviderSchema = z.object({
  models: CatalogModelsSchema.optional(),
})

const CatalogSchema = z.record(z.string(), CatalogProviderSchema)

/**
 * Resolve the model's context window (tokens) from the models.dev catalog
 * (cached in NATS). Falls back to `fallback` when the model is unknown or
 * the catalog is unavailable.
 */
export function resolveContextLimit(
  modelsDev: unknown,
  modelId: string,
  fallback: number,
): number {
  const parsed = CatalogSchema.safeParse(modelsDev)
  if (!parsed.success) return fallback
  for (const p of Object.values(parsed.data)) {
    const models = p.models
    if (models === undefined) continue
    const hit = models[modelId]
    if (hit !== undefined) {
      return hit.limit?.context ?? fallback
    }
  }
  return fallback
}

/**
 * Per-request provider resolution with a client cache. A model advertised by
 * a registered provider is served by that provider; anything else falls back
 * to the bootstrap default (env-configured).
 */
export class LlmRegistry {
  private readonly cache = new Map<string, LanguageModel>()

  constructor(private readonly config: ServerConfig) {}

  invalidate(): void {
    this.cache.clear()
  }

  defaultModelId(): string {
    return this.config.llmModel
  }

  async resolve(
    db: Db,
    modelId: string,
  ): Promise<Result<ResolvedModel, string>> {
    // No explicit session model: prefer the env-configured default when the
    // operator set one; otherwise fall through to the first registered
    // provider's model (never a hardcoded/synthetic default provider).
    const id = modelId === '' ? this.config.llmModel : modelId
    const rows = await Providers.list(db)
    if (rows.isErr()) return err(rows.error)
    if (id !== '') return this.resolveRows(rows.value, id)
    if (this.config.llmModel !== '') return this.resolveRows(rows.value, id)
    // Model unset AND no env default: use the first registered provider model.
    for (const p of rows.value) {
      const arr = parse(StringArraySchema, p.models)
      if (arr.isOk() && arr.value.length > 0) {
        return this.resolveRows(rows.value, arr.value[0]!)
      }
    }
    return err(
      'no model selected — register a provider or pick a model',
    )
  }

  /** Pure resolution over a provider snapshot (unit-testable). */
  resolveRows(
    rows: ProviderRow[],
    modelId: string,
  ): Result<ResolvedModel, string> {
    const hit = findProviderForModel(rows, modelId)
    if (hit !== null) {
      const cached = this.cache.get(hit.provider_id)
      if (cached !== undefined) return ok({ model: cached, modelId })
      const creds: ProviderCredentials = {
        apiType: hit.api_type,
        baseUrl: hit.base_url,
        apiKey: hit.api_key,
        headers: parseHeaders(hit.headers),
      }
      const built = buildModelForApiType(creds, modelId)
      if (built.isOk()) {
        this.cache.set(hit.provider_id, built.value)
        return ok({ model: built.value, modelId })
      }
      // Invalid provider row: log-and-fallthrough to the default below.
      logger.warn(
        { provider: hit.provider_id, err: built.error },
        'provider unusable, falling back',
      )
    }
    // No registered provider advertises the session model. Only fall back to
    // the env-configured LLM when an operator explicitly supplies one; with no
    // configured provider/model (the empty defaults) we must NOT invent a
    // default provider — fail loudly instead so the user configures one.
    if (
      this.config.llmApiType === '' &&
      this.config.llmBaseUrl === '' &&
      this.config.llmApiKey === '' &&
      this.config.llmModel === ''
    ) {
      return err(
        `no provider configured for model '${modelId}' — register a provider or set LLM_*`,
      )
    }
    const fallback = buildModelForApiType(
      {
        apiType: this.config.llmApiType,
        baseUrl: this.config.llmBaseUrl,
        apiKey: this.config.llmApiKey,
        headers: {},
      },
      modelId,
    )
    return fallback.map(model => ({ model, modelId }))
  }

  /**
   * Resolve an explicit `provider_id/model_id` reference from a registered
   * provider. The referenced provider must exist; no fallback is applied
   * (a tool that asks for a specific model wants that model or an error).
   */
  async resolveByProvider(
    db: Db,
    providerId: string,
    modelId: string,
  ): Promise<Result<ResolvedModel, string>> {
    const rows = await Providers.list(db)
    if (rows.isErr()) return err(rows.error)
    const hit = rows.value.find(r => r.provider_id === providerId)
    if (hit === undefined) {
      return err(`provider not found: ${providerId}`)
    }
    const creds: ProviderCredentials = {
      apiType: hit.api_type,
      baseUrl: hit.base_url,
      apiKey: hit.api_key,
      headers: parseHeaders(hit.headers),
    }
    const built = buildModelForApiType(creds, modelId)
    if (built.isErr()) return err(built.error)
    this.cache.set(hit.provider_id, built.value)
    return ok({ model: built.value, modelId })
  }
}

function parseHeaders(raw: string): Record<string, string> {
  return parse(HeadersSchema, raw).match(
    headers => headers,
    () => ({}),
  )
}
