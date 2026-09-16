import { createAnthropic } from '@ai-sdk/anthropic'
import { createDeepSeek } from '@ai-sdk/deepseek'
import { createGateway } from '@ai-sdk/gateway'
import { createGoogle } from '@ai-sdk/google'
import { createOpenAI } from '@ai-sdk/openai'
import { createOpenAICompatible } from '@ai-sdk/openai-compatible'
import type { LanguageModel } from 'ai'
import { err, ok, type Result } from 'neverthrow'
import { z } from 'zod'
import type { Db } from './db-client.js'
import { Providers } from './db-providers.js'
import { parse } from './json.js'
import { logger } from './logger.js'

const HeadersSchema = z.record(z.string(), z.string())

/**
 * The one special api type that fronts a Vercel-AI-SDK-compatible gateway
 * (`/v4/ai`). Unlike the text providers, a gateway is a SUPERSET: it can serve
 * the language model AND every multimodal capability (image / video / speech /
 * transcription). It is registered at most once (see the server validation).
 */
export const GATEWAY_API_TYPE = 'vercel-compatible-gateway'

/**
 * The gateway provider is a SINGLETON with a fixed id: multimodal model refs
 * are always `gateway/<model-id>` (the Vercel-compatible gateway is the only
 * thing that can serve image/video/speech/transcription models).
 */
export const GATEWAY_PROVIDER_ID = 'gateway'

export interface ProviderCredentials {
  providerId: string
  apiType: string
  baseUrl: string
  apiKey: string
  headers: Record<string, string>
}

/**
 * Non-text model capability, used by the TOOLS (and the provider test). It is
 * NOT stored on a provider model: a model's capability is implied by the tool
 * config knob that references it (image_model / video_model / tts_model /
 * asr_model). Kept here for the test-provider probe dispatch.
 */
export type ModelCapability =
  | 'text'
  | 'image'
  | 'video'
  | 'speech'
  | 'transcription'

export const MODEL_CAPABILITIES: readonly ModelCapability[] = [
  'text',
  'image',
  'video',
  'speech',
  'transcription',
]

/** Normalize a client-supplied capability string; empty = text. */
export function parseCapability(raw: string): Result<ModelCapability, string> {
  const v = raw.trim().toLowerCase()
  if (v === '') return ok('text')
  const hit = MODEL_CAPABILITIES.find(c => c === v)
  return hit !== undefined
    ? ok(hit)
    : err(
        `unknown capability: ${raw} (expected text|image|video|speech|transcription)`,
      )
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
  GATEWAY_API_TYPE,
])

export function validateApiType(apiType: string): Result<void, string> {
  return KNOWN_API_TYPES.has(apiType.toLowerCase())
    ? ok(undefined)
    : err(
        `unknown api type: ${apiType} (expected anthropic|openai|openai-compatible|deepseek|google|${GATEWAY_API_TYPE})`,
      )
}

export function isGatewayApiType(apiType: string): boolean {
  return apiType.toLowerCase() === GATEWAY_API_TYPE
}

/**
 * Default context window assigned to a gateway LANGUAGE model discovered via
 * `/config` (the gateway does not advertise one). It is only a starting point
 * — the user can edit it. Multimodal models always get 0 (which, together with
 * a positive limit on text, is how listModels/tool knobs tell them apart).
 */
export const GATEWAY_DEFAULT_CONTEXT_LIMIT = 200_000

/**
 * A model discovered from the gateway's `/config` (AI-SDK's
 * `getAvailableModels`). `modelType` distinguishes language from the
 * multimodal kinds; `model_type` is the normalized display kind.
 */
export interface DiscoveredGatewayModel {
  id: string
  name: string
  contextLimit: number
  /** Normalized kind: text|image|video|speech|transcription|<raw>. */
  modelType: string
}

/**
 * Normalize a gateway `/config` `modelType` to a short display tag. `language`
 * → `text`; every other known kind is passed through; unknown/empty → `''`.
 */
export function normalizeGatewayModelType(
  raw: string | null | undefined,
): string {
  const t = (raw ?? '').trim().toLowerCase()
  if (t === '') return ''
  if (t === 'language') return 'text'
  return t
}

/**
 * Ask a Vercel-compatible gateway which models it serves and classify each by
 * its advertised `modelType` (language → a real context limit, everything else
 * → 0). Throws-free: returns an error string for the caller to surface.
 */
export async function discoverGatewayModels(
  credentials: ProviderCredentials,
): Promise<Result<DiscoveredGatewayModel[], string>> {
  if (!isGatewayApiType(credentials.apiType)) {
    return err(
      `model discovery requires a '${GATEWAY_API_TYPE}' provider (got '${credentials.apiType}')`,
    )
  }
  try {
    const gw = buildGateway(credentials)
    const { models } = await gw.getAvailableModels()
    const out: DiscoveredGatewayModel[] = []
    for (const m of models) {
      const type = normalizeGatewayModelType(m.modelType)
      const language = type === '' || type === 'text'
      out.push({
        id: m.id,
        name: m.name !== '' ? m.name : m.id,
        contextLimit: language ? GATEWAY_DEFAULT_CONTEXT_LIMIT : 0,
        modelType: type,
      })
    }
    return ok(out)
  } catch (e) {
    return err(`gateway model discovery failed: ${String(e)}`)
  }
}

function buildGateway(credentials: ProviderCredentials) {
  return createGateway({
    baseURL: credentials.baseUrl,
    apiKey: credentials.apiKey,
    ...(Object.keys(credentials.headers).length > 0
      ? { headers: credentials.headers }
      : {}),
  })
}

/** Pure text (language) model factory — mirrors the provider registry. */
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
    case GATEWAY_API_TYPE:
      return ok(buildGateway(credentials).languageModel(modelId))
    default:
      return err(`unknown api type: ${apiType}`)
  }
}

/**
 * Multimodal-model factory. MULTIMODAL MODELS ARE ONLY SERVED BY THE
 * Vercel-compatible gateway: a direct openai/google/anthropic provider has no
 * unified generative surface (and their video/image protocols diverge), so a
 * non-gateway provider is rejected here. The gateway returns the
 * capability-specific AI-SDK model object driven by `generateImage` /
 * `experimental_generateVideo` / `generateSpeech` / `transcribe`.
 */
export function buildGenerativeModel(
  credentials: ProviderCredentials,
  modelId: string,
  capability: Exclude<ModelCapability, 'text'>,
): Result<unknown, string> {
  if (!isGatewayApiType(credentials.apiType)) {
    return err(
      `multimodal models require a '${GATEWAY_API_TYPE}' gateway — ` +
        `provider '${credentials.providerId}' is '${credentials.apiType}'`,
    )
  }
  const gw = buildGateway(credentials)
  switch (capability) {
    case 'image':
      return ok(gw.imageModel(modelId))
    case 'video':
      return ok(gw.videoModel(modelId))
    case 'speech':
      return ok(gw.speechModel(modelId))
    case 'transcription':
      return ok(gw.transcriptionModel(modelId))
    default:
      return err(`unsupported multimodal capability: ${capability}`)
  }
}

export interface ResolvedModel {
  model: LanguageModel
  modelId: string
  providerId: string
  apiType: string
}

/** A resolved multimodal model (capability-tagged, not a LanguageModel). */
export interface ResolvedGenerativeModel {
  model: unknown
  modelId: string
  providerId: string
  apiType: string
  capability: Exclude<ModelCapability, 'text'>
}

/**
 * Parse a canonical `provider/model` reference. This is the ONLY accepted
 * model reference: a provider is always required, so a model id offered by
 * several providers is never ambiguous. Returns null for malformed input.
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

/**
 * Build a canonical `provider_id/model_id` reference.
 */
export function modelRef(providerId: string, modelId: string): string {
  return `${providerId}/${modelId}`
}

/**
 * Per-request provider resolution with a client cache. A model is ALWAYS
 * addressed as `provider_id/model_id`; there is no flat (model-id-only)
 * resolution — that would be ambiguous when several providers offer the same
 * model id.
 */
export class LlmRegistry {
  private readonly cache = new Map<string, LanguageModel>()
  private readonly genCache = new Map<string, ResolvedGenerativeModel>()

  invalidate(): void {
    this.cache.clear()
    this.genCache.clear()
  }

  /**
   * Resolve a canonical `provider_id/model_id` reference. A model is ALWAYS
   * addressed explicitly by provider + model: there is NO fallback when the
   * reference is empty or malformed. The referenced provider must exist.
   */
  async resolve(
    db: Db,
    tenant: string,
    ref: string,
  ): Promise<Result<ResolvedModel, string>> {
    const parsed = parseProviderModelRef(ref)
    if (parsed === null) {
      return err(
        `no model selected — set a model as "provider_id/model_id" (got ${JSON.stringify(ref)})`,
      )
    }
    return this.resolveByProvider(db, tenant, parsed.providerId, parsed.modelId)
  }

  /**
   * Resolve an explicit `provider_id/model_id` reference from a registered
   * provider. The referenced provider must exist; no fallback is applied.
   */
  async resolveByProvider(
    db: Db,
    tenant: string,
    providerId: string,
    modelId: string,
  ): Promise<Result<ResolvedModel, string>> {
    const rows = await Providers.list(db, tenant)
    if (rows.isErr()) return err(rows.error)
    const hit = rows.value.find(r => r.provider_id === providerId)
    if (hit === undefined) {
      return err(`provider not found: ${providerId}`)
    }
    const creds: ProviderCredentials = {
      providerId: hit.provider_id,
      apiType: hit.api_type,
      baseUrl: hit.base_url,
      apiKey: hit.api_key,
      headers: parseHeaders(hit.headers),
    }
    const cacheKey = `${tenant}\n${hit.provider_id}/${modelId}`
    const cached = this.cache.get(cacheKey)
    if (cached !== undefined) {
      return ok({
        model: cached,
        modelId,
        providerId: hit.provider_id,
        apiType: hit.api_type,
      })
    }
    const built = buildModelForApiType(creds, modelId)
    if (built.isErr()) {
      logger.warn(
        { provider: hit.provider_id, err: built.error },
        'provider unusable',
      )
      return err(built.error)
    }
    this.cache.set(cacheKey, built.value)
    return ok({
      model: built.value,
      modelId,
      providerId: hit.provider_id,
      apiType: hit.api_type,
    })
  }

  /**
   * Resolve a multimodal model (image / video / speech / transcription). The
   * referenced provider must be the gateway; no fallback: a tool asking for a
   * specific model gets that model or an error.
   */
  async resolveGenerative(
    db: Db,
    tenant: string,
    ref: string,
    capability: Exclude<ModelCapability, 'text'>,
  ): Promise<Result<ResolvedGenerativeModel, string>> {
    const parsed = parseProviderModelRef(ref)
    if (parsed === null) {
      return err(
        `no model selected — set a ${capability} model as "provider_id/model_id" (got ${JSON.stringify(ref)})`,
      )
    }
    const rows = await Providers.list(db, tenant)
    if (rows.isErr()) return err(rows.error)
    const hit = rows.value.find(r => r.provider_id === parsed.providerId)
    if (hit === undefined) {
      return err(`provider not found: ${parsed.providerId}`)
    }
    const cacheKey = `${tenant}\n${hit.provider_id}/${parsed.modelId}#${capability}`
    const cached = this.genCache.get(cacheKey)
    if (cached !== undefined) return ok(cached)
    const creds: ProviderCredentials = {
      providerId: hit.provider_id,
      apiType: hit.api_type,
      baseUrl: hit.base_url,
      apiKey: hit.api_key,
      headers: parseHeaders(hit.headers),
    }
    const built = buildGenerativeModel(creds, parsed.modelId, capability)
    if (built.isErr()) {
      logger.warn(
        { provider: hit.provider_id, capability, err: built.error },
        'generative provider unusable',
      )
      return err(built.error)
    }
    const resolved: ResolvedGenerativeModel = {
      model: built.value,
      modelId: parsed.modelId,
      providerId: hit.provider_id,
      apiType: hit.api_type,
      capability,
    }
    this.genCache.set(cacheKey, resolved)
    return ok(resolved)
  }
}

function parseHeaders(raw: string): Record<string, string> {
  return parse(HeadersSchema, raw).match(
    headers => headers,
    () => ({}),
  )
}
