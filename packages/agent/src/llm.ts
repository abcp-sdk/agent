import { createAnthropic } from '@ai-sdk/anthropic'
import { createCohere } from '@ai-sdk/cohere'
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
 * The api type that fronts a Vercel-AI-SDK-compatible gateway (`/v4/ai`).
 * A gateway can serve EVERY capability (text + all six non-text kinds), but
 * it is no longer special-cased: any provider api type may register any of
 * the capabilities its protocol supports (see CAPABILITY_MATRIX).
 */
export const GATEWAY_API_TYPE = 'vercel-compatible-gateway'

/**
 * Historical default id for a gateway provider. Purely conventional now —
 * gateway providers may use ANY id and there may be several of them (e.g.
 * two gateways with different keys).
 */
export const GATEWAY_PROVIDER_ID = 'gateway'

/** The Cohere protocol api type (rerank; text via the cohere chat API). */
export const COHERE_API_TYPE = 'cohere'

export interface ProviderCredentials {
  providerId: string
  apiType: string
  baseUrl: string
  apiKey: string
  headers: Record<string, string>
}

/**
 * A model's capability. `text` models drive chat turns; the non-text kinds
 * are registered per model (`model_type`) and resolved by the tools / the
 * provider test probes.
 */
export type ModelCapability =
  | 'text'
  | 'image'
  | 'video'
  | 'speech'
  | 'transcription'
  | 'embedding'
  | 'rerank'

export const MODEL_CAPABILITIES: readonly ModelCapability[] = [
  'text',
  'image',
  'video',
  'speech',
  'transcription',
  'embedding',
  'rerank',
]

/** Normalize a client-supplied capability string; empty = text. */
export function parseCapability(raw: string): Result<ModelCapability, string> {
  const v = raw.trim().toLowerCase()
  if (v === '') return ok('text')
  const hit = MODEL_CAPABILITIES.find(c => c === v)
  return hit !== undefined
    ? ok(hit)
    : err(
        `unknown capability: ${raw} (expected ${MODEL_CAPABILITIES.join('|')})`,
      )
}

/**
 * Which capabilities each provider api type can serve. This is the SINGLE
 * source of truth for registration validation, model resolution, and the
 * generative-model factory — the gateway has no special-cased role anymore.
 *
 *   - openai / openai-compatible: the OpenAI protocol surfaces
 *     (embeddings / images / audio-speech / audio-transcriptions) — but NO
 *     video (no standard endpoint) and NO rerank (not an OpenAI API).
 *   - vercel-compatible-gateway: every capability, via /v4/ai.
 *   - cohere: rerank ({baseURL}/v1/rerank) + text.
 *   - anthropic / deepseek / google: text only.
 */
export const CAPABILITY_MATRIX: Record<string, ReadonlySet<ModelCapability>> = {
  openai: new Set(['text', 'embedding', 'image', 'speech', 'transcription']),
  'openai-compatible': new Set([
    'text',
    'embedding',
    'image',
    'speech',
    'transcription',
  ]),
  openai_compatible: new Set([
    'text',
    'embedding',
    'image',
    'speech',
    'transcription',
  ]),
  [GATEWAY_API_TYPE]: new Set(MODEL_CAPABILITIES),
  [COHERE_API_TYPE]: new Set(['text', 'rerank']),
  anthropic: new Set(['text']),
  claude: new Set(['text']),
  deepseek: new Set(['text']),
  google: new Set(['text']),
  gemini: new Set(['text']),
}

/**
 * Canonical (user-facing) api type ids — the historical aliases
 * (openai_compatible / claude / gemini) still VALIDATE but are hidden from
 * the served catalog so client pickers list one entry per protocol.
 */
export const CANONICAL_API_TYPES: readonly string[] = [
  'openai-compatible',
  'openai',
  'anthropic',
  'deepseek',
  'google',
  GATEWAY_API_TYPE,
  COHERE_API_TYPE,
]

/** The capabilities an api type may serve (empty set for unknown types). */
export function capabilitiesOf(apiType: string): ReadonlySet<ModelCapability> {
  return CAPABILITY_MATRIX[apiType.toLowerCase()] ?? new Set()
}

/** True when [apiType] may serve [capability]. */
export function supportsCapability(
  apiType: string,
  capability: ModelCapability,
): boolean {
  return capabilitiesOf(apiType).has(capability)
}

const KNOWN_API_TYPES = new Set(Object.keys(CAPABILITY_MATRIX))

export function validateApiType(apiType: string): Result<void, string> {
  return KNOWN_API_TYPES.has(apiType.toLowerCase())
    ? ok(undefined)
    : err(
        `unknown api type: ${apiType} (expected anthropic|openai|openai-compatible|deepseek|google|${GATEWAY_API_TYPE}|${COHERE_API_TYPE})`,
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
    case COHERE_API_TYPE:
      return ok(
        createCohere({ baseURL: baseUrl, apiKey, headers }).languageModel(
          modelId,
        ),
      )
    default:
      return err(`unknown api type: ${apiType}`)
  }
}

/**
 * Non-text model factory, dispatched by (apiType x capability) per
 * CAPABILITY_MATRIX:
 *
 *   - openai / openai-compatible → the OpenAI protocol surfaces via
 *     `createOpenAI({baseURL, apiKey, name})` (embeddings / images /
 *     audio-speech / audio-transcriptions multipart).
 *   - vercel-compatible-gateway → the /v4/ai gateway factories (all kinds).
 *   - cohere → rerank via `{baseURL}/v1/rerank`.
 *
 * Video has no OpenAI-protocol endpoint, so it remains gateway-only (the
 * matrix rejects the combination at registration; this is the backstop).
 */
export function buildGenerativeModel(
  credentials: ProviderCredentials,
  modelId: string,
  capability: Exclude<ModelCapability, 'text'>,
): Result<unknown, string> {
  const { providerId, apiType, baseUrl, apiKey, headers } = credentials
  if (!supportsCapability(apiType, capability)) {
    return err(
      `provider '${providerId}' (${apiType}) cannot serve capability '${capability}'`,
    )
  }
  switch (apiType.toLowerCase()) {
    case 'openai':
    case 'openai-compatible':
    case 'openai_compatible': {
      // The openai package honors a custom baseURL, so it doubles as the
      // factory for ANY OpenAI-protocol endpoint (the openai-compatible
      // package lacks speech/transcription factories).
      const oai = createOpenAI({ baseURL: baseUrl, apiKey, headers })
      switch (capability) {
        case 'embedding':
          return ok(oai.embeddingModel(modelId))
        case 'image':
          return ok(oai.imageModel(modelId))
        case 'speech':
          return ok(oai.speech(modelId))
        case 'transcription':
          return ok(oai.transcription(modelId))
        default:
          return err(
            `capability '${capability}' has no OpenAI-protocol endpoint (video/rerank are gateway/cohere only)`,
          )
      }
    }
    case GATEWAY_API_TYPE: {
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
        case 'embedding':
          return ok(gw.embeddingModel(modelId))
        case 'rerank':
          return ok(gw.rerankingModel(modelId))
        default:
          return err(`unsupported gateway capability: ${capability}`)
      }
    }
    case COHERE_API_TYPE: {
      if (capability !== 'rerank') {
        return err(`cohere providers serve only rerank (got '${capability}')`)
      }
      const cohere = createCohere({ baseURL: baseUrl, apiKey, headers })
      return ok(cohere.rerankingModel(modelId))
    }
    default:
      return err(
        `provider '${providerId}' (${apiType}) cannot serve capability '${capability}'`,
      )
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
    // The model's DECLARED capability must match what the caller asks for:
    // pointing image_model at a text model (same provider) is a config error.
    // LEGACY rows registered before capabilities existed store an EMPTY
    // model_type — those skip the strict match (the api-type matrix still
    // applies); every row registered since carries an explicit type.
    const declared = declaredModelType(hit.models, parsed.modelId)
    if (declared.isErr()) return err(declared.error)
    if (declared.value.explicit && declared.value.capability !== capability) {
      return err(
        `model '${parsed.modelId}' is registered as '${declared.value.capability}', not '${capability}'`,
      )
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

/** Shape of a stored provider-model row (the JSON `models` column). */
interface StoredModel {
  id?: unknown
  model_type?: unknown
}

/** The declared capability of one model on a provider; errors when absent.
 * `explicit=false` marks a LEGACY row (empty stored model_type). */
function declaredModelType(
  modelsJson: unknown,
  modelId: string,
): Result<{ capability: ModelCapability; explicit: boolean }, string> {
  let list: StoredModel[] = []
  try {
    const raw = JSON.parse(String(modelsJson ?? '[]')) as unknown
    if (Array.isArray(raw)) list = raw as StoredModel[]
  } catch {
    return err('provider model list is malformed')
  }
  const hit = list.find(m => String(m?.id ?? '') === modelId)
  if (hit === undefined) {
    return err(`model '${modelId}' is not registered on this provider`)
  }
  const rawType = String(hit.model_type ?? '').trim()
  if (rawType === '') return ok({ capability: 'text', explicit: false })
  const cap = parseCapability(rawType)
  if (cap.isErr()) return err(cap.error)
  return ok({ capability: cap.value, explicit: true })
}

function parseHeaders(raw: string): Record<string, string> {
  return parse(HeadersSchema, raw).match(
    headers => headers,
    () => ({}),
  )
}
