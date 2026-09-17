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
 *
 * It is ONE protocol among many — there is no gateway special case, no fixed
 * provider id and no discovery RPC. It simply happens to serve every modality,
 * exactly like any protocol may serve the modalities its wire format supports
 * (see CAPABILITY_MATRIX).
 */
export const GATEWAY_API_TYPE = 'vercel-compatible-gateway'

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
 * A model's capability (modality). `text` models drive chat turns; every other
 * kind is registered per provider (semantic grouping) and resolved by the tools
 * / the provider test probes. `realtime` is a first-class modality with no
 * consumer yet, but a real factory (OpenAI + gateway protocols).
 */
export type ModelCapability =
  | 'text'
  | 'image'
  | 'video'
  | 'speech'
  | 'transcription'
  | 'embedding'
  | 'rerank'
  | 'realtime'

export const MODEL_CAPABILITIES: readonly ModelCapability[] = [
  'text',
  'image',
  'video',
  'speech',
  'transcription',
  'embedding',
  'rerank',
  'realtime',
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
 * Which capabilities each provider protocol can serve. This is the SINGLE
 * source of truth for registration validation, model resolution, and the model
 * factories. A modality is registered under exactly one protocol per provider
 * (semantic grouping); the protocol list for a modality is the inverse of this
 * table.
 *
 *   - openai / openai-compatible: the OpenAI protocol surfaces — text,
 *     embeddings, images, audio speech, audio transcriptions and realtime.
 *     NO video (no standard endpoint) and NO rerank.
 *   - vercel-compatible-gateway: every modality, via /v4/ai.
 *   - cohere: rerank ({baseURL}/v1/rerank) + text.
 *   - anthropic / deepseek / google: text only.
 */
export const CAPABILITY_MATRIX: Record<string, ReadonlySet<ModelCapability>> = {
  openai: new Set([
    'text',
    'embedding',
    'image',
    'speech',
    'transcription',
    'realtime',
  ]),
  'openai-compatible': new Set([
    'text',
    'embedding',
    'image',
    'speech',
    'transcription',
    'realtime',
  ]),
  openai_compatible: new Set([
    'text',
    'embedding',
    'image',
    'speech',
    'transcription',
    'realtime',
  ]),
  [GATEWAY_API_TYPE]: new Set(MODEL_CAPABILITIES),
  [COHERE_API_TYPE]: new Set(['text', 'rerank']),
  anthropic: new Set(['text']),
  claude: new Set(['text']),
  deepseek: new Set(['text']),
  google: new Set(['text']),
  gemini: new Set(['text']),
}

/** The protocols (canonical api types) that can serve [capability]. */
export function protocolsForCapability(capability: ModelCapability): string[] {
  return CANONICAL_API_TYPES.filter(apiType =>
    supportsCapability(apiType, capability),
  )
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
 * Non-text model factory, dispatched by (protocol x capability) per
 * CAPABILITY_MATRIX:
 *
 *   - openai / openai-compatible → the OpenAI protocol surfaces via
 *     `createOpenAI({baseURL, apiKey, name})` (embeddings / images /
 *     audio-speech / audio-transcriptions / realtime).
 *   - vercel-compatible-gateway → the /v4/ai gateway factories (all modalities).
 *   - cohere → rerank via `{baseURL}/v1/rerank`.
 *
 * Video has no OpenAI-protocol endpoint, so only the gateway protocol serves it
 * (the matrix rejects the combination at registration; this is the backstop).
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
      // package lacks speech/transcription/realtime factories).
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
        case 'realtime':
          return ok(oai.experimental_realtime(modelId))
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
        case 'realtime':
          return ok(gw.experimental_realtime(modelId))
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
    // Semantic grouping: only a TEXT provider can drive chat turns.
    if ((hit.capability ?? 'text') !== 'text') {
      return err(
        `provider '${providerId}' serves '${hit.capability ?? 'text'}', not 'text'`,
      )
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
   * Resolve a non-text model (image / video / speech / transcription /
   * embedding / rerank / realtime). A tool / knob asking for a specific
   * modality gets that model or an error — never a silent fallback.
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
    // Semantic grouping: the provider serves exactly ONE modality, and the
    // reference must name a model of it. Pointing an image knob at a text
    // provider (or vice versa) is a config error.
    if ((hit.capability ?? 'text') !== capability) {
      return err(
        `provider '${parsed.providerId}' serves '${hit.capability ?? 'text'}', not '${capability}'`,
      )
    }
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
