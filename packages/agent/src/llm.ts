import { createAnthropic } from '@ai-sdk/anthropic'
import { createDeepSeek } from '@ai-sdk/deepseek'
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

export interface ProviderCredentials {
  apiType: string
  baseUrl: string
  apiKey: string
  headers: Record<string, string>
}

/**
 * What a provider model generates. `text` (default) = chat/vision language
 * model feeding sessions; `image` / `video` / `speech` / `transcription` =
 * generation models resolved by tools (image-generate / video-generate /
 * tts-generate / audio-transcribe) through the SAME provider registry — one
 * endpoint, one key, capability-tagged.
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
])

export function validateApiType(apiType: string): Result<void, string> {
  return KNOWN_API_TYPES.has(apiType.toLowerCase())
    ? ok(undefined)
    : err(
        `unknown api type: ${apiType} (expected anthropic|openai|openai-compatible|deepseek|google)`,
      )
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
    default:
      return err(`unknown api type: ${apiType}`)
  }
}

/**
 * Generation-model factory for the non-text capabilities. Returns the
 * capability-specific AI-SDK model object (ImageModelV4 /
 * Experimental_VideoModelV4 / SpeechModelV4) — callers drive it with
 * `generateImage` / `experimental_generateVideo` / `generateSpeech`.
 *
 * api-type support matrix:
 *   - image:         openai, google, openai-compatible
 *   - video:         google (AI SDK video is experimental; openai has none)
 *   - speech:        openai, google
 *   - transcription: openai (OpenAI-compatible /audio/transcriptions endpoints
 *                    work through it — the AI SDK posts multipart to
 *                    `<base>/audio/transcriptions`, which the easylab gateway
 *                    and most OpenAI-compatible servers implement)
 */
export function buildGenerativeModel(
  credentials: ProviderCredentials,
  modelId: string,
  capability: Exclude<ModelCapability, 'text'>,
): Result<unknown, string> {
  const apiType = credentials.apiType.toLowerCase()
  const { baseUrl, apiKey, headers } = credentials
  const baseURL = baseUrl ? { baseURL: baseUrl } : {}

  switch (apiType) {
    case 'openai': {
      const p = createOpenAI({ ...baseURL, apiKey, headers })
      if (capability === 'image') return ok(p.imageModel(modelId))
      if (capability === 'speech') return ok(p.speech(modelId))
      if (capability === 'transcription')
        return ok(p.transcription(modelId))
      return err(`api type 'openai' does not support ${capability} generation`)
    }
    case 'google':
    case 'gemini': {
      const p = createGoogle({ ...baseURL, apiKey, headers })
      if (capability === 'image') return ok(p.image(modelId))
      if (capability === 'video') return ok(p.videoModel(modelId))
      if (capability === 'speech') return ok(p.speechModel(modelId))
      return err(`api type 'google' does not support ${capability} generation`)
    }
    case 'openai-compatible':
    case 'openai_compatible': {
      // The openai provider hits the same OpenAI wire format but implements
      // the full surface (transcription included); route openai-compatible
      // transcription requests through it so any OpenAI-shaped ASR endpoint
      // (e.g. the easylab gateway) resolves via provider/model refs.
      if (capability === 'transcription') {
        const p = createOpenAI({ ...baseURL, apiKey, headers })
        return ok(p.transcription(modelId))
      }
      const p = createOpenAICompatible({
        name: 'openai-compatible',
        baseURL: baseUrl,
        apiKey,
        headers,
      })
      if (capability === 'image') return ok(p.imageModel(modelId))
      return err(
        `api type 'openai-compatible' does not support ${capability} generation (use an openai or google provider)`,
      )
    }
    case 'anthropic':
    case 'claude':
    case 'deepseek':
      return err(
        `api type '${apiType}' does not support ${capability} generation (use an openai or google provider)`,
      )
    default:
      return err(`unknown api type: ${apiType}`)
  }
}

export interface ResolvedModel {
  model: LanguageModel
  modelId: string
  providerId: string
  apiType: string
}

/** A resolved generation model (capability-tagged, not a LanguageModel). */
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
 * model id. If no registered provider matches, the env-configured bootstrap
 * provider is used (when one is set).
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
   * reference is empty or malformed — a session/tool must select a model from
   * a registered provider. The referenced provider must exist.
   */
  async resolve(db: Db, ref: string): Promise<Result<ResolvedModel, string>> {
    const parsed = parseProviderModelRef(ref)
    if (parsed === null) {
      return err(
        `no model selected — set a model as "provider_id/model_id" (got ${JSON.stringify(ref)})`,
      )
    }
    return this.resolveByProvider(db, parsed.providerId, parsed.modelId)
  }

  /**
   * Resolve an explicit `provider_id/model_id` reference from a registered
   * provider. The referenced provider must exist; no fallback is applied
   * (a tool/session that asks for a specific model wants that model or an
   * error).
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
    const cacheKey = `${hit.provider_id}/${modelId}`
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
   * Resolve a generation model (image / video / speech) from the SAME
   * provider registry. `ref` is a canonical `provider_id/model_id`; the
   * referenced provider must exist and the provider's api type must support
   * the capability. No fallback: a tool asking for a specific model gets that
   * model or an error.
   */
  async resolveGenerative(
    db: Db,
    ref: string,
    capability: Exclude<ModelCapability, 'text'>,
  ): Promise<Result<ResolvedGenerativeModel, string>> {
    const parsed = parseProviderModelRef(ref)
    if (parsed === null) {
      return err(
        `no model selected — set a ${capability} model as "provider_id/model_id" (got ${JSON.stringify(ref)})`,
      )
    }
    const rows = await Providers.list(db)
    if (rows.isErr()) return err(rows.error)
    const hit = rows.value.find(r => r.provider_id === parsed.providerId)
    if (hit === undefined) {
      return err(`provider not found: ${parsed.providerId}`)
    }
    const cacheKey = `${hit.provider_id}/${parsed.modelId}#${capability}`
    const cached = this.genCache.get(cacheKey)
    if (cached !== undefined) return ok(cached)
    const creds: ProviderCredentials = {
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
