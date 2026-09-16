import { Agent as AbcAgent, isSessionRunning } from '@abc-protocol/sdk'
import type { JsonObject, JsonValue } from '@bufbuild/protobuf'
import { create, fromJson, toJson } from '@bufbuild/protobuf'
import { StructSchema, type Value, ValueSchema } from '@bufbuild/protobuf/wkt'
import {
  Code,
  ConnectError,
  type ConnectRouter,
  type HandlerContext,
  type ServiceImpl,
} from '@connectrpc/connect'
import {
  type AgentDeps,
  appendSessionId,
  BUCKET_SESSION_STATE,
  CAPABILITY_MATRIX,
  type ChainMessage,
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  Config,
  catalogModel,
  clearActiveRun,
  compactSession,
  DEFAULT_PRESET,
  deleteSessionIds,
  discoverGatewayModels,
  discoverTools,
  factFromPersist,
  fileByCode,
  findVariant,
  fireAndForget,
  GATEWAY_API_TYPE,
  GATEWAY_PROVIDER_ID,
  getModelsDev,
  interruptRun,
  isGatewayApiType,
  localizeSchema,
  Mailbox,
  Messages,
  mailboxSubject,
  maskSecret,
  Parts,
  Presets,
  Providers,
  parse,
  parseCapability,
  parseProviderModelRef,
  pickDescription,
  pickLocalized,
  projectMessageFact,
  publishLifecycle,
  publishSessionChanged,
  pushChainChanged,
  readActiveRun,
  readMessageFacts,
  renderTemplate,
  resolveLocale,
  Sessions,
  supportsCapability,
  TextPartDataSchema,
  toModelVariant,
  toolConfigMap,
  validateApiType,
  variantsForApiType,
  writeMessageFact,
} from '@easylab-agent/agent'
import {
  type AgentService,
  GetAgentConfigResponseSchema,
  GetFileResponseSchema,
  IngestFileResponseSchema,
  ListToolsResponseSchema,
  type WatchSessionResponse,
  WatchSessionResponseSchema,
  type WatchSessionsResponse,
  WatchSessionsResponseSchema,
} from '@easylab-agent/schema'
import { EidDedup } from '../context.js'
import {
  fieldString,
  isRecord,
  toJsonObject,
  toJsonValue,
  toStructFields,
  toStructValue,
  toValue,
  valueToRaw,
} from '../proto-json.js'
import { runProviderTest } from '../provider-test.js'
import { resolveSessionDefaults } from '../session-defaults.js'
import { tenantOf } from '../tenant.js'
import {
  parseProviderModels,
  presetToMsg,
  providerToMsg,
  sessionToMsg,
} from '../views.js'
import { refreshMessageFactFromTip, storeBytes } from './helpers.js'

/**
 * Provider registry handlers: list/register/delete, gateway discovery, test probes, listModels.
 */

export function providersHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async listProviders(_req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const r = await Providers.list(deps.db, tenant)
      if (r.isErr()) throw new Error(r.error)
      return { providers: r.value.map(providerToMsg) }
    },

    async listProvidersCatalog(_req, ctx: HandlerContext) {
      // Auth-gated like every tenant RPC (the matrix itself is not tenant
      // scoped). Serve the capability matrix: the single source of truth
      // client registration forms consume (api type -> its capabilities).
      tenantOf(ctx)
      const apiTypes: Record<string, { capabilities: string[] }> = {}
      for (const [apiType, caps] of Object.entries(CAPABILITY_MATRIX)) {
        apiTypes[apiType] = { capabilities: [...caps] }
      }
      return { apiTypes }
    },

    async registerProvider(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const p = req.provider
      if (!p) throw new Error('provider required')
      const apiType = p.apiType
      const validType = validateApiType(apiType)
      if (validType.isErr()) {
        throw new ConnectError(validType.error, Code.InvalidArgument)
      }
      // No gateway special-casing anymore: ANY api type registers like any
      // other provider (several providers of one api type allowed, each with
      // models of any capability its protocol serves — see CAPABILITY_MATRIX).
      // A model's `model_type` IS its declared capability:
      //   text  -> context_limit required (> 0)
      //   other -> context_limit must be 0 (not a chat model)
      const models: {
        id: string
        name: string
        context_limit: number
        model_type: string
      }[] = []
      for (const m of p.models ?? []) {
        if (m.id === '') {
          throw new ConnectError('model id is required', Code.InvalidArgument)
        }
        const cap = parseCapability(m.modelType ?? '')
        if (cap.isErr()) {
          throw new ConnectError(
            `model '${m.id}': ${cap.error}`,
            Code.InvalidArgument,
          )
        }
        if (!supportsCapability(apiType, cap.value)) {
          throw new ConnectError(
            `model '${m.id}': api type '${apiType}' cannot serve capability '${cap.value}'`,
            Code.InvalidArgument,
          )
        }
        if (cap.value === 'text' && m.contextLimit <= 0n) {
          throw new ConnectError(
            `model '${m.id}': context_limit is required and must be > 0 for text models`,
            Code.InvalidArgument,
          )
        }
        if (cap.value !== 'text' && m.contextLimit > 0n) {
          throw new ConnectError(
            `model '${m.id}': context_limit must be 0 for '${cap.value}' models`,
            Code.InvalidArgument,
          )
        }
        models.push({
          id: m.id,
          name: m.name !== '' ? m.name : m.id,
          context_limit: Number(m.contextLimit),
          model_type: cap.value,
        })
      }
      // Edit-time sentinel: a client that LISTED providers prefills the form
      // with the MASKED key; saving untouched round-trips the mask verbatim.
      // Treat that as "unchanged" and keep the stored secret — only a
      // genuinely different (user-typed) key overwrites it.
      let apiKey = p.apiKey ?? ''
      const existingRows = await Providers.list(deps.db, tenant)
      if (existingRows.isErr()) throw new Error(existingRows.error)
      const existing = existingRows.value.find(
        r => r.provider_id === p.providerId,
      )
      if (
        existing !== undefined &&
        apiKey !== '' &&
        apiKey === maskSecret(existing.api_key)
      ) {
        apiKey = existing.api_key
      }
      const r = await Providers.upsert(deps.db, tenant, {
        providerId: p.providerId,
        apiType,
        baseUrl: p.baseUrl,
        apiKey,
        headers: p.headers ?? {},
        models,
      })
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },

    async discoverGatewayModels(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      // Ask the gateway's /config for its models and classify by modelType.
      // The gateway is the only multimodal provider, so a non-gateway type
      // is rejected here.
      const creds = {
        providerId:
          req.providerId !== '' ? req.providerId : GATEWAY_PROVIDER_ID,
        apiType: req.apiType,
        baseUrl: req.baseUrl,
        apiKey: req.apiKey ?? '',
        headers: req.headers ?? {},
      }
      const r = await discoverGatewayModels(creds)
      if (r.isErr()) return { ok: false, error: r.error, models: [] }
      return {
        ok: true,
        error: '',
        models: r.value.map(m => ({
          id: m.id,
          name: m.name,
          contextLimit: BigInt(m.contextLimit),
          modelType: m.modelType,
        })),
      }
    },

    async deleteProvider(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const id = req.providerId
      const r = await Providers.delete(deps.db, tenant, id)
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },

    async testProvider(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const r = req
      // ONLY a real (smallest-possible) generation proves a model is usable.
      // No /models fallback. An empty model is an error (nothing to test).
      if (r.model === undefined || r.model === '') {
        return { ok: false, result: 'model is required to test' }
      }
      const cap = parseCapability(r.capability)
      if (cap.isErr()) {
        return { ok: false, result: cap.error }
      }
      // `model` accepts a canonical provider/model ref or a bare id; use the
      // trailing model id for the model factory.
      const ref = parseProviderModelRef(r.model)
      const modelId = ref !== null ? ref.modelId : r.model
      const providerId =
        r.providerId !== '' ? r.providerId : (ref?.providerId ?? '')
      // Credential resolution: an EMPTY key or the edit-time MASK means
      // "test what is registered" — fall back to the stored secret. A
      // genuinely different key tests the unregistered credentials as-is.
      let apiKey = r.apiKey ?? ''
      if (apiKey === '' || (providerId !== '' && apiKey.includes('****'))) {
        const rows = await Providers.list(deps.db, tenant)
        if (rows.isOk()) {
          const row = rows.value.find(x => x.provider_id === providerId)
          if (row !== undefined) apiKey = row.api_key
        }
      }
      // Text models may carry a reasoning variant; resolve it to the
      // providerOptions the test generation should exercise.
      let textProviderOptions: Record<string, unknown> | undefined
      if (cap.value === 'text') {
        const meta = catalogModel(
          await getModelsDev(deps.bus),
          providerId,
          modelId,
        )
        const variantDef =
          meta === null ? null : findVariant(meta, r.apiType, r.variant ?? '')
        if (
          variantDef !== null &&
          Object.keys(variantDef.providerOptions).length > 0
        ) {
          textProviderOptions = variantDef.providerOptions
        }
      }
      try {
        return await runProviderTest(
          {
            apiType: r.apiType,
            baseUrl: r.baseUrl,
            apiKey,
            modelId,
            capability: cap.value,
            providerId,
            variant: r.variant ?? '',
          },
          textProviderOptions,
        )
      } catch (e) {
        return { ok: false, result: `provider test failed: ${String(e)}` }
      }
    },

    async listModels(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const pid = req.providerId
      // provider_id is REQUIRED: a global (all-providers) model list is
      // rejected outright — it invites duplicate model ids across providers.
      if (pid === '') {
        throw new ConnectError('provider_id is required', Code.InvalidArgument)
      }
      const r = await Providers.list(deps.db, tenant)
      if (r.isErr()) throw new Error(r.error)
      const catalog = await getModelsDev(deps.bus)
      const provider = r.value.find(p => p?.provider_id === pid)
      const apiType = provider?.api_type ?? ''
      const parsed =
        provider === undefined ? [] : parseProviderModels(provider.models)
      // Session model listing surfaces TEXT models only. For a text provider
      // every model is text; for the gateway (superset) only models with a
      // positive context_limit are text (context_limit 0 = multimodal).
      const models = parsed
        .filter(m => m.contextLimit > 0n)
        .map(m => {
          const meta = catalogModel(catalog, pid, m.id)
          const variants =
            meta === null
              ? []
              : variantsForApiType(meta, apiType).map(toModelVariant)
          return {
            id: m.id,
            name: m.name,
            variants,
            contextLimit: m.contextLimit,
          }
        })
      return { models }
    },
  }
}
