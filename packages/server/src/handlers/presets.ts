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
  type ChainMessage,
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  Config,
  catalogModel,
  clearActiveRun,
  compactSession,
  DEFAULT_PRESET,
  deleteSessionIds,
  discoverTools,
  factFromPersist,
  fileByCode,
  findVariant,
  fireAndForget,
  GATEWAY_API_TYPE,
  getModelsDev,
  interruptRun,
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
  TextPartDataSchema,
  toModelVariant,
  toolConfigMap,
  validateApiType,
  variantsForApiType,
  writeMessageFact,
} from '@abcp-agent/agent'
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
} from '@abcp-agent/schema'
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
 * Preset handlers: list/upsert/delete/preview.
 */

export function presetsHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async listPresets(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const r = await Presets.list(deps.bus, tenant)
      if (r.isErr()) throw new Error(r.error)
      // Locale chain: request → config KV → "en".
      const configLocale = (
        await Config.get(deps.bus, tenant, 'locale')
      ).unwrapOr(null)
      const locale = resolveLocale(req.locale, configLocale, 'en')
      return { presets: r.value.map(p => presetToMsg(p, locale)) }
    },

    async upsertPreset(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const p = req.preset
      if (!p) throw new Error('preset required')
      const r = await Presets.upsert(deps.bus, tenant, {
        id: p.id,
        systemPrompt: p.systemPrompt ?? '',
        systemPromptI18n: p.systemPromptI18n ?? '{}',
        tools: JSON.stringify(p.tools ?? []),
        maxTurns: p.maxTurns ?? 0,
      })
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },

    async deletePreset(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const id = req.id
      const r = await Presets.delete(deps.bus, tenant, id)
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },

    async previewPreset(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const id = req.id
      const r = await Presets.get(deps.bus, tenant, id)
      if (r.isErr()) throw new Error(r.error)
      if (r.value === null) throw new Error('preset not found')
      const i18n = r.value.system_prompt_i18n
      const template =
        i18n !== undefined && i18n !== '' && i18n !== '{}'
          ? i18n
          : r.value.system_prompt
      const rendered = await renderTemplate(template, deps.bus, tenant)
      return { template, rendered }
    },
  }
}
