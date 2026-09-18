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
  logger,
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
 * Config + tool/extension config handlers and getAgentConfig.
 */

export function configHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async getConfig(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const key = req.key
      const r = await Config.get(deps.bus, tenant, key)
      return { key, value: r.isOk() && r.value ? r.value : '' }
    },

    async setConfig(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { key, value } = req
      const r = await Config.set(deps.bus, tenant, key, value)
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },

    async listTools(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const tools = await discoverTools(deps.bus)
      const configLocale = (
        await Config.get(deps.bus, tenant, 'locale')
      ).unwrapOr(null)
      const locale = resolveLocale(req.locale, configLocale, 'en')
      return create(ListToolsResponseSchema, {
        tools: tools.map(t => ({
          name: t.name,
          description: pickDescription(t.description, t.descriptions, locale),
          category: t.extId,
          parameters: toJsonObject(localizeSchema(t.inputSchema, locale) ?? {}),
          configFields: (t.extConfig ?? []).map(c => ({
            name: c.name,
            type: c.type,
            kind: c.kind ?? 'value',
            capability: c.capability ?? '',
            enumValues: c.enum_values ?? [],
            description: pickDescription(
              c.description ?? '',
              c.descriptions,
              locale,
            ),
            scope: c.scope ?? 'global',
          })),
          requiredConfig: t.requiredConfig ?? [],
        })),
      })
    },

    async getToolConfig(_req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const value = await toolConfigMap(deps.bus, tenant)
      // Response.config is a ToolConfig whose `values` is a
      // map<string, google.protobuf.Value>: each tool maps to a single
      // Value that is a Struct of its declared knobs. Wrap the whole per-tool
      // knob map into one Struct Value.
      const values: Record<string, Value> = {}
      for (const [toolName, cfg] of Object.entries(value)) {
        values[toolName] = toValue(cfg)
      }
      return { config: { values } }
    },

    async setToolConfig(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      // Request.config is a google.protobuf.Struct — already a plain JSON
      // object on the wire, so serialize it directly.
      const r = await Config.set(
        deps.bus,
        tenant,
        'tool_config',
        JSON.stringify(req.config ?? {}),
      )
      if (r.isErr()) throw new Error(r.error)
      return { ok: true }
    },

    async setExtensionConfig(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { extId, name, value } = req
      // The LONG-LIVED agent owns the manifest cache + config authority
      // (`serveConfig()` runs once at boot). A per-request AbcAgent would have
      // an empty cache and an unstarted authority, so the write could never be
      // applied (it surfaced as an opaque internal error).
      const agent = deps.agent ?? new AbcAgent(deps.bus)
      await agent.discover(500)
      // Request.value is a google.protobuf.Value message; unwrap it with the
      // canonical toJson() mapping into the raw value the config store needs.
      const v = valueToRaw(value)
      if (deps.agent === undefined) {
        // Defensive fallback for embedders that did not wire the agent role.
        await agent.serveConfig()
      }
      try {
        await agent.setConfig(tenant, extId, name, v)
      } catch (e) {
        // The SDK throws ConfigError ({code,message}); surface the REAL reason
        // instead of an opaque `internal error` (a bad/missing declaration, a
        // session-scoped knob written without a session, a rejected value…).
        const err = e as { code?: string; message?: string }
        logger.warn(
          { extId, name, code: err?.code, err: err?.message },
          'setExtensionConfig failed',
        )
        throw new ConnectError(
          `setExtensionConfig ${extId}.${name}: ${err?.message ?? String(e)}`,
          err?.code === 'invalid_argument'
            ? Code.InvalidArgument
            : err?.code === 'not_found'
              ? Code.NotFound
              : Code.Internal,
        )
      }
      return { ok: true }
    },

    async getAgentConfig(_req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const r = await Providers.list(deps.db, tenant)
      if (r.isErr()) throw new Error(r.error)
      const providers: Record<string, string> = {}
      for (const p of r.value) {
        if (p) {
          providers[p.provider_id] = JSON.stringify(
            providerToMsg(p),
            (_k, v) => (typeof v === 'bigint' ? v.toString() : v),
          )
        }
      }
      return create(GetAgentConfigResponseSchema, {
        config: { providers },
      })
    },
  }
}
