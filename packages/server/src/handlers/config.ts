import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  type AgentDeps,
  Config,
  discoverTools,
  localizeSchema,
  logger,
  Providers,
  pickDescription,
  resolveLocale,
  toolConfigMap,
} from '@abcp-agent/agent'
import {
  type AgentService,
  GetAgentConfigResponseSchema,
  ListToolsResponseSchema,
} from '@abcp-agent/schema'
import { create } from '@bufbuild/protobuf'
import type { Value } from '@bufbuild/protobuf/wkt'
import {
  Code,
  ConnectError,
  type HandlerContext,
  type ServiceImpl,
} from '@connectrpc/connect'
import { toJsonObject, toValue, valueToRaw } from '../proto-json.js'
import { tenantOf } from '../tenant.js'
import { providerToMsg } from '../views.js'

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
