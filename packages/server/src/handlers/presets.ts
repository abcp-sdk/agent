import {
  type AgentDeps,
  Config,
  Presets,
  renderTemplate,
  resolveLocale,
} from '@abcp-agent/agent'
import type { AgentService } from '@abcp-agent/schema'
import type { HandlerContext, ServiceImpl } from '@connectrpc/connect'
import { tenantOf } from '../tenant.js'
import { presetToMsg } from '../views.js'

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
