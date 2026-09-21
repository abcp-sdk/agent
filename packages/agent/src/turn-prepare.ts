import { setSessionVariable } from '@abc-protocol/sdk'
import type { PresetRow } from '@abcp-agent/schema'
import type { Tool } from 'ai'
import { z } from 'zod'
import { Sessions } from './db-sessions.js'
import { renderTemplate } from './extensions.js'
import { buildEnvBlock, pickLocalized, resolveLocale } from './i18n.js'
import { parse } from './json.js'
import { Config, Presets } from './kv-store.js'
import { logger } from './logger.js'
import type { AgentDeps } from './session-agent.js'
import { getModelsDev } from './store.js'
import {
  buildAiTools,
  discoverToolsCached,
  filterDeniedTools,
  toolQualifiedName,
  toolsBlockedByMissingRequired,
} from './tools.js'
import { catalogModel, findVariant, type JsonObject } from './variants.js'

/**
 * Fallback identity when neither the session nor its preset yields any system
 * prompt text. A preset with no prompt is valid (e.g. one that only whitelists
 * tools); the turn must still run, and the language directive always comes
 * from `<env>` regardless.
 */
export const DEFAULT_SYSTEM_PROMPT = 'You are a helpful assistant.'

export interface TurnCtx {
  tools: Record<string, Tool>
  system: string
  maxTurns: number
  model: import('ai').LanguageModel
  providerOptions:
    | Record<string, import('./variants.js').JsonObject>
    | undefined
  headers: Record<string, string> | undefined
}

export async function prepare(
  deps: AgentDeps,
  tenant: string,
  sid: string,
  abortSignal: AbortSignal,
): Promise<TurnCtx | string> {
  const sessionRes = await Sessions.get(deps.db, tenant, sid)
  if (sessionRes.isErr()) return sessionRes.error
  const session = sessionRes.value
  if (session === null) return 'session not found'

  const presetRow =
    session.preset !== ''
      ? (await Presets.get(deps.bus, tenant, session.preset)).unwrapOr(null)
      : null
  const presetTools = parse(z.array(z.string()), presetRow?.tools ?? '[]')
  const toolNames = presetTools.isOk() ? presetTools.value : []
  const whitelist = toolNames.length > 0 ? new Set(toolNames) : null

  // Effective locale: session → KV config (live) → env → "en". The KV read
  // makes `PUT /api/v1/config {key:"locale"}` take effect on the very next
  // turn (no restart), while per-session `PATCH /sessions/{id}/settings`
  // {locale} still wins. Used to localize tool descriptions and the system
  // prompt, and projected as vars.agent.locale.
  const configLocale = (await Config.get(deps.bus, tenant, 'locale')).unwrapOr(
    null,
  )
  const locale = resolveLocale(session.locale, configLocale, 'en')

  // Host hard-denylist (env DISABLED_TOOLS) runs FIRST: removing a colliding
  // tool before qualified names are computed lets the surviving same-named tool
  // fall back to its bare name, which is what preset whitelists reference. A
  // denylist entry may be a bare name or `<extId>.<name>`.
  const discovered = filterDeniedTools(
    await discoverToolsCached(deps.bus),
    deps.config.disabledTools,
  )
  const active =
    whitelist === null
      ? discovered
      : discovered.filter(t => whitelist.has(toolQualifiedName(discovered, t)))
  logger.info(
    {
      sid,
      tools: active.map(t => toolQualifiedName(discovered, t)),
      whitelisted: whitelist !== null,
    },
    'tools prepared for turn',
  )
  // Hard-disable tools whose required config is unset: the model must not call
  // a tool it cannot run. Reads the `cfg` bucket per turn (cheap, few knobs).
  const blocked = await toolsBlockedByMissingRequired(deps.bus, tenant, active)
  if (blocked.size > 0) {
    logger.info(
      { sid, blocked: [...blocked] },
      'tools blocked (required config unset)',
    )
  }
  const tools = buildAiTools(
    active,
    deps.bus,
    deps.config.toolTimeoutMs,
    tenant,
    sid,
    abortSignal,
    locale,
    blocked,
  )

  // Session-level settings override the preset. An EMPTY resolved prompt is
  // never an error: it falls back to the built-in default identity so a
  // preset that supplies only, say, a tools list still runs. (The language
  // directive is injected via `<env>` below and needs no preset text.)
  const presetPrompt =
    session.system_prompt !== ''
      ? session.system_prompt
      : presetRow !== null
        ? presetPromptFor(presetRow, locale)
        : ''
  const systemPrompt =
    presetPrompt.trim() !== '' ? presetPrompt : DEFAULT_SYSTEM_PROMPT
  // The `<env>` block always carries the language directive, so the model
  // answers and reasons in the effective language whatever the preset says.
  const env = buildEnvBlock(locale, new Date())

  // Render extension-provided template variables ({{ext.<id>.<name>}}) and
  // built-ins ({{date}}/{{datetime}}) into the system prompt. Unresolvable
  // variables are left as literal placeholders.
  const sessionName = sid
  const renderedPrompt = await renderTemplate(
    systemPrompt,
    deps.bus,
    tenant,
    sessionName,
  )

  // Max steps per turn: session override → preset → fixed default. A
  // resolved value of 0 is invalid (the loop would never run).
  const maxTurns =
    session.max_turns > 0
      ? session.max_turns
      : presetRow !== null && presetRow.max_turns > 0
        ? presetRow.max_turns
        : deps.config.defaultMaxTurns
  if (maxTurns <= 0) {
    return `max_turns must be > 0 (session/preset/default all resolved to 0)`
  }

  const resolved = await deps.llm.resolve(deps.db, tenant, session.model)
  if (resolved.isErr()) return resolved.error

  // Resolve the selected reasoning variant (if any) into AI-SDK
  // providerOptions + request headers. Strict provider+model lookup against
  // the models.dev catalog; no variant ⇒ no providerOptions sent.
  const { providerOptions, headers } = await resolveVariantOptions(
    deps,
    resolved.value.providerId,
    resolved.value.modelId,
    resolved.value.apiType,
    session.variant,
  )

  // Project the effective locale as a session variable so extensions can
  // localize their tool-result text. Written by the agent (provider "agent")
  // into the shared vars bucket during each turn. AWAITED (failures are
  // logged, not fatal): a tool running later in THIS turn (e.g.
  // subsession-create reading the locale) must observe the write — a
  // fire-and-forget put raced the tool's read in the wild.
  await setSessionVariable(
    deps.bus,
    tenant,
    'agent',
    sid,
    'locale',
    locale,
  ).catch(e => {
    logger.warn(
      { tenant, sid, err: String(e) },
      'locale session-variable projection failed',
    )
  })

  return {
    tools,
    system: `${renderedPrompt}\n\n${env}`,
    maxTurns,
    model: resolved.value.model,
    providerOptions,
    headers,
  }
}

/** Resolve the preset's system prompt honoring the effective locale. */
function presetPromptFor(preset: PresetRow, locale: string): string {
  // Parse `system_prompt_i18n` as { locale: template }; fall back to the
  // default `system_prompt` (English) when absent or unmatched.
  if (preset.system_prompt_i18n && preset.system_prompt_i18n !== '{}') {
    const map = parse(
      z.record(z.string(), z.string()),
      preset.system_prompt_i18n,
    )
    if (map.isOk()) {
      const picked = pickLocalized(map.value, locale)
      if (picked !== null) return picked
    }
  }
  return preset.system_prompt
}

/**
 * Resolve the selected reasoning variant to AI-SDK providerOptions + request
 * headers. Unknown variant or catalog miss ⇒ `{}`/undefined (provider
 * defaults; no providerOptions sent).
 */
async function resolveVariantOptions(
  deps: AgentDeps,
  providerId: string,
  modelId: string,
  apiType: string,
  variantId: string,
): Promise<{
  providerOptions: Record<string, JsonObject> | undefined
  headers: Record<string, string> | undefined
}> {
  if (variantId === '')
    return { providerOptions: undefined, headers: undefined }
  const catalog = await getModelsDev(deps.bus)
  const model = catalogModel(catalog, providerId, modelId)
  if (model === null) {
    logger.warn(
      { provider: providerId, model: modelId, variant: variantId },
      'variant requested but model not in catalog; ignoring',
    )
    return { providerOptions: undefined, headers: undefined }
  }
  const def = findVariant(model, apiType, variantId)
  if (def === null) {
    logger.warn(
      { provider: providerId, model: modelId, variant: variantId },
      'unknown variant; ignoring',
    )
    return { providerOptions: undefined, headers: undefined }
  }
  return {
    providerOptions:
      Object.keys(def.providerOptions).length > 0
        ? def.providerOptions
        : undefined,
    headers: def.headers,
  }
}
