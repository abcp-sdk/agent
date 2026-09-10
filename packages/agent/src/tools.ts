import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  type ExtensionConfigItem,
  ExtensionManifestSchema,
  type ExtensionTool,
} from '@easylab-agent/schema'
import { jsonSchema, type Tool } from 'ai'
import { z } from 'zod'
import type { Bus } from './bus.js'
import { EXTENSION_DISCOVER_SUBJECT } from './extensions.js'
import { localizeSchema, pickDescription } from './i18n.js'
import { parse, type ToolResult } from './json.js'

export const ToolManifestSchema = z.object({
  name: z.string(),
  description: z.string(),
  descriptions: z.record(z.string(), z.string()).optional(),
  input_schema: z.record(z.string(), z.unknown()).optional(),
})

/** KV bucket that mirrors applied extension config values (per-extension). */
export const EXT_CONFIG_KV_BUCKET = 'cfg'

/**
 * KV key for a global extension config value, mirroring the SDK's
 * `kvKey(extId, 'global', '', name) => '${extId}.${name}'`. Session-scoped
 * overrides would add an escaped segment and are not part of the global
 * config surface surfaced here.
 */
function extConfigKey(extId: string, name: string): string {
  return `${extId}.${name}`
}

/// Read a global extension config value from the `cfg` bucket. Returns
/// `undefined` when unset. Handles the SDK envelope (`{r,v}`) and the
/// pre-envelope bare-value fallback.
async function readExtConfigValue(
  bus: Bus,
  extId: string,
  name: string,
): Promise<unknown> {
  const raw = await bus.kvGet(EXT_CONFIG_KV_BUCKET, extConfigKey(extId, name))
  if (raw === null || raw === undefined) return undefined
  try {
    const parsed: unknown = JSON.parse(raw)
    if (parsed !== null && typeof parsed === 'object' && 'v' in parsed) {
      return (parsed as { v: unknown }).v
    }
    return parsed
  } catch {
    return raw
  }
}

/**
 * Aggregate the per-extension config surface into `{ [toolName]: { [knob]:
 * value } }`, keyed the way the UI reads it. Reads global values from the `cfg`
 * bucket (the single source of truth for per-knob PUTs), so saving via
 * `PUT /tool-config/{extId}/{name}` is immediately visible here.
 */
export async function toolConfigMap(
  bus: Bus,
): Promise<Record<string, Record<string, unknown>>> {
  const discovered = await discoverToolsCached(bus)
  const out: Record<string, Record<string, unknown>> = {}
  for (const t of discovered) {
    for (const c of t.extConfig ?? []) {
      const v = await readExtConfigValue(bus, t.extId, c.name)
      if (v !== undefined) {
        const bucket = out[t.name] ?? {}
        bucket[c.name] = v
        out[t.name] = bucket
      }
    }
  }
  return out
}

/**
 * Tools whose declared `requiredConfig` value is unset. Used to hard-disable
 * them from the AI tool set (so the model cannot call a tool it cannot run)
 * and keep the UI's "needs config" accurate. Keyed by qualified name. A tool
 * that declares a required_config and finds it empty cannot run now, so it is
 * blocked regardless of the config item's own flags.
 */
export async function toolsBlockedByMissingRequired(
  bus: Bus,
  discovered: DiscoveredTool[],
): Promise<Set<string>> {
  const blocked = new Set<string>()
  for (const t of discovered) {
    for (const c of t.requiredConfig ?? []) {
      const v = await readExtConfigValue(bus, t.extId, c)
      if (v === undefined || v === null || v === '') {
        blocked.add(toolQualifiedName(discovered, t))
        break
      }
    }
  }
  return blocked
}

export interface DiscoveredTool {
  /** Owning extension id — tool calls go to `tool.call.{extId}.{name}`. */
  extId: string
  name: string
  description: string
  /** Localized descriptions (locale → text); `description` is the fallback. */
  descriptions?: Record<string, string>
  /** JSON Schema object describing the tool's arguments. */
  inputSchema: Record<string, unknown>
  /** Config names whose value this tool requires to run (may be shared). */
  requiredConfig?: string[]
  /** Declared config knobs of the owning extension (extension-level). */
  extConfig?: {
    name: string
    type: string
    enum_values?: string[]
    default?: unknown
    description?: string
    /** Localized descriptions (locale → text); `description` is the fallback. */
    descriptions?: Record<string, string>
    scope?: string
  }[]
}

/**
 * Wire-safe qualified name for a discovered tool: `name` when the name is
 * unique across extensions, `extId.name` when it collides. This is the exact
 * key used in the AI-tool set and in preset whitelists.
 */
export function toolQualifiedName(
  discovered: DiscoveredTool[],
  t: DiscoveredTool,
): string {
  return discovered.filter(d => d.name === t.name).length > 1
    ? `${t.extId}.${t.name}`
    : t.name
}

/**
 * Discover tools from all extensions via a NATS broadcast. Every extension
 * that declares `tools` capability contributes its tool manifests; a reply
 * that fails validation is skipped.
 */
export async function discoverTools(
  bus: Bus,
  maxWaitMs = 500,
): Promise<DiscoveredTool[]> {
  const replies = await bus
    .requestMany(EXTENSION_DISCOVER_SUBJECT, {}, { maxWaitMs })
    .catch(() => [])
  const out: DiscoveredTool[] = []
  for (const env of replies) {
    const parsed = parse(ExtensionManifestSchema, JSON.stringify(env.payload))
    if (parsed.isErr()) continue
    const m = parsed.value
    if (!m.capabilities.includes('tools')) continue
    const config = m.config as unknown as ExtensionConfigItem[]
    for (const t of m.tools ?? []) {
      out.push(withExtConfig(toolToDiscovered(m.id, t), config))
    }
  }
  return out
}

// ---- cached discovery (the per-turn hot path) ----

interface DiscoveryCache {
  tools: DiscoveredTool[]
  expiresAt: number
  inFlight: Promise<DiscoveredTool[]> | null
}

const discoveryCache: DiscoveryCache = {
  tools: [],
  expiresAt: 0,
  inFlight: null,
}

/**
 * discoverTools with a TTL cache: every turn would otherwise pay the full
 * broadcast wait (~500ms) before the model even starts. Concurrent turns
 * share one in-flight broadcast (no thundering herd). Extensions restarted
 * with a changed toolset surface within one (fixed) TTL.
 */
export async function discoverToolsCached(
  bus: Bus,
  ttlMs = discoveryTtlMs(),
): Promise<DiscoveredTool[]> {
  if (ttlMs <= 0) return discoverTools(bus)
  const now = Date.now()
  if (discoveryCache.expiresAt > now) return discoveryCache.tools
  if (discoveryCache.inFlight !== null) return discoveryCache.inFlight

  discoveryCache.inFlight = discoverTools(bus).then(tools => {
    discoveryCache.tools = tools
    discoveryCache.expiresAt = Date.now() + ttlMs
    discoveryCache.inFlight = null
    return tools
  })
  return discoveryCache.inFlight
}

/** Drop the cached discovery result (e.g. after config changes). */
export function invalidateDiscoveryCache(): void {
  discoveryCache.tools = []
  discoveryCache.expiresAt = 0
}

function discoveryTtlMs(): number {
  // Fixed cache TTL for extension/tool discovery (not configurable).
  return 30_000
}

function toolToDiscovered(extId: string, t: ExtensionTool): DiscoveredTool {
  return {
    extId,
    name: t.name,
    description: t.description,
    ...(t.descriptions !== undefined ? { descriptions: t.descriptions } : {}),
    inputSchema: t.input_schema ?? { type: 'object', properties: {} },
    ...(t.required_config !== undefined
      ? { requiredConfig: t.required_config }
      : {}),
  }
}

/**
 * Attach the owning extension's declared config knobs to a tool, but ONLY the
 * ones the tool declares it requires (`tool.requiredConfig`). A tool that
 * declares no required config (or references one the extension did not
 * declare) gets none. Config is extension-level; `requiredConfig` narrows it
 * for the per-tool UI, so a config can be shared by several tools without
 * being exposed everywhere.
 */
function withExtConfig(
  t: DiscoveredTool,
  config?: ExtensionConfigItem[],
): DiscoveredTool {
  if (!config || config.length === 0) return t
  const wanted = new Set(t.requiredConfig ?? [])
  const extConfig = config
    .filter(c => wanted.has(c.name))
    .map(c => ({
      name: c.name,
      type: c.type,
      enum_values: c.enum_values ?? [],
      default: c.default,
      description: c.description,
      ...(c.descriptions !== undefined ? { descriptions: c.descriptions } : {}),
      scope: c.scope,
    }))
  if (extConfig.length === 0) return t
  return {
    ...t,
    extConfig: extConfig as NonNullable<DiscoveredTool['extConfig']>,
  }
}

/**
 * Build the AI SDK tool set from discovered manifests. `sessionId`, when
 * given, rides the first-class `session_name` envelope field (carried by
 * AbcAgent.callTool) — it is NOT injected into tool arguments, so the model
 * can neither see nor forge it.
 */
export function buildAiTools(
  discovered: DiscoveredTool[],
  bus: Bus,
  timeoutMs: number,
  sessionId?: string,
  abortSignal?: AbortSignal,
  locale?: string,
  blocked?: Set<string>,
): Record<string, Tool<Record<string, unknown>, ToolResult>> {
  const tools: Record<string, Tool<Record<string, unknown>, ToolResult>> = {}
  for (const t of discovered) {
    // Two extensions may expose the same bare tool name; the AI SDK tool set
    // is keyed by name, so a collision would silently drop one. Namespace the
    // AI-tool key by extension id while keeping the wire tool name intact.
    const aiName = toolQualifiedName(discovered, t)
    if (tools[aiName] !== undefined) continue
    // Hard-disable tools whose required config is unset: the model must not be
    // handed a tool it cannot run. Blocked names are qualified (aiName).
    if (blocked?.has(aiName)) continue
    const description = locale
      ? pickDescription(t.description, t.descriptions, locale)
      : t.description
    // Property-level i18n uses the same `descriptions` convention inside the
    // schema; resolve it for the locale and strip the non-standard keys.
    const inputSchema = locale
      ? localizeSchema(t.inputSchema, locale)
      : t.inputSchema
    tools[aiName] = {
      description,
      inputSchema: jsonSchema(inputSchema),
      execute: async (args, { toolCallId }) => {
        return await raceFinal(
          new AbcAgent(bus)
            .callTool(sessionId ?? '', t.extId, t.name, toolCallId, args ?? {})
            .then(r => {
              // A tool that failed surfaces its message in `r.error` (set by the
              // extension server). Propagate it as a thrown error so the AI SDK
              // emits a `tool-error` part and the agent surfaces the message,
              // instead of silently mapping it to an empty content (which would
              // render as a blank tool-result).
              if (r.error != null) {
                throw new Error(
                  String((r.error as { message?: string }).message ?? r.error),
                )
              }
              return {
                content: r.content ?? '',
                metadata: r.data ?? null,
              }
            }),
          timeoutMs,
          t.name,
          abortSignal,
        )
      },
    }
  }
  return tools
}

/**
 * Await a single terminal ToolResult against a wall-clock deadline. Resolves
 * to the result when it arrives first; on timeout (or a rejected call) it
 * resolves to an error-content ToolResult so a stuck tool call never hangs
 * the turn and never feeds a raw error to the model.
 */
async function raceFinal(
  p: Promise<ToolResult>,
  timeoutMs: number,
  name: string,
  abortSignal?: AbortSignal,
): Promise<ToolResult> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const deadline = new Promise<ToolResult>(resolve => {
    timer = setTimeout(() => {
      resolve({ content: `tool '${name}' timed out`, metadata: null })
    }, timeoutMs)
  })
  // Interrupt (stop button) must also cancel an in-flight tool call, not just
  // the LLM stream: resolve early the moment the abort signal fires so a long
  // tool (e.g. sandbox-job-wait) cannot block the turn past an interrupt.
  const aborted = new Promise<ToolResult>(resolve => {
    const onAbort = () =>
      resolve({ content: `tool '${name}' interrupted`, metadata: null })
    if (abortSignal === undefined) return
    if (abortSignal.aborted) {
      onAbort()
      return
    }
    abortSignal.addEventListener('abort', onAbort, { once: true })
  })
  try {
    return await Promise.race([p, deadline, aborted]).catch(e => ({
      content: `tool '${name}' failed: ${String(e)}`,
      metadata: null,
    }))
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}
