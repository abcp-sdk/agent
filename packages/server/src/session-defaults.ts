import {
  type AgentDeps,
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  Config,
  DEFAULT_PRESET,
  Presets,
} from '@abcp-agent/agent'
import type { PresetRow } from '@abcp-agent/schema'
import { Code, ConnectError } from '@connectrpc/connect'

export async function resolveSessionDefaults(
  deps: AgentDeps,
  tenant: string,
  requestedPreset: string | undefined,
  requestedModel: string | undefined,
  current?: { preset?: string; model?: string },
): Promise<{ preset: string; model: string }> {
  // ---- preset ----
  let preset = (requestedPreset ?? '').trim()
  if (preset === '' && current?.preset !== undefined && current.preset !== '') {
    preset = current.preset // update with no preset change: keep the current
  }
  if (preset === '') {
    const cfg = await Config.get(deps.bus, tenant, CONFIG_DEFAULT_PRESET)
    preset = (cfg.isOk() && cfg.value ? cfg.value : '').trim()
  }
  // A REQUESTED preset that does not exist is a client error. A DEFAULT (empty
  // request → tenant `default_preset` → built-in) that no longer exists must
  // NOT break session creation: fall back to the built-in `default`, which is
  // always seeded. Otherwise a tenant whose configured default preset was
  // deleted could never create a session again.
  const requested = (requestedPreset ?? '').trim() !== ''
  let presetRow = await getPreset(deps, tenant, preset)
  if (presetRow === null) {
    if (requested) {
      throw new ConnectError(`unknown preset: ${preset}`, Code.InvalidArgument)
    }
    preset = DEFAULT_PRESET
    presetRow = await getPreset(deps, tenant, preset)
    if (presetRow === null) {
      throw new ConnectError(`unknown preset: ${preset}`, Code.InvalidArgument)
    }
  }

  // ---- model ----
  let model = (requestedModel ?? '').trim()
  if (model === '' && current?.model !== undefined && current.model !== '') {
    model = current.model // update with no model change: keep the current
  }
  if (model === '') {
    const cfg = await Config.get(deps.bus, tenant, CONFIG_DEFAULT_MODEL)
    model = (cfg.isOk() && cfg.value ? cfg.value : '').trim()
  }
  if (model !== '') {
    const resolved = await deps.llm.resolve(deps.db, tenant, model)
    if (resolved.isErr()) {
      throw new ConnectError(
        `unknown or unusable model: ${model} (${resolved.error})`,
        Code.InvalidArgument,
      )
    }
  }
  return { preset, model }
}

/** Fetch a preset row, or null when absent. Throws on a store error. */
async function getPreset(
  deps: AgentDeps,
  tenant: string,
  id: string,
): Promise<PresetRow | null> {
  const r = await Presets.get(deps.bus, tenant, id)
  if (r.isErr()) throw new Error(r.error)
  return r.value
}
