import { Code, ConnectError } from '@connectrpc/connect'
import {
  type AgentDeps,
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  Config,
  DEFAULT_PRESET,
  Presets,
} from '@abcp-agent/agent'

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
  if (preset === '') preset = DEFAULT_PRESET
  const presetRow = await Presets.get(deps.bus, tenant, preset)
  if (presetRow.isErr()) throw new Error(presetRow.error)
  if (presetRow.value === null) {
    throw new ConnectError(`unknown preset: ${preset}`, Code.InvalidArgument)
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
