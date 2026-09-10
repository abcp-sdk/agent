// Built-in system presets for the GENERIC standalone agent.
//
// The base agent ships exactly ONE preset — `default` — which has NO tool
// whitelist (empty `tools` = every discovered tool is allowed) and a generic
// bilingual system prompt. Host-specific presets (e.g. easylab's
// plan/explore/build) are NOT baked in here; they are injected at deploy time
// via `SYSTEM_PRESETS_FILE` (see `seedDefaults`), so the standalone agent
// never sees them.
import type { PresetRowInternal } from './kv-store.js'

export const SYSTEM_PRESETS: readonly PresetRowInternal[] = [
  {
    id: 'default',
    systemPrompt: 'You are a helpful assistant.',
    systemPromptI18n: JSON.stringify({
      en: 'You are a helpful assistant.',
      zh: '你是一个有用的助手。',
    }),
    // Empty whitelist = no restriction: all discovered tools are allowed.
    tools: '[]',
    maxTurns: 25,
    isSystem: true,
  },
]

export function isSystemPreset(id: string): boolean {
  return SYSTEM_PRESETS.some(p => p.id === id)
}

/**
 * System-preset ids that existed in a previous set but were retired. Kept
 * distinct from user presets so bootstrap can remove exactly these stale keys
 * (never a user's custom preset). The three-role orchestrator/executor/analyst
 * names, plus the easylab plan/explore/build trio that used to be baked into
 * the generic agent (they now live in easylab's own deployment).
 */
const RETIRED_SYSTEM_PRESETS = [
  'orchestrator',
  'executor',
  'analyst',
] as const

export function isRetiredSystemPreset(id: string): boolean {
  return (RETIRED_SYSTEM_PRESETS as readonly string[]).includes(id)
}
