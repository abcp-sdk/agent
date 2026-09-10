import type { PresetRow } from '@easylab-agent/schema'
import { readFileSync } from 'node:fs'
import { ResultAsync } from 'neverthrow'
import type { Bus } from './bus.js'
import { BUCKET_CONFIG, BUCKET_PRESETS } from './bus.js'
import {
  isRetiredSystemPreset,
  isSystemPreset,
  SYSTEM_PRESETS,
} from './default-presets.js'
import { logger } from './logger.js'

/** No-expiry TTL for durable KV entries. */
const NO_TTL = 0

/** Key holding the JSON array of preset ids (KV has no native listing). */
const PRESET_INDEX_KEY = '__ids__'

function ra<T>(op: Promise<T>, context: string): ResultAsync<T, string> {
  return ResultAsync.fromPromise(op, e => `${context}: ${String(e)}`)
}

interface PresetRowInternal {
  id: string
  systemPrompt: string
  systemPromptI18n: string
  tools: string
  maxTurns: number
  /** True for immutable system presets (built-in or host-injected). Persisted
   * so host presets are system without hardcoding their ids in the agent. */
  isSystem?: boolean
}

export type { PresetRowInternal }

function rowToJson(row: PresetRowInternal): string {
  return JSON.stringify({
    id: row.id,
    system_prompt: row.systemPrompt,
    system_prompt_i18n: row.systemPromptI18n,
    tools: row.tools,
    max_turns: row.maxTurns,
    is_system: row.isSystem ?? false,
  })
}

function jsonToRow(raw: string): PresetRowInternal | null {
  try {
    const v = JSON.parse(raw) as Record<string, unknown>
    const id = String(v.id ?? '')
    if (id === '') return null
    return {
      id,
      systemPrompt: String(v.system_prompt ?? ''),
      systemPromptI18n: String(v.system_prompt_i18n ?? '{}'),
      tools: String(v.tools ?? '[]'),
      maxTurns: Number(v.max_turns ?? 0),
      isSystem: v.is_system === true,
    }
  } catch {
    return null
  }
}

function toRow(r: PresetRowInternal): PresetRow {
  return {
    id: r.id,
    system_prompt: r.systemPrompt,
    system_prompt_i18n: r.systemPromptI18n,
    tools: r.tools,
    max_turns: r.maxTurns,
    // Persisted flag wins; built-in ids are always system (back-compat for
    // legacy rows seeded before is_system was persisted).
    is_system: r.isSystem === true || isSystemPreset(r.id),
  }
}

async function readPresetIndex(bus: Bus): Promise<string[]> {
  const raw = await bus.kvGet(BUCKET_PRESETS, PRESET_INDEX_KEY)
  if (raw === null) return []
  try {
    const ids = JSON.parse(raw)
    return Array.isArray(ids) ? ids.map(String).filter(id => id !== '') : []
  } catch {
    return []
  }
}

/**
 * Read-merge-put index maintenance. Preset writes are rare single-admin
 * operations, so the microsecond read/write race between two concurrent
 * upserts (losing one id from the index) is accepted; re-running the
 * upsert self-heals. `list` also drops ids whose key is gone.
 */
async function addToPresetIndex(bus: Bus, id: string): Promise<void> {
  const ids = await readPresetIndex(bus)
  if (!ids.includes(id)) {
    await bus.kvPut(
      BUCKET_PRESETS,
      PRESET_INDEX_KEY,
      JSON.stringify([...ids, id]),
      NO_TTL,
    )
  }
}

async function removeFromPresetIndex(bus: Bus, id: string): Promise<void> {
  const ids = (await readPresetIndex(bus)).filter(x => x !== id)
  await bus.kvPut(BUCKET_PRESETS, PRESET_INDEX_KEY, JSON.stringify(ids), NO_TTL)
}

/** Load host-injected system presets from `SYSTEM_PRESETS_FILE`: a JSON file
 *  containing an array of `{ id, system_prompt, system_prompt_i18n?, tools?,
 *  max_turns? }` (or the snake_case JSON the API uses). Mounted as a
 *  ConfigMap at deploy time. Each entry is normalized to a PresetRowInternal
 *  (always is_system). Invalid entries are skipped with a warning so a
 *  malformed file never crashes boot. */
function loadInjectedPresets(): PresetRowInternal[] {
  let raw: string | null = null
  const file = process.env.SYSTEM_PRESETS_FILE
  try {
    if (file !== undefined && file !== '') {
      raw = readFileSync(file, 'utf8')
    }
  } catch (e) {
    logger.warn({ err: String(e), file }, 'SYSTEM_PRESETS_FILE read failed')
    return []
  }
  if (raw === null) return []
  let arr: unknown
  try {
    arr = JSON.parse(raw)
  } catch (e) {
    logger.warn({ err: String(e) }, 'injected presets JSON parse failed')
    return []
  }
  if (!Array.isArray(arr)) return []
  const out: PresetRowInternal[] = []
  for (const item of arr) {
    if (item === null || typeof item !== 'object') continue
    const v = item as Record<string, unknown>
    const id = String(v.id ?? '')
    if (id === '') continue
    const i18n = v.system_prompt_i18n ?? v.systemPromptI18n ?? '{}'
    const tools = v.tools ?? []
    out.push({
      id,
      systemPrompt: String(v.system_prompt ?? v.systemPrompt ?? ''),
      systemPromptI18n: typeof i18n === 'string' ? i18n : JSON.stringify(i18n),
      tools: typeof tools === 'string' ? tools : JSON.stringify(tools),
      maxTurns: Number(v.max_turns ?? v.maxTurns ?? 0),
      isSystem: true,
    })
  }
  return out
}

export const Presets = {
  list(bus: Bus): ResultAsync<PresetRow[], string> {
    return ra(
      (async () => {
        const ids = await readPresetIndex(bus)
        const out: PresetRow[] = []
        for (const id of ids) {
          const raw = await bus.kvGet(BUCKET_PRESETS, id)
          if (raw === null) continue
          const row = jsonToRow(raw)
          if (row === null) continue
          out.push(toRow(row))
        }
        return out.sort((a, b) => (a.id < b.id ? -1 : 1))
      })(),
      'list presets',
    )
  },

  get(bus: Bus, id: string): ResultAsync<PresetRow | null, string> {
    return ra(
      (async () => {
        const raw = await bus.kvGet(BUCKET_PRESETS, id)
        if (raw === null) return null
        const row = jsonToRow(raw)
        return row === null ? null : toRow(row)
      })(),
      'get preset',
    )
  },

  upsert(bus: Bus, row: PresetRowInternal): ResultAsync<void, string> {
    return ra(
      (async () => {
        // A preset is immutable when it is a built-in system preset OR the
        // persisted row is flagged system (host-injected presets). Read the
        // current row so host presets are protected without hardcoding ids.
        if (isSystemPreset(row.id)) {
          throw new Error(`system preset '${row.id}' is immutable`)
        }
        const existing = await bus.kvGet(BUCKET_PRESETS, row.id)
        if (existing !== null && jsonToRow(existing)?.isSystem === true) {
          throw new Error(`system preset '${row.id}' is immutable`)
        }
        await bus.kvPut(BUCKET_PRESETS, row.id, rowToJson(row), NO_TTL)
        await addToPresetIndex(bus, row.id)
      })(),
      'upsert preset',
    )
  },

  delete(bus: Bus, id: string): ResultAsync<void, string> {
    return ra(
      (async () => {
        if (isSystemPreset(id)) {
          throw new Error(`system preset '${id}' is immutable`)
        }
        const existing = await bus.kvGet(BUCKET_PRESETS, id)
        if (existing !== null && jsonToRow(existing)?.isSystem === true) {
          throw new Error(`system preset '${id}' is immutable`)
        }
        await bus.kvDelete(BUCKET_PRESETS, id)
        await removeFromPresetIndex(bus, id)
      })(),
      'delete preset',
    )
  },

  /**
   * Seed the immutable system presets at boot. The built-in `default` preset
   * is always seeded; host-specific presets are loaded from the environment
   * (`SYSTEM_PRESETS_FILE`: a JSON file containing an array) so a deployment
   * (e.g. easylab) can register its own immutable presets without baking them
   * into the generic agent. All injected presets are marked `is_system`.
   * Content drift is refreshed in place; retired ids are pruned.
   */
  seedDefaults(bus: Bus): ResultAsync<void, string> {
    return ra(
      (async () => {
        const injected = loadInjectedPresets()
        const seededIds = new Set(
          [...SYSTEM_PRESETS, ...injected].map(p => p.id),
        )
        for (const d of [...SYSTEM_PRESETS, ...injected]) {
          const want = rowToJson({ ...d, isSystem: true })
          const existing = await bus.kvGet(BUCKET_PRESETS, d.id)
          const created =
            existing === null
              ? await bus.kvCreate(BUCKET_PRESETS, d.id, want, NO_TTL)
              : null
          if (created !== null) {
            await addToPresetIndex(bus, d.id)
          } else if (existing !== want) {
            // Drifted system preset (renamed/fixed tool names, kebab-case):
            // refresh in place so the bucket mirrors the shipped preset.
            await bus.kvPut(BUCKET_PRESETS, d.id, want, NO_TTL)
          }
        }
        // Clean retired system-preset ids that are no longer seeded. Never
        // touches seeded presets or other user presets.
        const ids = await readPresetIndex(bus)
        for (const id of ids) {
          if (seededIds.has(id)) continue
          if (isRetiredSystemPreset(id)) {
            await bus.kvDelete(BUCKET_PRESETS, id)
            await removeFromPresetIndex(bus, id)
          }
        }
      })(),
      'seed default presets',
    )
  },
}

export const Config = {
  get(bus: Bus, key: string): ResultAsync<string | null, string> {
    return ra(bus.kvGet(BUCKET_CONFIG, key), 'get config')
  },

  set(bus: Bus, key: string, value: string): ResultAsync<void, string> {
    return ra(bus.kvPut(BUCKET_CONFIG, key, value, NO_TTL), 'set config')
  },
}
