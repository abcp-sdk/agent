import type { PresetRow } from '@easylab-agent/schema'
import { errAsync, ResultAsync } from 'neverthrow'
import type { Bus } from './bus.js'
import { BUCKET_CONFIG, BUCKET_PRESETS } from './bus.js'
import {
  isRetiredSystemPreset,
  isSystemPreset,
  SYSTEM_PRESETS,
} from './default-presets.js'

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
}

export type { PresetRowInternal }

function rowToJson(row: PresetRowInternal): string {
  return JSON.stringify({
    id: row.id,
    system_prompt: row.systemPrompt,
    system_prompt_i18n: row.systemPromptI18n,
    tools: row.tools,
    max_turns: row.maxTurns,
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
      maxTurns: Number(v.max_turns ?? 30),
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
    is_system: isSystemPreset(r.id),
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
    if (isSystemPreset(row.id)) {
      return errAsync(`system preset '${row.id}' is immutable`)
    }
    return ra(
      (async () => {
        await bus.kvPut(BUCKET_PRESETS, row.id, rowToJson(row), NO_TTL)
        await addToPresetIndex(bus, row.id)
      })(),
      'upsert preset',
    )
  },

  delete(bus: Bus, id: string): ResultAsync<void, string> {
    if (isSystemPreset(id)) {
      return errAsync(`system preset '${id}' is immutable`)
    }
    return ra(
      (async () => {
        await bus.kvDelete(BUCKET_PRESETS, id)
        await removeFromPresetIndex(bus, id)
      })(),
      'delete preset',
    )
  },

  /**
   * Seed the immutable system presets at boot. System presets are refreshed
   * whenever their content drifted from the embedded version (e.g. a tool-set
   * or kebab-case tool-name change), so the KV bucket always matches the
   * running agent. `kvCreate` is atomic: an existing key is created once, then
   * a content comparison refreshes it. User presets are never touched — only
   * keys whose id is a system preset are compared/updated.
   */
  seedDefaults(bus: Bus): ResultAsync<void, string> {
    return ra(
      (async () => {
        for (const d of SYSTEM_PRESETS) {
          const want = rowToJson(d)
          const existing = await bus.kvGet(BUCKET_PRESETS, d.id)
          const created =
            existing === null
              ? await bus.kvCreate(BUCKET_PRESETS, d.id, want, NO_TTL)
              : null
          if (created !== null) {
            await addToPresetIndex(bus, d.id)
          } else if (existing !== want) {
            // Drifted system preset (recnamed/fixed tool names, kebab-case):
            // refresh in place so the bucket mirrors the shipped preset.
            await bus.kvPut(BUCKET_PRESETS, d.id, want, NO_TTL)
          }
        }
        // Clean retired system-preset ids that are no longer in SYSTEM_PRESETS
        // (e.g. after a preset-set change). Done in the same bootstrap pass so
        // stale system keys never linger as editable user presets.
        const ids = await readPresetIndex(bus)
        for (const id of ids) {
          if (isSystemPreset(id)) continue
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
