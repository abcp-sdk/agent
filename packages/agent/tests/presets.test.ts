import { randomUUID } from 'node:crypto'
import { writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { SYSTEM_PRESETS } from '../src/default-presets.js'
import { Presets } from '../src/kv-store.js'

const BUCKET = 'abc-presets'
const T = 't1'
const pk = (id: string) => `t.${T}.${id}`
const PKEY = `t.${T}.__ids__`

interface KV {
  [key: string]: string
}

function fakeBus(initial: KV = {}) {
  const kv: KV = { ...initial }
  const bus = {
    kvGet: async (b: string, key: string) =>
      b === BUCKET ? (kv[key] ?? null) : null,
    kvPut: async (b: string, key: string, value: string) => {
      if (b === BUCKET) kv[key] = value
    },
    kvCreate: async (b: string, key: string, value: string) => {
      if (b !== BUCKET) return null
      if (kv[key] !== undefined) return null
      kv[key] = value
      return 1
    },
    kvDelete: async (b: string, key: string) => {
      if (b === BUCKET) delete kv[key]
    },
  } as unknown as Bus
  return { bus, kv }
}

// Preset env vars must never leak across tests.
const PRESET_ENV = ['SYSTEM_PRESETS_FILE']
afterEach(() => {
  for (const k of PRESET_ENV) delete process.env[k]
})

describe('built-in system preset set', () => {
  it('ships exactly one generic preset: default', () => {
    expect(SYSTEM_PRESETS.map(p => p.id)).toEqual(['default'])
  })

  it('default preset has no whitelist and a bilingual IDENTITY prompt', () => {
    const def = SYSTEM_PRESETS[0]
    // Empty whitelist = every discovered tool is allowed.
    expect(JSON.parse(def.tools)).toEqual([])
    expect(def.isSystem).toBe(true)
    const i18n = JSON.parse(def.systemPromptI18n)
    // The prompt is identity ONLY: the "answer in this language" directive is
    // injected per-turn via `<env>` (languageDirective), not baked in here, so
    // it applies to every preset.
    expect(i18n.zh).toBe('你是一个有用的助手。')
    expect(i18n.zh).not.toContain('中文')
    expect(i18n.en).toBe('You are a helpful assistant.')
    expect(i18n.en).not.toContain('English')
    expect(def.systemPrompt).toBe(i18n.en)
  })
})

describe('Presets.seedDefaults', () => {
  it('seeds the built-in default preset into an empty bucket', async () => {
    const { bus, kv } = fakeBus()
    const r = await Presets.seedDefaults(bus, T)
    expect(r.isOk()).toBe(true)
    expect(kv[pk('default')]).toBeDefined()
    expect(JSON.parse(kv[PKEY])).toContain('default')
  })

  it('refreshes a drifted system preset to the embedded version', async () => {
    const { bus, kv } = fakeBus()
    await Presets.seedDefaults(bus, T)
    const edited = SYSTEM_PRESETS[0]
    await bus.kvPut(
      BUCKET,
      pk(edited.id),
      JSON.stringify({
        ...JSON.parse(kv[pk(edited.id)]),
        system_prompt: 'EDITED',
      }),
    )
    await Presets.seedDefaults(bus, T)
    const after = await bus.kvGet(BUCKET, pk(edited.id))
    expect(after).toBe(
      JSON.stringify({
        id: edited.id,
        system_prompt: edited.systemPrompt,
        system_prompt_i18n: edited.systemPromptI18n,
        tools: edited.tools,
        max_turns: edited.maxTurns,
        is_system: true,
      }),
    )
  })

  it('leaves an already-correct system preset unchanged', async () => {
    const { bus } = fakeBus()
    await Presets.seedDefaults(bus, T)
    const before = await bus.kvGet(BUCKET, pk(SYSTEM_PRESETS[0].id))
    await Presets.seedDefaults(bus, T)
    expect(await bus.kvGet(BUCKET, pk(SYSTEM_PRESETS[0].id))).toBe(before)
  })

  it('never touches user presets', async () => {
    const { bus } = fakeBus()
    await Presets.seedDefaults(bus, T)
    const mine = JSON.stringify({
      id: 'my',
      system_prompt: 's',
      system_prompt_i18n: '{}',
      tools: '[]',
      max_turns: 5,
    })
    await bus.kvPut(BUCKET, pk('my'), mine)
    await Presets.seedDefaults(bus, T)
    expect(await bus.kvGet(BUCKET, pk('my'))).toBe(mine)
  })
})

describe('host-injected system presets (SYSTEM_PRESETS_FILE)', () => {
  const writePresets = (arr: unknown): string => {
    const f = join(tmpdir(), `presets-${randomUUID()}.json`)
    writeFileSync(f, JSON.stringify(arr))
    return f
  }

  it('seeds injected presets as immutable system presets', async () => {
    process.env.SYSTEM_PRESETS_FILE = writePresets([
      {
        id: 'plan',
        system_prompt: 'plan-en',
        system_prompt_i18n: { en: 'plan-en', zh: 'plan-zh' },
        tools: ['read', 'ls'],
        max_turns: 15,
      },
    ])
    const { bus, kv } = fakeBus()
    const r = await Presets.seedDefaults(bus, T)
    expect(r.isOk()).toBe(true)
    // built-in default + injected plan both present
    expect(kv[pk('default')]).toBeDefined()
    expect(kv[pk('plan')]).toBeDefined()
    const planRow = JSON.parse(kv[pk('plan')])
    expect(planRow.is_system).toBe(true)
    expect(planRow.system_prompt).toBe('plan-en')

    // Injected presets are immutable: upsert/delete rejected.
    const up = await Presets.upsert(bus, T, {
      id: 'plan',
      systemPrompt: 'x',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 3,
    })
    expect(up.isErr()).toBe(true)
    expect(String(up.error)).toContain('immutable')
    const del = await Presets.delete(bus, T, 'plan')
    expect(del.isErr()).toBe(true)
    expect(String(del.error)).toContain('immutable')
  })

  it('is_system is surfaced on list for injected presets', async () => {
    process.env.SYSTEM_PRESETS_FILE = writePresets([
      { id: 'build', system_prompt: 'b', tools: [], max_turns: 30 },
    ])
    const { bus } = fakeBus()
    await Presets.seedDefaults(bus, T)
    const list = await Presets.list(bus, T)
    expect(list.isOk()).toBe(true)
    expect(list.value.find(p => p.id === 'build')?.is_system).toBe(true)
  })
})

describe('system presets are immutable', () => {
  it('rejects upsert with a built-in system id', async () => {
    const { bus } = fakeBus()
    const r = await Presets.upsert(bus, T, {
      id: SYSTEM_PRESETS[0].id,
      systemPrompt: 'x',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 3,
    })
    expect(r.isErr()).toBe(true)
    expect(String(r.error)).toContain('immutable')
  })

  it('rejects delete with a built-in system id', async () => {
    const { bus } = fakeBus()
    const r = await Presets.delete(bus, T, SYSTEM_PRESETS[0].id)
    expect(r.isErr()).toBe(true)
    expect(String(r.error)).toContain('immutable')
  })

  it('allows user presets upsert/delete', async () => {
    const { bus, kv } = fakeBus()
    await Presets.upsert(bus, T, {
      id: 'my',
      systemPrompt: 's',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 5,
    })
    expect(kv[pk('my')]).toBeDefined()
    await Presets.delete(bus, T, 'my')
    expect(kv[pk('my')]).toBeUndefined()
  })

  it('exposes is_system on list', async () => {
    const { bus } = fakeBus()
    await Presets.seedDefaults(bus, T)
    const list = await Presets.list(bus, T)
    expect(list.isOk()).toBe(true)
    expect(list.value.find(p => p.id === 'default')?.is_system).toBe(true)
    await Presets.upsert(bus, T, {
      id: 'user1',
      systemPrompt: 's',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 2,
    })
    const list2 = await Presets.list(bus, T)
    expect(list2.value.find(p => p.id === 'user1')?.is_system).toBe(false)
  })

  it('serves a repeated list from the short-lived cache', async () => {
    // The cache is keyed by BUS identity, so count reads on THIS bus only.
    let reads = 0
    const bus = {
      kvGet: async (b: string, key: string) => {
        if (b !== BUCKET) return null
        reads++
        if (key === PKEY) return JSON.stringify(['default'])
        if (key === pk('default'))
          return JSON.stringify({
            id: 'default',
            system_prompt: 's',
            system_prompt_i18n: '{}',
            tools: '[]',
            max_turns: 1,
          })
        return null
      },
      kvPut: async () => {},
      kvCreate: async () => null,
      kvDelete: async () => {},
    } as unknown as Bus
    await Presets.list(bus, T)
    const afterFirst = reads
    const again = await Presets.list(bus, T)
    expect(again.isOk() && again.value[0]?.id).toBe('default')
    // A cache hit performs ZERO additional KV reads.
    expect(reads).toBe(afterFirst)
  })

  it('invalidates the list cache on upsert', async () => {
    const { bus } = fakeBus()
    await Presets.seedDefaults(bus, T)
    await Presets.list(bus, T) // prime the cache
    await Presets.upsert(bus, T, {
      id: 'fresh',
      systemPrompt: 's',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 1,
    })
    const list = await Presets.list(bus, T)
    expect(list.value.find(p => p.id === 'fresh')).toBeDefined()
  })
})

describe('retired system presets are cleaned on seed', () => {
  it('removes retired ids (orchestrator/executor/analyst)', async () => {
    const { bus } = fakeBus()
    for (const id of ['orchestrator', 'executor', 'analyst', 'my']) {
      await bus.kvPut(
        BUCKET,
        pk(id),
        JSON.stringify({
          id,
          system_prompt: 'x',
          system_prompt_i18n: '{}',
          tools: '[]',
          max_turns: 3,
        }),
      )
    }
    await bus.kvPut(
      BUCKET,
      PKEY,
      JSON.stringify(['orchestrator', 'executor', 'analyst', 'my']),
    )
    await Presets.seedDefaults(bus, T)
    expect(await bus.kvGet(BUCKET, pk('orchestrator'))).toBeNull()
    expect(await bus.kvGet(BUCKET, pk('executor'))).toBeNull()
    expect(await bus.kvGet(BUCKET, pk('analyst'))).toBeNull()
    expect(await bus.kvGet(BUCKET, pk('my'))).not.toBeNull()
  })
})
