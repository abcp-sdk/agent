import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { AgentDeps, Bus, LlmRegistry } from '@easylab-agent/agent'
import {
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  connectDb,
  DEFAULT_PRESET,
  Presets,
} from '@easylab-agent/agent'
import { err, ok } from 'neverthrow'
import { describe, expect, it } from 'vitest'
import { resolveSessionDefaults } from '../src/connect.js'

const T = 't1'

/**
 * A bus stub that stores KV in memory and serves BOTH the presets bucket and
 * the config bucket that resolveSessionDefaults touches.
 */
function fakeBus(kv: Record<string, string> = {}) {
  const store = { ...kv }
  const bus = {
    kvGet: async (bucket: string, key: string) =>
      store[`${bucket}\n${key}`] ?? null,
    kvPut: async (bucket: string, key: string, value: string) => {
      store[`${bucket}\n${key}`] = value
    },
    kvCreate: async (bucket: string, key: string, value: string) => {
      const k = `${bucket}\n${key}`
      if (store[k] !== undefined) return null
      store[k] = value
      return 1
    },
    kvDelete: async (bucket: string, key: string) => {
      delete store[`${bucket}\n${key}`]
    },
  } as unknown as Bus
  return { bus, store }
}

// A registry whose resolve() accepts only refs in `known` (provider/model).
function fakeLlm(known: string[]): LlmRegistry {
  return {
    resolve: async (_db: unknown, _tenant: string, ref: string) =>
      known.includes(ref)
        ? ok({
            model: {} as never,
            modelId: ref,
            providerId: ref.split('/')[0] ?? '',
            apiType: 'openai-compatible',
          })
        : err(`provider not found: ${ref}`),
  } as unknown as LlmRegistry
}

async function fixture(opts: {
  presets?: string[]
  config?: Record<string, string>
  knownModels?: string[]
}) {
  const dir = mkdtempSync(join(tmpdir(), 'sess-defaults-'))
  const dbRes = await connectDb('sqlite', `sqlite://${join(dir, 'agent.db')}`)
  if (dbRes.isErr()) throw new Error(dbRes.error)
  const deps = {
    db: dbRes.value,
    bus: fakeBus().bus,
    llm: fakeLlm(opts.knownModels ?? []),
  } as unknown as AgentDeps

  // Seed presets: the built-in `default` via seedDefaults, plus any extra
  // user presets requested (the system `default` is immutable via upsert).
  const { bus } = fakeBus()
  ;(deps as { bus: Bus }).bus = bus
  const seeded = await Presets.seedDefaults(bus, T)
  if (seeded.isErr()) throw new Error(seeded.error)
  for (const id of opts.presets ?? []) {
    const r = await Presets.upsert(bus, T, {
      id,
      systemPrompt: 'sys',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 25,
      isSystem: false,
    })
    if (r.isErr()) throw new Error(r.error)
  }
  // Seed config keys.
  for (const [k, v] of Object.entries(opts.config ?? {})) {
    await bus.kvPut('abc-agent-config', `t.${T}.${k}`, v)
  }
  return deps
}

describe('resolveSessionDefaults', () => {
  it('empty preset falls back to built-in default', async () => {
    const deps = await fixture({})
    const r = await resolveSessionDefaults(deps, T, '', '')
    expect(r.preset).toBe(DEFAULT_PRESET)
    expect(r.model).toBe('')
  })

  it('uses tenant default_preset / default_model when the request is empty', async () => {
    const deps = await fixture({
      presets: ['work'],
      config: {
        [CONFIG_DEFAULT_PRESET]: 'work',
        [CONFIG_DEFAULT_MODEL]: 'openai/gpt-x',
      },
      knownModels: ['openai/gpt-x'],
    })
    const r = await resolveSessionDefaults(deps, T, '', '')
    expect(r.preset).toBe('work')
    expect(r.model).toBe('openai/gpt-x')
  })

  it('request value beats the tenant default', async () => {
    const deps = await fixture({
      presets: ['work'],
      config: { [CONFIG_DEFAULT_PRESET]: 'work' },
      knownModels: ['p/m'],
    })
    const r = await resolveSessionDefaults(deps, T, 'default', 'p/m')
    expect(r.preset).toBe('default')
    expect(r.model).toBe('p/m')
  })

  it('unknown preset is rejected (InvalidArgument)', async () => {
    const deps = await fixture({})
    await expect(
      resolveSessionDefaults(deps, T, 'nope', ''),
    ).rejects.toMatchObject({ code: 3 })
  })

  it('unresolvable model is rejected (InvalidArgument)', async () => {
    const deps = await fixture({ knownModels: [] })
    await expect(
      resolveSessionDefaults(deps, T, 'default', 'ghost/model'),
    ).rejects.toMatchObject({ code: 3 })
  })

  it('empty model is allowed (session may exist with no model)', async () => {
    const deps = await fixture({})
    const r = await resolveSessionDefaults(deps, T, 'default', '')
    expect(r.model).toBe('')
  })

  it('update with empty request keeps the current preset/model', async () => {
    const deps = await fixture({
      config: { [CONFIG_DEFAULT_PRESET]: 'default' },
      knownModels: ['p/m'],
    })
    const r = await resolveSessionDefaults(deps, T, '', '', {
      preset: 'default',
      model: 'p/m',
    })
    expect(r.preset).toBe('default')
    expect(r.model).toBe('p/m')
  })
})
