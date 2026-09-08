import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { SYSTEM_PRESETS } from '../src/default-presets.js'
import { Presets } from '../src/kv-store.js'

const BUCKET = 'abc-presets'

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

describe('system preset set', () => {
  it('defines plan/explore/build', () => {
    const ids = SYSTEM_PRESETS.map(p => p.id)
    expect(ids).toEqual(['plan', 'explore', 'build'])
  })

  it('build preset includes full capability; plan/explore are restricted', () => {
    const build = SYSTEM_PRESETS.find(p => p.id === 'build')!
    const plan = SYSTEM_PRESETS.find(p => p.id === 'plan')!
    const explore = SYSTEM_PRESETS.find(p => p.id === 'explore')!
    const buildTools = JSON.parse(build.tools)
    const planTools = JSON.parse(plan.tools)
    const exploreTools = JSON.parse(explore.tools)
    // build has repo writes + sandbox + build/deploy/publish + read-only searches
    expect(buildTools).toContain('write')
    expect(buildTools).toContain('vcs-rebase')
    expect(buildTools).toContain('sandbox-run')
    expect(buildTools).toContain('container-build')
    expect(buildTools).toContain('package-publish')
    expect(buildTools).toContain('service-deploy')
    expect(buildTools).toContain('container-search')
    expect(buildTools).toContain('service-list')
    expect(buildTools).toContain('package-search')
    expect(buildTools).toContain('pull-git-repo')
    // no helm / mr in build
    expect(
      buildTools.some(
        (t: string) => t.startsWith('helm') || t.startsWith('mr'),
      ),
    ).toBe(false)
    // plan has no writes/sandbox/build
    expect(
      planTools.some(
        (t: string) =>
          t.startsWith('sandbox') ||
          t === 'write' ||
          t === 'delete' ||
          t === 'edit' ||
          t.startsWith('container') ||
          t.startsWith('package'),
      ),
    ).toBe(false)
    // explore adds sandbox but still no writes/build
    expect(exploreTools).toContain('sandbox-run')
    // explore must not port sandbox changes into the repo (the one sandbox
    // tool that writes the repo — only build may).
    expect(exploreTools.includes('sandbox-port')).toBe(false)
    expect(
      exploreTools.some(
        (t: string) =>
          t === 'write' ||
          t === 'delete' ||
          t === 'edit' ||
          t.startsWith('container') ||
          t.startsWith('package'),
      ),
    ).toBe(false)
  })

  it('every system preset is bilingual and lists known tools', () => {
    for (const p of SYSTEM_PRESETS) {
      expect(p.tools).toBeTruthy()
      expect(JSON.parse(p.tools).length).toBeGreaterThan(0)
      const i18n = JSON.parse(p.systemPromptI18n)
      expect(i18n.zh).toMatch(/.+/)
      expect(i18n.en).toBe(p.systemPrompt)
    }
  })
})

describe('Presets.seedDefaults', () => {
  it('seeds every system preset into an empty bucket', async () => {
    const { bus, kv } = fakeBus()
    const r = await Presets.seedDefaults(bus)
    expect(r.isOk()).toBe(true)
    for (const p of SYSTEM_PRESETS) {
      expect(kv[p.id]).toBeDefined()
    }
    expect(JSON.parse(kv.__ids__)).toEqual(
      expect.arrayContaining(SYSTEM_PRESETS.map(p => p.id)),
    )
  })

  it('refreshes a drifted system preset to the embedded version', async () => {
    const { bus, kv } = fakeBus()
    await Presets.seedDefaults(bus)
    // Simulate a restore/another writer drifting one system preset's value
    // (e.g. an old snake_case tool name from a previous KV seed).
    const edited = SYSTEM_PRESETS[0]
    await bus.kvPut(
      BUCKET,
      edited.id,
      JSON.stringify({ ...JSON.parse(kv[edited.id]), system_prompt: 'EDITED' }),
    )
    await Presets.seedDefaults(bus)
    const after = await bus.kvGet(BUCKET, edited.id)
    expect(after).toBe(
      JSON.stringify({
        id: edited.id,
        system_prompt: edited.systemPrompt,
        system_prompt_i18n: edited.systemPromptI18n,
        tools: edited.tools,
        max_turns: edited.maxTurns,
      }),
    )
  })

  it('leaves an already-correct system preset unchanged', async () => {
    const { bus, kv } = fakeBus()
    await Presets.seedDefaults(bus)
    const before = await bus.kvGet(BUCKET, SYSTEM_PRESETS[0].id)
    await Presets.seedDefaults(bus)
    expect(await bus.kvGet(BUCKET, SYSTEM_PRESETS[0].id)).toBe(before)
  })

  it('never touches user presets', async () => {
    const { bus, kv } = fakeBus()
    await Presets.seedDefaults(bus)
    const mine = JSON.stringify({
      id: 'my',
      system_prompt: 's',
      system_prompt_i18n: '{}',
      tools: '[]',
      max_turns: 5,
    })
    await bus.kvPut(BUCKET, 'my', mine)
    await Presets.seedDefaults(bus)
    expect(await bus.kvGet(BUCKET, 'my')).toBe(mine)
  })
})

describe('system presets are immutable', () => {
  it('rejects upsert with a system id', async () => {
    const { bus } = fakeBus()
    const r = await Presets.upsert(bus, {
      id: SYSTEM_PRESETS[0].id,
      systemPrompt: 'x',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 3,
    })
    expect(r.isErr()).toBe(true)
    expect(String(r.error)).toContain('immutable')
  })

  it('rejects delete with a system id', async () => {
    const { bus } = fakeBus()
    const r = await Presets.delete(bus, SYSTEM_PRESETS[0].id)
    expect(r.isErr()).toBe(true)
    expect(String(r.error)).toContain('immutable')
  })

  it('allows user presets upsert/delete', async () => {
    const { bus, kv } = fakeBus()
    await Presets.upsert(bus, {
      id: 'my',
      systemPrompt: 's',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 5,
    })
    expect(kv.my).toBeDefined()
    await Presets.delete(bus, 'my')
    expect(kv.my).toBeUndefined()
  })

  it('exposes is_system on list', async () => {
    const { bus, kv } = fakeBus()
    await Presets.seedDefaults(bus)
    const list = await Presets.list(bus)
    expect(list.isOk()).toBe(true)
    const sys = list.value.find(p => p.id === 'build')
    expect(sys?.is_system).toBe(true)
    await Presets.upsert(bus, {
      id: 'user1',
      systemPrompt: 's',
      systemPromptI18n: '{}',
      tools: '[]',
      maxTurns: 2,
    })
    const list2 = await Presets.list(bus)
    expect(list2.value.find(p => p.id === 'user1')?.is_system).toBe(false)
  })
})

describe('retired system presets are cleaned on seed', () => {
  it('removes retired ids (orchestrator/executor/analyst) from the bucket', async () => {
    const { bus, kv } = fakeBus()
    // Seed retired keys as if a previous preset-set existed.
    for (const id of ['orchestrator', 'executor', 'analyst', 'my', 'plan']) {
      const v =
        id === 'my' || id === 'plan'
          ? (SYSTEM_PRESETS.find(p => p.id === 'plan')?.tools ?? '[]')
          : '[]'
      await bus.kvPut(
        BUCKET,
        id,
        JSON.stringify({
          id,
          system_prompt: 'x',
          system_prompt_i18n: '{}',
          tools: v,
          max_turns: 3,
        }),
      )
    }
    // rebuild index
    await bus.kvPut(
      BUCKET,
      '__ids__',
      JSON.stringify(['orchestrator', 'executor', 'analyst', 'my', 'plan']),
    )
    await Presets.seedDefaults(bus)
    // retired ids gone; user 'my' kept; system 'plan' retained
    expect(await bus.kvGet(BUCKET, 'orchestrator')).toBeNull()
    expect(await bus.kvGet(BUCKET, 'executor')).toBeNull()
    expect(await bus.kvGet(BUCKET, 'analyst')).toBeNull()
    expect(await bus.kvGet(BUCKET, 'my')).not.toBeNull()
    expect(await bus.kvGet(BUCKET, 'plan')).not.toBeNull()
  })
})
