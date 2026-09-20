import { describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config.js'

describe('loadConfig disabledTools', () => {
  it('parses comma-separated <extId>.<name> entries into a trimmed list', () => {
    const cfg = loadConfig({
      DISABLED_TOOLS: 'bundled.subsession-create, bundled.mail-send ,,',
    } as NodeJS.ProcessEnv)
    expect(cfg.disabledTools).toEqual([
      'bundled.subsession-create',
      'bundled.mail-send',
    ])
  })

  it('drops entries without an extension prefix', () => {
    const cfg = loadConfig({
      DISABLED_TOOLS: 'mail-send,bundled.mail-send',
    } as NodeJS.ProcessEnv)
    expect(cfg.disabledTools).toEqual(['bundled.mail-send'])
  })

  it('defaults to an empty list when unset', () => {
    const cfg = loadConfig({} as NodeJS.ProcessEnv)
    expect(cfg.disabledTools).toEqual([])
  })
})

describe('loadConfig extConfigSeed', () => {
  it('parses a JSON array of seed entries', () => {
    const cfg = loadConfig({
      AGENT_EXT_CONFIG_SEED: JSON.stringify([
        {
          tenant: 'default',
          extId: 'workspace',
          name: 'worker-url',
          value: 'http://easyworker.temp.svc.cluster.local:80',
        },
      ]),
    } as NodeJS.ProcessEnv)
    expect(cfg.extConfigSeed).toEqual([
      {
        tenant: 'default',
        extId: 'workspace',
        name: 'worker-url',
        value: 'http://easyworker.temp.svc.cluster.local:80',
      },
    ])
  })

  it('defaults to empty and drops malformed entries', () => {
    expect(loadConfig({} as NodeJS.ProcessEnv).extConfigSeed).toEqual([])
    const cfg = loadConfig({
      AGENT_EXT_CONFIG_SEED: JSON.stringify([
        { tenant: 'default', extId: 'workspace' }, // missing name/value
        { tenant: 'default', extId: 'w', name: 'n', value: 3 },
      ]),
    } as NodeJS.ProcessEnv)
    expect(cfg.extConfigSeed).toEqual([
      { tenant: 'default', extId: 'w', name: 'n', value: '3' },
    ])
  })

  it('ignores invalid JSON without throwing', () => {
    expect(
      loadConfig({ AGENT_EXT_CONFIG_SEED: '{not json' } as NodeJS.ProcessEnv)
        .extConfigSeed,
    ).toEqual([])
  })
})
