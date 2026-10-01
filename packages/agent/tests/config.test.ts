import { describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config.js'

describe('loadConfig healthPort', () => {
  it('defaults to 8081 when AGENT_HEALTH_PORT is unset', () => {
    expect(loadConfig({} as NodeJS.ProcessEnv).healthPort).toBe(8081)
  })

  it('parses an explicit port, including 0 to disable', () => {
    expect(
      loadConfig({ AGENT_HEALTH_PORT: '9090' } as NodeJS.ProcessEnv).healthPort,
    ).toBe(9090)
    expect(
      loadConfig({ AGENT_HEALTH_PORT: '0' } as NodeJS.ProcessEnv).healthPort,
    ).toBe(0)
  })

  it('falls back to the default on a bad value', () => {
    expect(
      loadConfig({
        AGENT_HEALTH_PORT: 'not-a-port',
      } as NodeJS.ProcessEnv).healthPort,
    ).toBe(8081)
    expect(
      loadConfig({ AGENT_HEALTH_PORT: '99999' } as NodeJS.ProcessEnv)
        .healthPort,
    ).toBe(8081)
  })
})

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

describe('loadConfig retry budgets', () => {
  it('defaults maxRetries to 20 (request-start) and streamRetries to 3', () => {
    const cfg = loadConfig({} as NodeJS.ProcessEnv)
    expect(cfg.llmMaxRetries).toBe(20)
    expect(cfg.llmStreamRetries).toBe(3)
  })

  it('honors LLM_MAX_RETRIES / LLM_STREAM_RETRIES overrides', () => {
    const cfg = loadConfig({
      LLM_MAX_RETRIES: '2',
      LLM_STREAM_RETRIES: '5',
    } as NodeJS.ProcessEnv)
    expect(cfg.llmMaxRetries).toBe(2)
    expect(cfg.llmStreamRetries).toBe(5)
  })

  it('falls back to the default on an invalid count', () => {
    const cfg = loadConfig({ LLM_MAX_RETRIES: 'abc' } as NodeJS.ProcessEnv)
    expect(cfg.llmMaxRetries).toBe(20)
  })
})

describe('loadConfig idlewatch', () => {
  it('defaults to disabled with a 120s interval', () => {
    const cfg = loadConfig({} as NodeJS.ProcessEnv)
    expect(cfg.idlewatchEnabled).toBe(false)
    expect(cfg.idlewatchIntervalMs).toBe(120_000)
  })

  it('enables via IDLEWATCH_ENABLED=true (case-insensitive)', () => {
    expect(
      loadConfig({ IDLEWATCH_ENABLED: 'TRUE' } as NodeJS.ProcessEnv)
        .idlewatchEnabled,
    ).toBe(true)
    expect(
      loadConfig({ IDLEWATCH_ENABLED: 'false' } as NodeJS.ProcessEnv)
        .idlewatchEnabled,
    ).toBe(false)
  })

  it('parses a Go-style interval (2m, 90s, 500ms)', () => {
    expect(
      loadConfig({ IDLEWATCH_INTERVAL: '2m' } as NodeJS.ProcessEnv)
        .idlewatchIntervalMs,
    ).toBe(120_000)
    expect(
      loadConfig({ IDLEWATCH_INTERVAL: '90s' } as NodeJS.ProcessEnv)
        .idlewatchIntervalMs,
    ).toBe(90_000)
    expect(
      loadConfig({ IDLEWATCH_INTERVAL: '500ms' } as NodeJS.ProcessEnv)
        .idlewatchIntervalMs,
    ).toBe(500)
  })

  it('falls back on an invalid interval', () => {
    expect(
      loadConfig({ IDLEWATCH_INTERVAL: 'nope' } as NodeJS.ProcessEnv)
        .idlewatchIntervalMs,
    ).toBe(120_000)
  })
})
