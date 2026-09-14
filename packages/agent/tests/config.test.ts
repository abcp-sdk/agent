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
