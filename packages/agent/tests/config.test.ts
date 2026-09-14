import { describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config.js'

describe('loadConfig disabledTools', () => {
  it('parses a comma-separated DISABLED_TOOLS into a trimmed, non-empty list', () => {
    const cfg = loadConfig({ DISABLED_TOOLS: 'subsession-create, mail-send ,,' } as NodeJS.ProcessEnv)
    expect(cfg.disabledTools).toEqual(['subsession-create', 'mail-send'])
  })

  it('defaults to an empty list when unset', () => {
    const cfg = loadConfig({} as NodeJS.ProcessEnv)
    expect(cfg.disabledTools).toEqual([])
  })
})
