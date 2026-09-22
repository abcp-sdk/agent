import { describe, expect, it } from 'vitest'
import {
  buildEnvBlock,
  languageDirective,
  localizeSchema,
  normalizeLocale,
  pickLocalized,
  resolveLocale,
} from '../src/i18n.js'

describe('languageDirective', () => {
  it('zh (and its region variants) mandates Chinese reply AND reasoning', () => {
    for (const l of ['zh', 'zh-CN', 'zh-hant', 'ZH']) {
      const d = languageDirective(l)
      expect(d).toContain('中文')
      expect(d).toContain('思考')
    }
  })

  it('en (and any unknown locale) mandates English', () => {
    for (const l of ['en', 'en-US', 'ja', 'fr', '']) {
      const d = languageDirective(l)
      expect(d).toContain('English')
      expect(d).toContain('reason')
    }
  })

  it('covers BOTH reply and reasoning in each language', () => {
    expect(languageDirective('zh')).toContain('回复')
    expect(languageDirective('en')).toContain('reply')
  })
})

describe('buildEnvBlock', () => {
  const now = new Date('2026-09-21T12:00:00Z')

  it('always includes the date AND the language directive', () => {
    const zh = buildEnvBlock('zh', now)
    expect(zh).toContain("Today's date: 2026-09-21")
    expect(zh).toContain(languageDirective('zh'))
    expect(zh).toContain('中文')

    const en = buildEnvBlock('en', now)
    expect(en).toContain(languageDirective('en'))
    expect(en).toContain('English')
  })

  it('is a well-formed <env> block', () => {
    const b = buildEnvBlock('zh', now)
    expect(b.startsWith('<env>\n')).toBe(true)
    expect(b.endsWith('\n</env>')).toBe(true)
  })
})

describe('pickLocalized', () => {
  it('exact match wins, region falls back to primary', () => {
    const map = { zh: '中文', 'zh-hant': '繁體' }
    expect(pickLocalized(map, 'zh')).toBe('中文')
    expect(pickLocalized(map, 'zh-hans')).toBe('中文')
    expect(pickLocalized(map, 'zh-hant')).toBe('繁體')
    expect(pickLocalized(map, 'en')).toBeNull()
  })
})

describe('localizeSchema', () => {
  const schema = {
    type: 'object',
    properties: {
      target: {
        type: 'string',
        description: 'Target bookmark',
        descriptions: { zh: '目标书签' },
      },
      plain: { type: 'string', description: 'No translations here' },
      nested: {
        type: 'object',
        properties: {
          deep: {
            type: 'string',
            description: 'Deep prop',
            descriptions: { zh: '深层属性' },
          },
        },
      },
      list: {
        type: 'array',
        items: {
          type: 'string',
          description: 'Item',
          descriptions: { zh: '条目' },
        },
      },
    },
    required: ['target'],
  }

  it('resolves property descriptions for the locale and strips the map', () => {
    const out = localizeSchema(schema, 'zh')
    expect(out.properties.target.description).toBe('目标书签')
    expect(out.properties.target.descriptions).toBeUndefined()
    expect(out.properties.nested.properties.deep.description).toBe('深层属性')
    expect(out.properties.list.items.description).toBe('条目')
    // Untouched nodes keep their description.
    expect(out.properties.plain.description).toBe('No translations here')
    // Structural keys survive.
    expect(out.required).toEqual(['target'])
    expect(out.type).toBe('object')
  })

  it('missing locale entries fall back to the default description', () => {
    const out = localizeSchema(schema, 'ja')
    expect(out.properties.target.description).toBe('Target bookmark')
    expect(out.properties.target.descriptions).toBeUndefined()
  })

  it('region variants fall back to the primary language', () => {
    const out = localizeSchema(schema, 'zh-hans')
    expect(out.properties.target.description).toBe('目标书签')
  })

  it('does not mutate the input schema', () => {
    localizeSchema(schema, 'zh')
    expect(schema.properties.target.descriptions).toEqual({ zh: '目标书签' })
    expect(schema.properties.target.description).toBe('Target bookmark')
  })
})

describe('resolveLocale', () => {
  it('an explicit session locale ALWAYS wins, including "en"', () => {
    // Regression: the old loop skipped a normalized "en", so a session pinned
    // to English was overridden by a zh tenant config.
    expect(resolveLocale('en', 'zh', 'en')).toBe('en')
    expect(resolveLocale('zh', 'en', 'en')).toBe('zh')
  })

  it('an empty/unset session locale falls through to the tenant config', () => {
    expect(resolveLocale('', 'zh', 'en')).toBe('zh')
    expect(resolveLocale(undefined, 'zh', 'en')).toBe('zh')
    expect(resolveLocale(null, 'en', 'zh')).toBe('en')
  })

  it('falls back to the env default when neither is set', () => {
    expect(resolveLocale('', '', 'zh')).toBe('zh')
    expect(resolveLocale(undefined, undefined, 'en')).toBe('en')
  })

  it('normalizes region/underscore/uppercase variants', () => {
    expect(resolveLocale('ZH_CN', 'en', 'en')).toBe('zh-cn')
    expect(normalizeLocale('ZH_CN')).toBe('zh-cn')
  })
})
