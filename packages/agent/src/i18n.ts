/**
 * Minimal i18n for the agent. Locale is a short BCP-47 tag normalized to
 * lowercase (e.g. "zh", "en", "ja", "zh-hans"). Language selection follows
 * session → config → env → default "en". Region fallback: a request for
 * "zh-hans" falls back to "zh" when the exact key is absent.
 */

export type Locale = string

/** Normalize an arbitrary locale string to a stable lowercase tag. */
export function normalizeLocale(locale: string | undefined | null): string {
  const l = (locale ?? '').trim().toLowerCase()
  if (l === '') return 'en'
  // Accept underscores/upper as aliases: zh_CN / zh-CN / ZH -> zh-cn.
  return l.replace('_', '-')
}

/** Strip the region for a primary-language fallback. */
function primaryOf(tag: string): string {
  const i = tag.indexOf('-')
  return i === -1 ? tag : tag.slice(0, i)
}

/**
 * The language directive injected into every turn's `<env>` block, so the
 * model ALWAYS answers and reasons in the effective agent language regardless
 * of the preset's system prompt (built-in, host-injected, or user-authored).
 * Only the primary language matters; an unknown locale falls back to English.
 */
export function languageDirective(locale: string): string {
  switch (primaryOf(normalizeLocale(locale))) {
    case 'zh':
      return '无论用户使用什么语言，你都必须始终用中文回复，并用中文进行思考。'
    default:
      return 'Regardless of the language the user writes in, you must always reply and reason in English.'
  }
}

/**
 * Build the per-turn `<env>` block appended to the model's system prompt. It
 * ALWAYS carries the language directive, so the answer/reasoning language is
 * enforced for every preset regardless of its own prompt text.
 *
 * Pure (takes `now` explicitly) so it is unit-testable.
 */
export function buildEnvBlock(locale: string, now: Date): string {
  return [
    '<env>',
    `  Today's date: ${now.toISOString().slice(0, 10)}`,
    `  ${languageDirective(locale)}`,
    '</env>',
  ].join('\n')
}

/**
 * Resolve a localized string from a `Record<locale, value>` map.
 * Exact match → region fallback → null (so callers fall back to default).
 */
export function pickLocalized(
  map: Record<string, string> | undefined | null,
  locale: string,
): string | null {
  if (map == null) return null
  const l = normalizeLocale(locale)
  if (map[l] !== undefined) return map[l]
  const p = primaryOf(l)
  if (p !== l && map[p] !== undefined) return map[p]
  return null
}

/**
 * Resolve a tool/variable description: `descriptions[locale]` →
 * `descriptions[primary]` → `description` (the default/English fallback).
 */
export function pickDescription(
  description: string,
  descriptions: Record<string, string> | undefined | null,
  locale: string,
): string {
  return pickLocalized(descriptions, locale) ?? description
}

/**
 * Resolve a JSON-schema node's `descriptions` map the same way: exact →
 * primary-language fallback → null.
 */
function pickNodeDescription(
  node: Record<string, unknown>,
  locale: string,
): string | null {
  const d = node.descriptions
  if (d == null || typeof d !== 'object' || Array.isArray(d)) return null
  const map: Record<string, string> = {}
  for (const [k, v] of Object.entries(d)) {
    if (typeof v === 'string') map[k] = v
  }
  return pickLocalized(map, locale)
}

/**
 * Localize a tool's input schema in place-safe fashion: every node carrying
 * a `descriptions: Record<locale, string>` map (the same convention as
 * tool-level descriptions) gets its `description` resolved for the locale
 * and the `descriptions` key stripped, so the model only ever sees standard
 * JSON-Schema keys. Nodes without the map pass through untouched; a missing
 * locale entry falls back to the existing `description`.
 */
export function localizeSchema<T>(schema: T, locale: string): T {
  const walk = (value: unknown): unknown => {
    if (Array.isArray(value)) {
      return value.map(walk)
    }
    if (value === null || typeof value !== 'object') {
      return value
    }
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      if (k === 'descriptions') continue
      out[k] = walk(v)
    }
    const picked = pickNodeDescription(value as Record<string, unknown>, locale)
    if (picked !== null) out.description = picked
    return out
  }
  return walk(schema) as T
}

/** Resolve a locale from the effective preference chain. */
export function resolveLocale(
  sessionLocale: string | undefined | null,
  configLocale: string | undefined | null,
  envDefault: string,
): string {
  for (const v of [sessionLocale, configLocale]) {
    const n = normalizeLocale(v)
    if (n !== 'en' && n !== '') return n
    if (n === '' && v == null) continue
  }
  return normalizeLocale(envDefault)
}
