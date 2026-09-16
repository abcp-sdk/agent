/**
 * Estimated token count — deliberately NOT a real tokenizer. A cheap,
 * deterministic heuristic: CJK chars count as ~1.5 chars/token, everything
 * else ~4 chars/token. Good enough for compaction budget gating.
 */

const CJK_RE =
  /[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff00-\uffef]/

export function estimateTokens(text: string): number {
  let cjk = 0
  let other = 0
  for (const ch of text) {
    if (CJK_RE.test(ch)) cjk++
    else other++
  }
  return Math.ceil(cjk / 1.5) + Math.ceil(other / 4)
}

/**
 * Display mask for a stored secret (provider api keys). Long secrets show a
 * recognizable `<first4>****<last4>` prefix/suffix; short ones mask fully so
 * nothing usable leaks. The masked value doubles as the EDIT-TIME SENTINEL:
 * `registerProvider`/`testProvider` treat an incoming key equal to the
 * currently stored mask as "unchanged" and keep the stored secret, so a
 * client that lists providers, prefills the form and saves without touching
 * the key cannot corrupt it.
 */
export function maskSecret(secret: string): string {
  if (secret.length <= 8) return '****'
  return `${secret.slice(0, 4)}****${secret.slice(-4)}`
}
