import { z } from 'zod'

const patterns = [
  /prompt is too long/i,
  /request_too_large/i,
  /input is too long for requested model/i,
  /exceeds the context window/i,
  /exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))/i,
  /input token count.*exceeds the maximum/i,
  /tokens in request more than max tokens allowed/i,
  /maximum prompt length is \d+/i,
  /reduce the length of the messages/i,
  /maximum context length is \d+ tokens/i,
  /exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?/i,
  /input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)/i,
  /exceeds the limit of \d+/i,
  /exceeds the available context size/i,
  /greater than the context length/i,
  /context window exceeds limit/i,
  /exceeded model token limit/i,
  /context[_ ]length[_ ]exceeded/i,
  /request entity too large/i,
  /context length is only \d+ tokens/i,
  /input length.*exceeds.*context length/i,
  /prompt too long; exceeded (?:max )?context length/i,
  /too large for model with \d+ maximum context length/i,
  /prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?/i,
  /model_context_window_exceeded/i,
  /too many tokens/i,
  /token limit exceeded/i,
  // Self-hosted gateway (api-gray.xueersi.com) wording: "Input exceeds the
  // context limit (N tokens). Please shorten the input." The generic table
  // above (ported from opencode) does not cover it, so it slipped through as a
  // hard turn failure instead of triggering overflow compaction.
  /input exceeds the context limit/i,
  /exceed(?:s|ed)? the model context limit/i,
]

const exclusions = [
  /^(throttling error|service unavailable):/i,
  /rate limit/i,
  /too many requests/i,
]

export const isContextOverflow = (message: string) =>
  !exclusions.some(pattern => pattern.test(message)) &&
  (patterns.some(pattern => pattern.test(message)) ||
    /^4(00|13)\s*(status code)?\s*\(no body\)/i.test(message))

const FailureShapeSchema = z.object({
  message: z.string().optional(),
  statusCode: z.number().optional(),
})

export const isContextOverflowFailure = (failure: unknown) => {
  if (failure === null || failure === undefined) return false
  if (typeof failure === 'string') return isContextOverflow(failure)
  const parsed = FailureShapeSchema.safeParse(failure)
  if (!parsed.success) return false
  const message = parsed.data.message
  if (message !== undefined && isContextOverflow(message)) return true
  // No message to match: fall back to status code only for the unambiguous
  // 413 (payload too large). A bare 400 is far more likely to be a model
  // config/auth error than a context overflow, so it is deliberately NOT
  // treated as overflow here — false positives trigger an irreversible
  // compaction of the session history.
  return parsed.data.statusCode === 413
}
