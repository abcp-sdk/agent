import type { MessageRow, PartRow } from '@easylab-agent/schema'
import type { ModelMessage } from 'ai'
import {
  FilePartDataSchema,
  parse,
  TextPartDataSchema,
  ToolPartDataSchema,
  ToolResultPartDataSchema,
} from './json.js'

/**
 * Rebuild AI SDK ModelMessages from a session's persisted chain.
 *
 * Roles map to the AI SDK contract as follows:
 * - `user`     → user message
 * - `event`    → user message (external event folded into context)
 * - `assistant` → assistant message; its `text` parts become text content and
 *   its `tool`/`tool_result` parts become an assistant tool-call message
 *   followed by a `role:"tool"` message carrying every result (paired by
 *   `tool_use_id`).
 *
 * Text is read exclusively from `parts(type=text)`; `messages` has no inline
 * content column.
 */
export function rebuildHistory(
  rows: MessageRow[],
  parts: PartRow[],
): ModelMessage[] {
  const out: ModelMessage[] = []
  const byMessage = new Map<string, PartRow[]>()
  for (const p of parts) {
    const list = byMessage.get(p.message_id) ?? []
    list.push(p)
    byMessage.set(p.message_id, list)
  }

  for (const m of rows) {
    if (m.role === 'user' || m.role === 'event') {
      const parts = byMessage.get(m.id) ?? []
      const text = textOf(parts)
      const fileText = fileRefsText(parts)
      const content = [fileText, text].filter(Boolean).join('\n')
      if (content !== '') out.push({ role: 'user', content })
      continue
    }
    if (m.role !== 'assistant') continue

    const msgParts = (byMessage.get(m.id) ?? [])
      .slice()
      .sort((a, b) => a.seq - b.seq)
    const uses: Array<{ id: string; name: string; input: unknown }> = []
    const results: Array<{ toolUseId: string; content: string }> = []
    let text = ''
    for (const p of msgParts) {
      if (p.type === 'text') {
        const d = parse(TextPartDataSchema, p.data)
        if (d.isOk()) text += d.value.text
      } else if (p.type === 'tool') {
        const d = parse(ToolPartDataSchema, p.data)
        if (d.isOk()) {
          uses.push({
            id: d.value.id,
            name: d.value.name,
            input: d.value.input,
          })
        }
      } else if (p.type === 'tool_result') {
        const d = parse(ToolResultPartDataSchema, p.data)
        if (d.isOk()) {
          results.push({
            toolUseId: d.value.tool_use_id,
            content: d.value.content,
          })
        }
      }
    }

    if (uses.length === 0 && results.length === 0) {
      if (text !== '') out.push({ role: 'assistant', content: text })
      continue
    }

    const content: Array<
      | { type: 'text'; text: string }
      | {
          type: 'tool-call'
          toolCallId: string
          toolName: string
          input: unknown
        }
    > = []
    if (text !== '') content.push({ type: 'text', text })
    for (const u of uses) {
      content.push({
        type: 'tool-call',
        toolCallId: u.id,
        toolName: u.name,
        input: u.input ?? {},
      })
    }
    out.push({ role: 'assistant', content })
    if (results.length > 0) {
      out.push({
        role: 'tool',
        content: results.map(r => ({
          type: 'tool-result',
          toolCallId: r.toolUseId,
          toolName: uses.find(u => u.id === r.toolUseId)?.name ?? 'tool',
          output: { type: 'text', value: r.content },
        })),
      })
    }
  }
  return out
}

/** Concatenated text of a message's text parts, in seq order. */
function textOf(msgParts: PartRow[]): string {
  let text = ''
  for (const p of msgParts) {
    if (p.type !== 'text') continue
    const d = parse(TextPartDataSchema, p.data)
    if (d.isOk()) text += d.value.text
  }
  return text
}

/** Render a message's `file` parts as attachment reference lines. */
function fileRefsText(msgParts: PartRow[]): string {
  let s = ''
  for (const p of msgParts) {
    if (p.type !== 'file') continue
    const d = parse(FilePartDataSchema, p.data)
    if (!d.isOk()) continue
    const f = d.value
    const name = f.name ?? f.code
    const meta = [f.mime, f.size != null ? `${f.size} B` : null]
      .filter(Boolean)
      .join(' | ')
    const ref = `[附件 ${name} | file:${f.code}` + (meta ? ` | ${meta}]` : ']')
    s += (s === '' ? '' : '\n') + ref
  }
  return s
}
