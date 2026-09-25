import { describe, expect, it } from 'vitest'
import {
  COMPACTION_ROLE,
  checkpointContent,
  type FoldEntry,
  foldQA,
  splitScan,
} from '../src/compaction.js'

function entry(
  id: string,
  role: string,
  text: string,
  toolCalls = 0,
  toolInputTokens = 0,
  toolResultTokens = 0,
): FoldEntry {
  return { id, role, text, toolCalls, toolInputTokens, toolResultTokens }
}

describe('foldQA', () => {
  it('folds user → assistant Q&A, dropping event messages', () => {
    const out = foldQA([
      entry('e1', 'event', 'system event text'),
      entry('u1', 'user', '请修 bug'),
      entry('a1', 'assistant', '', 3),
      entry('a2', 'assistant', '修复完成'),
      entry('u2', 'user', '再加个测试'),
      entry('a3', 'assistant', '好的'),
    ])
    expect(out).toBe(
      [
        'User: 请修 bug',
        'Assistant: [After 3 tool calls] 修复完成',
        'User: 再加个测试',
        'Assistant: [After 0 tool calls] 好的',
      ].join('\n\n'),
    )
  })

  it('concatenates multiple non-thinking text parts in one turn', () => {
    const out = foldQA([
      entry('u1', 'user', 'q'),
      entry('a1', 'assistant', '第一步', 1),
      entry('a2', 'assistant', '第二步', 2),
    ])
    expect(out).toContain('Assistant: [After 3 tool calls] 第一步\n第二步')
  })
})

describe('splitScan', () => {
  it('skips compaction messages entirely', () => {
    const entries = [
      entry('f1', 'user', '折叠的用户指令'),
      entry('f2', 'assistant', '折叠的回复', 2),
      entry('cm1', COMPACTION_ROLE, '', 0),
      entry('u1', 'user', '保留的用户指令'),
      entry('a1', 'assistant', '保留的回复'),
    ]
    const r = splitScan(entries, 4, 200)
    // Compaction message must not appear in tail or folded.
    expect(r.tail.map(e => e.id)).not.toContain('cm1')
    expect(r.folded.map(e => e.id)).not.toContain('cm1')
    expect(r.tail.map(e => e.id)).toContain('u1')
    expect(r.folded.map(e => e.id)).toContain('f1')
  })

  it('keeps everything in tail when within tail budget', () => {
    const entries = [entry('u1', 'user', 'hi'), entry('a1', 'assistant', 'ok')]
    const r = splitScan(entries, 10_000, 10_000)
    expect(r.tail.map(e => e.id)).toEqual(['u1', 'a1'])
    expect(r.folded).toEqual([])
  })

  it('aligns tail boundary to a user message', () => {
    const pad = 'x'.repeat(40)
    const entries = [
      entry('u1', 'user', `第一轮${pad}`),
      entry('a1', 'assistant', `回复一${pad}`, 2),
      entry('u2', 'user', `第二轮${pad}`),
      entry('a2', 'assistant', `回复二${pad}`, 1),
      entry('u3', 'user', `第三轮${pad}`),
      entry('a3', 'assistant', '回复三'),
    ]
    const r = splitScan(entries, 60, 60)
    expect(r.tail.length).toBeGreaterThan(0)
    expect(r.tail[0]!.role).toBe('user')
  })

  it('counts tool input/result weight so a tool-heavy turn folds (regression)', () => {
    // A turn whose TEXT is tiny but whose tool RESULTS are huge (the real bug:
    // 868k-context sessions measured ~40k and were declared "too short").
    const entries = [
      entry('u1', 'user', 'run it', 0, 0, 0),
      entry('a1', 'assistant', 'done', 5, 500, 50_000),
      entry('u2', 'user', 'again', 0, 0, 0),
      entry('a2', 'assistant', 'done', 3, 300, 40_000),
    ]
    // Small tail budget: the tool-heavy turn MUST land in `folded`, not be
    // silently ignored for having little text.
    const r = splitScan(entries, 100, 100)
    expect(r.folded.map(e => e.id)).toContain('a1')
    expect(r.folded.length).toBeGreaterThan(0)
  })
})

describe('foldQA tool-result trace', () => {
  it('keeps a truncated snippet of each tool result', () => {
    const long = 'Z'.repeat(500)
    const entries: FoldEntry[] = [
      {
        id: 'a1',
        role: 'assistant',
        text: 'working',
        toolCalls: 1,
        toolInputTokens: 10,
        toolResultTokens: 200,
        toolResultSnippets: [long],
      },
    ]
    const out = foldQA(entries)
    expect(out).toContain('Tool results:')
    // Truncated to the snippet cap (200 chars + ellipsis), not the full 500.
    expect(out).toContain('ZZZ')
    expect(out.length).toBeLessThan(400)
    expect(out).toContain('…')
  })

  it('omits the tool-result line when there are no snippets', () => {
    const entries: FoldEntry[] = [
      {
        id: 'a1',
        role: 'assistant',
        text: 'plain',
        toolCalls: 0,
        toolInputTokens: 0,
        toolResultTokens: 0,
      },
    ]
    expect(foldQA(entries)).not.toContain('Tool results:')
  })
})

describe('checkpointContent', () => {
  it('wraps summary in checkpoint tags', () => {
    const out = checkpointContent('hi')
    expect(out).toContain('<conversation-checkpoint>')
    expect(out).toContain('hi')
  })
})
