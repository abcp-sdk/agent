import type { ModelMessage } from 'ai'
import { describe, expect, it } from 'vitest'
import { appendStep } from '../src/turn-persist.js'

/**
 * `appendStep` builds the in-memory history for the NEXT step of a turn. The
 * assistant tool-calls and the following `role:"tool"` results MUST pair
 * one-to-one: the AI SDK's standardizePrompt throws `MissingToolResultsError`
 * when a tool-call has no result, which would abort the next step. A truncated
 * step (`finish_reason:"length"`) emits tool-calls the SDK never executed, so
 * appendStep must synthesize a placeholder for each — never leave a dangling
 * call.
 */
describe('appendStep tool-call/result pairing', () => {
  const call = (id: string, name = 'subsession-create') => ({
    id,
    name,
    input: { x: 1 },
  })
  const result = (id: string, content: string, name = 'subsession-create') => ({
    id,
    name,
    result: { content, metadata: null },
  })

  function toolMsg(msgs: ModelMessage[]): {
    content: Array<{ toolCallId: string; output?: unknown }>
  } {
    const m = msgs.find(x => x.role === 'tool')
    if (m === undefined || typeof m.content === 'string') {
      throw new Error('no tool message')
    }
    return m as unknown as {
      content: Array<{ toolCallId: string; output?: unknown }>
    }
  }

  it('pairs every call with its result when all results are present', () => {
    const out = appendStep(
      [],
      'hi',
      [call('a'), call('b')],
      [result('a', 'A'), result('b', 'B')],
    )
    const tool = toolMsg(out)
    expect(tool.content.map(c => c.toolCallId)).toEqual(['a', 'b'])
  })

  it('synthesizes a placeholder for a call with NO result (truncated step)', () => {
    // Only 'a' executed; 'b' was emitted but never ran (finish_reason:length).
    const out = appendStep([], 'hi', [call('a'), call('b')], [result('a', 'A')])
    const tool = toolMsg(out)
    // Exactly one result per call, in call order — no dangling call.
    expect(tool.content.map(c => c.toolCallId)).toEqual(['a', 'b'])
    const b = tool.content.find(c => c.toolCallId === 'b')!
    expect(JSON.stringify(b.output)).toContain('produced no output')
  })

  it('emits a tool message even when NO result is present', () => {
    const out = appendStep([], '', [call('a'), call('b'), call('c')], [])
    const tool = toolMsg(out)
    expect(tool.content).toHaveLength(3)
    expect(
      tool.content.every(c =>
        JSON.stringify(c.output).includes('produced no output'),
      ),
    ).toBe(true)
  })

  it('emits NO tool message when the step has no tool calls', () => {
    const out = appendStep([], 'just text', [], [])
    expect(out.some(m => m.role === 'tool')).toBe(false)
    expect(out[0]).toEqual({
      role: 'assistant',
      content: [{ type: 'text', text: 'just text' }],
    })
  })
})
