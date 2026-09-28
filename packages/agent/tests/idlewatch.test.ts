import { describe, expect, it } from 'vitest'
import { endsOnToolResult, nudgeText } from '../src/idlewatch.js'

describe('endsOnToolResult', () => {
  const asst = (content: string, toolParts: unknown[] = []) => ({
    role: 'assistant',
    content,
    tool_parts: toolParts,
  })

  it('is false for an empty chain', () => {
    expect(endsOnToolResult([])).toBe(false)
  })

  it('is false when the tip is a user message', () => {
    expect(
      endsOnToolResult([{ role: 'user', content: 'hi', tool_parts: [] }]),
    ).toBe(false)
  })

  it('is false when the assistant step has no tool parts', () => {
    expect(endsOnToolResult([asst('done')])).toBe(false)
  })

  it('is false when the assistant step ends with trailing text', () => {
    expect(endsOnToolResult([asst('here is the result', [{}])])).toBe(false)
  })

  it('is true when the tip is an assistant step ending on a tool result', () => {
    expect(endsOnToolResult([asst('', [{}, {}])])).toBe(true)
    // whitespace-only text counts as "no text"
    expect(endsOnToolResult([asst('   \n', [{}])])).toBe(true)
  })

  it('only inspects the TIP, not earlier steps', () => {
    expect(
      endsOnToolResult([
        asst('', [{}]), // earlier dangling tool
        asst('finished with a summary'), // tip with text
      ]),
    ).toBe(false)
  })
})

describe('nudgeText', () => {
  it('localizes to zh for a zh locale prefix', () => {
    expect(nudgeText('zh')).toContain('工具调用')
    expect(nudgeText('zh-CN')).toContain('工具调用')
  })

  it('uses English otherwise', () => {
    expect(nudgeText('en')).toContain('stopped after a tool call')
    expect(nudgeText('')).toContain('stopped after a tool call')
  })
})
