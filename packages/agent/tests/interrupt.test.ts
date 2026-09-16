import { describe, expect, it } from 'vitest'
import {
  clearRun,
  getAbortController,
  interruptRun,
  isAborted,
} from '../src/interrupt.js'

describe('interrupt controllers are tenant-scoped', () => {
  it('same-named sessions in different tenants get independent controllers', () => {
    const a = getAbortController('tenant-a', 'test')
    const b = getAbortController('tenant-b', 'test')
    expect(a).not.toBe(b)
    // Interrupting tenant-a's run must NOT abort tenant-b's same-named run
    // (regression: keys were session-only, so a delete/interrupt in one
    // tenant aborted the other tenant's in-flight turn).
    interruptRun('tenant-a', 'test')
    expect(a.signal.aborted).toBe(true)
    expect(b.signal.aborted).toBe(false)
    expect(isAborted('tenant-b', 'test')).toBe(false)
  })

  it('interruptRun for an unknown tenant/session is a no-op', () => {
    expect(() => interruptRun('nope', 'nope')).not.toThrow()
    expect(isAborted('nope', 'nope')).toBe(false)
  })

  it('clearRun removes only the scoped entry', () => {
    getAbortController('t1', 's')
    getAbortController('t2', 's')
    clearRun('t1', 's')
    interruptRun('t1', 's')
    expect(isAborted('t1', 's')).toBe(false)
    expect(isAborted('t2', 's')).toBe(false)
    // t2's controller is still live (fresh, un-aborted).
    expect(getAbortController('t2', 's').signal.aborted).toBe(false)
  })

  it('a new controller replaces an aborted one for the same key', () => {
    const first = getAbortController('t', 's')
    interruptRun('t', 's')
    const second = getAbortController('t', 's')
    expect(second).not.toBe(first)
    expect(second.signal.aborted).toBe(false)
  })
})
