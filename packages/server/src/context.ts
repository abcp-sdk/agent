import { randomUUID } from 'node:crypto'
import type { AgentDeps, LlmRegistry } from '@easylab-agent/agent'

/**
 * Bounded `eid` dedup for the SSE replay/live handover: replayed events are
 * marked, then live events carrying a seen eid are dropped once. Eviction
 * only happens on `mark` (replay path); live insertions may transiently
 * exceed the cap so a just-marked replay eid is never evicted by a live one.
 */
export class EidDedup {
  private readonly seen = new Set<string>()
  private readonly order: string[] = []

  constructor(private readonly cap = 4096) {}

  private evict(): void {
    while (this.order.length > this.cap) {
      const old = this.order.shift()
      if (old !== undefined) this.seen.delete(old)
    }
  }

  mark(eid: string | undefined): void {
    if (eid === undefined || this.seen.has(eid)) return
    this.seen.add(eid)
    this.order.push(eid)
    this.evict()
  }

  /** True when the eid was already seen (and is now marked). */
  duplicate(eid: string | undefined): boolean {
    if (eid === undefined) return false
    if (this.seen.has(eid)) return true
    this.seen.add(eid)
    this.order.push(eid)
    // Soft cap: trim eventually, but never below the hard cap's worth of
    // freshly marked ids.
    while (this.order.length > this.cap * 2) {
      const old = this.order.shift()
      if (old !== undefined) this.seen.delete(old)
    }
    return false
  }
}

export function newEid(): string {
  return randomUUID()
}

export type { AgentDeps, LlmRegistry }
