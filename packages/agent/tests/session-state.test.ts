import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import {
  factFromPersist,
  projectMessageFact,
  readMessageFacts,
} from '../src/session-state.js'

/** In-memory bus stub exposing just the KV + publish surface used here. */
function fakeBus() {
  const kv = new Map<string, string>()
  const published: string[] = []
  const bus = {
    kvCreate: () => Promise.resolve(1),
    kvGet: (_bucket: string, key: string) => Promise.resolve(kv.get(key) ?? null),
    kvPut: (_bucket: string, key: string, value: string) => {
      kv.set(key, value)
      return Promise.resolve()
    },
    publish: (subject: string) => {
      published.push(subject)
      return Promise.resolve()
    },
    kvDelete: (_bucket: string, key: string) => {
      kv.delete(key)
      return Promise.resolve()
    },
  }
  return { bus: bus as unknown as Bus, kv, published }
}

const settle = () => new Promise(r => setImmediate(r))

describe('projectMessageFact', () => {
  it('bumps message_seq monotonically and mirrors the preview', async () => {
    const { bus } = fakeBus()
    projectMessageFact(
      bus,
      'sess-seq-a',
      factFromPersist('2026-01-01T00:00:00Z', 'user', 'hello'),
    )
    await settle()
    projectMessageFact(
      bus,
      'sess-seq-a',
      factFromPersist('2026-01-01T00:00:01Z', 'assistant', 'hi there'),
    )
    await settle()

    const facts = await readMessageFacts(bus, ['sess-seq-a'])
    const f = facts.get('sess-seq-a')
    expect(f?.message_seq).toBe(2)
    expect(f?.last_message_role).toBe('assistant')
    expect(f?.last_message_preview).toBe('hi there')
    expect(f?.session_name).toBe('sess-seq-a')
  })

  it('keeps concurrent bumps for one session distinct', async () => {
    const { bus } = fakeBus()
    const sid = 'sess-seq-b'
    for (let i = 0; i < 5; i++) {
      projectMessageFact(
        bus,
        sid,
        factFromPersist('2026-01-01T00:00:00Z', 'assistant', `m${i}`),
      )
    }
    await settle()
    await settle()
    const facts = await readMessageFacts(bus, [sid])
    expect(facts.get(sid)?.message_seq).toBe(5)
  })

  it('does not double-signal: the KV watch is the only list trigger', async () => {
    const { bus, published } = fakeBus()
    projectMessageFact(
      bus,
      'sess-seq-c',
      factFromPersist('2026-01-01T00:00:00Z', 'user', 'x'),
    )
    await settle()
    // The list watcher lives on the KV bucket; no extra pub (which would
    // yield a duplicate upsert per message).
    expect(published).not.toContain('abc.session.changed')
  })
})
