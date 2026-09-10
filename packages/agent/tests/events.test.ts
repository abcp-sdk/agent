import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import { publishLifecycle, publishSessionChanged } from '../src/events.js'

function fakeBus() {
  const published: Array<{ subject: string; payload: unknown }> = []
  const bus = {
    inboxPublish: (subject: string, payload: unknown) => {
      published.push({ subject, payload })
      return Promise.resolve()
    },
    publish: (subject: string, payload: unknown) => {
      published.push({ subject, payload })
      return Promise.resolve()
    },
  }
  return { bus: bus as unknown as Bus, published }
}

describe('publishLifecycle', () => {
  it('publishes to abc.session.lifecycle.{event} with kind in payload', async () => {
    const { bus, published } = fakeBus()
    publishLifecycle(bus, 'created', { session_name: 'acme.api.main' })
    publishLifecycle(bus, 'forked', {
      session_name: 'acme.api.feat',
      parent: 'acme.api.main',
    })
    publishLifecycle(bus, 'renamed', { from: 'a.b.c', to: 'a.b.d' })
    publishLifecycle(bus, 'deleted', { session_name: 'a.b.c' })
    await new Promise(r => setImmediate(r))

    expect(published.map(p => p.subject)).toEqual([
      'abc.session.lifecycle.created',
      'abc.session.lifecycle.forked',
      'abc.session.lifecycle.renamed',
      'abc.session.lifecycle.deleted',
    ])
    expect(published[1]?.payload).toEqual({
      kind: 'forked',
      session_name: 'acme.api.feat',
      parent: 'acme.api.main',
    })
  })

  it('swallows publish errors (best-effort trigger hook)', async () => {
    const bus = {
      inboxPublish: () => Promise.reject(new Error('nats down')),
    } as unknown as Bus
    expect(() =>
      publishLifecycle(bus, 'deleted', { session_name: 'x' }),
    ).not.toThrow()
    await new Promise(r => setImmediate(r))
  })

  it('publishes session-changed nudges on the live pub subject', async () => {
    const { bus, published } = fakeBus()
    publishSessionChanged(bus, 'sess-a')
    await new Promise(r => setImmediate(r))
    expect(published).toEqual([
      { subject: 'abc.session.changed', payload: { session_name: 'sess-a' } },
    ])
  })
})
