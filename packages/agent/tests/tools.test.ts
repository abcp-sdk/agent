import { describe, expect, it } from 'vitest'
import type { Bus } from '../src/bus.js'
import {
  buildAiTools,
  type DiscoveredTool,
  discoverToolsCached,
  invalidateDiscoveryCache,
  toolQualifiedName,
} from '../src/tools.js'

interface Published {
  subject: string
  payload: unknown
}

/** A Bus fake compatible with the request-based tool call path. */
function fakeBus() {
  const published: Published[] = []
  const log: string[] = []
  let resultFor: ((callId: string) => unknown) | null = null

  const bus = {
    request: (
      subject: string,
      payload: unknown,
      opts?: { sessionName?: string },
    ) => {
      log.push(`req:${subject}`)
      published.push({
        subject,
        payload: {
          ...(payload as Record<string, unknown>),
          session_name: opts?.sessionName ?? '',
        },
      })
      const callId = (payload as { call_id?: string }).call_id ?? ''
      const reply = resultFor
        ? resultFor(callId)
        : {
            call_id: callId,
            tool: 'read',
            content: 'ok',
            metadata: null,
          }
      return Promise.resolve({ payload: reply })
    },
  }
  return {
    bus: bus as unknown as Bus,
    published,
    log,
    replyWith: (fn: (callId: string) => unknown) => {
      resultFor = fn
    },
  }
}

const tool: DiscoveredTool = {
  extId: 'repo',
  name: 'read',
  description: 'read a file',
  inputSchema: { type: 'object', properties: { path: { type: 'string' } } },
}

const execOpts = (toolCallId: string) => ({ toolCallId, messages: [] }) as never

const T = 't1'

describe('buildAiTools session_name envelope', () => {
  it('carries the session envelope when sessionId given', async () => {
    const { bus, published } = fakeBus()
    const tools = buildAiTools([tool], bus, 500, T, 'acme--api--main')
    const result = await tools.read.execute({ path: 'x' }, execOpts('c1'))
    expect(result).toEqual({ content: 'ok', metadata: null })
    const call = published.find(p => p.subject === 'abc.t1.tool.call.repo.read')
    expect(call?.payload).toEqual({
      call_id: 'c1',
      session_name: 'acme--api--main',
      arguments: { path: 'x' },
    })
  })

  it('passes arguments unchanged when no sessionId', async () => {
    const { bus, published } = fakeBus()
    const tools = buildAiTools([tool], bus, 500, T)
    await tools.read.execute({ path: 'x' }, execOpts('c2'))
    const call = published.find(p => p.subject === 'abc.t1.tool.call.repo.read')
    expect(call?.payload).toEqual({
      call_id: 'c2',
      session_name: '',
      arguments: { path: 'x' },
    })
  })

  it('issues one request per call (reply routing is transport-internal)', async () => {
    const { bus, log } = fakeBus()
    const tools = buildAiTools([tool], bus, 500, T, 's')
    await tools.read.execute({}, execOpts('c3'))
    expect(log).toEqual(['req:abc.t1.tool.call.repo.read'])
  })

  it('maps the wire data field onto the canonical metadata', async () => {
    const { bus, replyWith } = fakeBus()
    replyWith(() => ({
      call_id: 'c4',
      tool: 'read',
      content: 'payload text',
      data: { rows: 3 },
    }))
    const tools = buildAiTools([tool], bus, 500, T, 's')
    const out = await tools.read.execute({}, execOpts('c4'))
    expect(out).toEqual({ content: 'payload text', metadata: { rows: 3 } })
  })
})
