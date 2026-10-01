import { okAsync } from 'neverthrow'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { Bus } from '../src/bus.js'

/**
 * Regression tests for the mid-flight-killed-turn resume (the 2026-10-01
 * incident): the agent container was restarted by its liveness probe while
 * several turns were running. Those turns never wrote a turn-end fact, so
 * the durable fact still described an OLDER turn — including a stale
 * `interrupted` reason, which made idlewatch refuse to resume them, and the
 * dangling tips carried trailing text (narration before the tool call),
 * which the old `endsOnToolResult` shape check rejected.
 *
 * The sweep is driven end-to-end with the db/session-state surfaces mocked,
 * so these cover the DECISION, not the storage.
 */

const chainMock = vi.fn()
const isInChainMock = vi.fn()
const listMock = vi.fn()
const knownTenantsMock = vi.fn()
const factsMock = vi.fn()
const statusesMock = vi.fn()

vi.mock('../src/db-messages.js', () => ({
  Messages: {
    chain: (...a: unknown[]) => chainMock(...a),
    isInChain: (...a: unknown[]) => isInChainMock(...a),
  },
}))
vi.mock('../src/db-sessions.js', () => ({
  Sessions: { list: (...a: unknown[]) => listMock(...a) },
  knownTenants: (...a: unknown[]) => knownTenantsMock(...a),
}))
vi.mock('../src/session-state.js', () => ({
  readMessageFacts: (...a: unknown[]) => factsMock(...a),
  readSessionStatuses: (...a: unknown[]) => statusesMock(...a),
}))

const { IdleWatchdog } = await import('../src/idlewatch.js')

const T = 't1'
const SID = 'org:repo:main'

/** Fake bus: records inboxPublish (the nudge); kvGet serves nothing. */
function fakeBus() {
  const published: Array<{ subject: string; payload: unknown }> = []
  const bus = {
    inboxPublish: (subject: string, payload: unknown) => {
      published.push({ subject, payload })
      return Promise.resolve()
    },
    kvGet: () => Promise.resolve(null),
  }
  return { bus: bus as unknown as Bus, published }
}

function watchdog(deps: { db?: unknown; bus: Bus }) {
  return new IdleWatchdog({
    db: (deps.db ?? {}) as never,
    bus: deps.bus,
    config: {
      idlewatchEnabled: true,
      idlewatchIntervalMs: 120_000,
      resumeDanglingOnBoot: false,
    } as never,
  })
}

/** Wire every mock for one session whose tip is an assistant tool step. */
function setup(opts: {
  content: string
  factTip: string
  factReason?: string
  factReachable?: boolean
}) {
  const tip = 'tip-now'
  knownTenantsMock.mockReturnValue(okAsync([T]))
  listMock.mockReturnValue(
    okAsync([{ name: SID, model: 'm', tip_id: tip, locale: '' }]),
  )
  statusesMock.mockResolvedValue(new Map([[SID, 'idle']]))
  chainMock.mockReturnValue(
    okAsync([
      {
        id: tip,
        role: 'assistant',
        content: opts.content,
        tool_parts: [
          { type: 'tool', name: 'sandbox-exec', input: {}, result: '' },
        ],
      },
    ] as never),
  )
  factsMock.mockResolvedValue(
    new Map([
      [
        SID,
        {
          tenant: T,
          session_name: SID,
          last_turn_tip: opts.factTip,
          ...(opts.factReason !== undefined
            ? { last_turn_reason: opts.factReason }
            : {}),
        },
      ],
    ]),
  )
  isInChainMock.mockReturnValue(okAsync(opts.factReachable ?? false))
}

beforeEach(() => {
  vi.clearAllMocks()
})

describe('idlewatch resume of a turn killed mid-flight', () => {
  it('resumes when the chain advanced past the recorded turn end, even with trailing text and a stale interrupted fact', async () => {
    // The incident shape: text narration + tool_result, and the durable fact
    // still holds the PREVIOUS turn's `interrupted` outcome.
    setup({
      content: '修好了 smart.go，继续单测',
      factTip: 'tip-old',
      factReason: 'interrupted',
      factReachable: true,
    })
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(1)
    expect(published[0]!.payload).toMatchObject({
      type: 'trigger',
      source: 'system:idlewatch',
    })
  })

  it('does not resume when the tip moved BACKWARDS (an undo withdrew the recorded end)', async () => {
    // factTip is a DESCENDANT of the tip: not reachable walking prev_id.
    setup({
      content: 'some narration',
      factTip: 'tip-future',
      factReachable: false,
    })
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(0)
  })

  it('does not resume a turn that cleanly ended at this tip with trailing text', async () => {
    setup({
      content: 'all done, merged the MR',
      factTip: 'tip-now',
      factReason: 'stop',
    })
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(0)
  })

  it('does not resume a USER interrupt recorded at this tip', async () => {
    // The classic shape (tool call, no text) with reason=interrupted at the
    // current tip: the user stopped it, respect that.
    setup({ content: '', factTip: 'tip-now', factReason: 'interrupted' })
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(0)
  })

  it('still resumes the classic model-stopped-after-a-tool-call', async () => {
    setup({ content: '', factTip: 'tip-now', factReason: 'stop' })
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(1)
  })

  it('does not resume a session with no recorded turn end yet (fork / first turn)', async () => {
    // factTip absent and trailing text present: too little evidence.
    setup({ content: 'partial narration', factTip: '' })
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(0)
  })

  it('ignores a user message tip (a prompt awaiting its turn)', async () => {
    knownTenantsMock.mockReturnValue(okAsync([T]))
    listMock.mockReturnValue(
      okAsync([{ name: SID, model: 'm', tip_id: 'tip-now', locale: '' }]),
    )
    statusesMock.mockResolvedValue(new Map([[SID, 'idle']]))
    chainMock.mockReturnValue(
      okAsync([
        {
          id: 'tip-now',
          role: 'user',
          content: 'please continue',
          tool_parts: [],
        },
      ] as never),
    )
    factsMock.mockResolvedValue(new Map())
    const { bus, published } = fakeBus()
    await watchdog({ bus }).sweepOnce()
    expect(published).toHaveLength(0)
  })
})
