import { Agent as AbcAgent } from '@abc-protocol/sdk'
import type { Bus } from './bus.js'
import { BUCKET_SESSION_TURN, natsToken, tenantKVKey } from './bus.js'
import type { ServerConfig } from './config.js'
import type { Db } from './db-client.js'
import { Messages } from './db-messages.js'
import { knownTenants, Sessions } from './db-sessions.js'
import { logger } from './logger.js'
import { readSessionStatuses } from './session-state.js'

/**
 * Idle-turn watchdog: re-trigger a session that stopped mid-task after a tool
 * call.
 *
 * Every sweep it walks each tenant's sessions and, for a session that is
 *   - IDLE (no run lease held), AND
 *   - whose LATEST message is an assistant step ending on a `tool_result`
 *     (the model executed a tool and then produced no text — it stopped),
 *   - and whose last turn was NOT ended by a user interrupt,
 *   - and which has a model configured (otherwise the nudge can only fail),
 *
 * publishes a `trigger` into the session's mailbox (source `system:idlewatch`)
 * telling the model to continue or wrap up.
 *
 * Ported from the workspace-gateway's `internal/idlewatch` so the behaviour
 * lives with the agent (single source of truth) and is gated by config
 * (`IDLEWATCH_ENABLED` / `IDLEWATCH_INTERVAL`). The gateway copy can then be
 * disabled.
 *
 * Safety: a session whose tip does NOT advance across consecutive nudges is
 * backed off (the nudge is not helping — e.g. the provider is rate-limiting or
 * the turn keeps failing). The counter resets as soon as the tip changes, so a
 * genuinely-progressing session is never throttled.
 */

/** Max consecutive no-progress nudges before a session is skipped. */
const MAX_NO_PROGRESS_NUDGES = 3

export interface IdleWatchDeps {
  db: Db
  bus: Bus
  config: ServerConfig
  /** Injectable clock (tests). */
  now?: () => number
}

/** Per-session nudge bookkeeping (in-memory; resets on restart). */
interface NudgeState {
  /** Tip observed at the last nudge. */
  tip: string
  /** Consecutive nudges where the tip did NOT advance. */
  noProgress: number
}

export class IdleWatchdog {
  private timer: ReturnType<typeof setInterval> | null = null
  private running = false
  private readonly nudges = new Map<string, NudgeState>()
  private readonly now: () => number

  constructor(private readonly deps: IdleWatchDeps) {
    this.now = deps.now ?? (() => Date.now())
  }

  /** Start the sweep loop. No-op when disabled. */
  start(): void {
    if (!this.deps.config.idlewatchEnabled) {
      logger.info('idlewatch: disabled (IDLEWATCH_ENABLED=false)')
      return
    }
    const interval = this.deps.config.idlewatchIntervalMs
    logger.info({ intervalMs: interval }, 'idlewatch: started')
    this.timer = setInterval(() => {
      void this.sweepOnce().catch(err => {
        logger.warn({ err: String(err) }, 'idlewatch: sweep failed')
      })
    }, interval)
    this.timer.unref()
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer)
      this.timer = null
    }
  }

  /** One pass over every tenant's sessions. Exposed for tests. */
  async sweepOnce(): Promise<void> {
    if (this.running) return
    this.running = true
    try {
      const tenantsRes = await knownTenants(this.deps.db, 'default')
      if (tenantsRes.isErr()) {
        logger.warn({ err: tenantsRes.error }, 'idlewatch: list tenants failed')
        return
      }
      for (const tenant of tenantsRes.value) {
        const listRes = await Sessions.list(this.deps.db, tenant)
        if (listRes.isErr()) {
          logger.warn(
            { tenant, err: listRes.error },
            'idlewatch: list sessions failed',
          )
          continue
        }
        const sessions = listRes.value
        const statuses = await readSessionStatuses(
          this.deps.bus,
          tenant,
          sessions.map(s => s.name),
        )
        for (const s of sessions) {
          try {
            await this.maybeNudge(tenant, s, statuses.get(s.name) ?? 'idle')
          } catch (err) {
            logger.warn(
              { tenant, sid: s.name, err: String(err) },
              'idlewatch: maybeNudge failed',
            )
          }
        }
      }
    } finally {
      this.running = false
    }
  }

  private async maybeNudge(
    tenant: string,
    s: { name: string; model: string; tip_id: string | null; locale: string },
    status: 'busy' | 'idle',
  ): Promise<void> {
    const name = s.name
    if (status !== 'idle') return
    // No model ⇒ a turn can only fail; nudging would just churn.
    if (s.model === '') return
    const tip = s.tip_id ?? ''
    if (tip === '') return

    const chainRes = await Messages.chain(this.deps.db, tenant, tip, 1, null)
    if (chainRes.isErr()) return
    if (!endsOnToolResult(chainRes.value)) return

    if (await this.turnInterrupted(tenant, name)) return

    // No-progress backoff: skip when the tip has not moved across the last
    // few nudges (the nudge is not helping — e.g. provider rate-limiting).
    const prev = this.nudges.get(name)
    if (prev !== undefined && prev.tip === tip) {
      if (prev.noProgress >= MAX_NO_PROGRESS_NUDGES) return
    }

    const locale = await this.effectiveLocale(tenant, name, s.locale)
    await new AbcAgent(this.deps.bus).publishMailbox(
      tenant,
      name,
      'trigger',
      { text: nudgeText(locale) },
      'system:idlewatch',
    )
    const noProgress =
      prev !== undefined && prev.tip === tip ? prev.noProgress + 1 : 0
    this.nudges.set(name, { tip, noProgress })
    logger.info(
      { tenant, sid: name, noProgress },
      'idlewatch: re-triggered (stopped after a tool call)',
    )
  }

  /** True when the session's LAST turn was ended by a user interrupt. A
   *  missing/unreadable marker is treated as "not interrupted". */
  private async turnInterrupted(
    tenant: string,
    session: string,
  ): Promise<boolean> {
    const raw = await this.deps.bus
      .kvGet(BUCKET_SESSION_TURN, tenantKVKey(tenant, natsToken(session)))
      .catch(() => null)
    if (raw === null || raw === '') return false
    try {
      const marker = JSON.parse(raw) as { reason?: string }
      return marker.reason === 'interrupted'
    } catch {
      return false
    }
  }

  /** Prefer the session's projected `vars.agent.locale`, then the row, then
   *  "en". */
  private async effectiveLocale(
    tenant: string,
    session: string,
    sessionLocale: string,
  ): Promise<string> {
    const raw = await this.deps.bus
      .kvGet('vars', tenantKVKey(tenant, `agent.${natsToken(session)}.locale`))
      .catch(() => null)
    if (raw !== null && raw !== '') return raw
    return sessionLocale !== '' ? sessionLocale : 'en'
  }
}

/**
 * True when the newest message is an assistant step whose last content is a
 * tool result (no trailing text) — the "ran a tool then stopped" shape. A
 * normal finish leaves a trailing text part (`content !== ''`).
 */
export function endsOnToolResult(
  msgs: ReadonlyArray<{ role: string; content: string; tool_parts: unknown[] }>,
): boolean {
  if (msgs.length === 0) return false
  const last = msgs[msgs.length - 1]
  if (last === undefined || last.role !== 'assistant') return false
  if (last.tool_parts.length === 0) return false
  // Trailing text ⇒ the model finished with a summary, not a dangling tool.
  return last.content.trim() === ''
}

/** The mailbox nudge body, localized by the session's effective locale. */
export function nudgeText(locale: string): string {
  if (locale.toLowerCase().startsWith('zh')) {
    return '你在一次工具调用之后停止了，没有继续。请继续完成你的任务；若已完成，请用文本说明结果并收尾。'
  }
  return 'You stopped after a tool call without continuing. Please carry on with your task; if you are done, respond with a text summary to finish.'
}
