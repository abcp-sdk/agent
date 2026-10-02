import { Agent as AbcAgent } from '@abc-protocol/sdk'
import {
  type AgentDeps,
  BUCKET_SESSION_STATE,
  LEASE_BUCKET,
  natsToken,
  OWNER_BUCKET,
  readActiveRun,
  readLeaseOwner,
  readMessageFacts,
  readSessionStatus,
  readSessionStatuses,
  Sessions,
} from '@abcp-agent/agent'
import {
  type AgentService,
  type WatchSessionResponse,
  WatchSessionResponseSchema,
  type WatchSessionsResponse,
  WatchSessionsResponseSchema,
} from '@abcp-agent/schema'
import { create } from '@bufbuild/protobuf'
import type { HandlerContext, ServiceImpl } from '@connectrpc/connect'
import { EidDedup } from '../context.js'
import { fieldString, isRecord, toJsonObject } from '../proto-json.js'
import { tenantOf } from '../tenant.js'
import { sessionToMsg } from '../views.js'

/**
 * Server-streaming watch handlers: watchSession and watchSessions.
 */

export function watchHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async *watchSession(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { id, sinceSeq } = req
      const agent = new AbcAgent(deps.bus)
      // Resume is by STREAM SEQUENCE (no message-id / wall-clock anchor, no
      // O(n) time scan, no re-replay):
      //   - sinceSeq > 0 -> resume the ordered consumer AFTER that sequence
      //     (O(1) by_start_sequence) so a reconnecting client gets exactly the
      //     events it missed while offline;
      //   - sinceSeq == 0 -> live-from-now, EXCEPT while a run is active, where
      //     we surface that run's events from its start (an in-progress turn is
      //     not otherwise replayable).
      // No DB anchor is needed: a turn that COMPLETED while the client was
      // offline is reconciled by ListMessages (the client re-pulls the message
      // chain), so the event stream only has to carry live/in-progress deltas.
      const activeRun = await readActiveRun(deps.bus, tenant, id)
      // `sinceSeq` arrives as an int64 -> bigint; the SDK's `startSeq` is a
      // number. `by_start_sequence` is INCLUSIVE, so resume AFTER the last seen
      // event (sinceSeq + 1) for an exact continuation with no duplicate
      // delivery. A sequence beyond Number.MAX_SAFE_INTEGER is unrepresentable
      // here; it is astronomically far off, so fall back to live-from-now.
      const sinceSeqNum =
        sinceSeq !== undefined && sinceSeq > 0n ? Number(sinceSeq) : 0
      const startSeq =
        sinceSeqNum > 0 && Number.isSafeInteger(sinceSeqNum + 1)
          ? sinceSeqNum + 1
          : undefined
      const startTimeMs: number | undefined =
        startSeq === undefined && activeRun !== null
          ? activeRun.startedAtMs
          : undefined
      // Live runs we are allowed to surface. Seeded with the turn active at
      // subscription time; a NEW run is adopted the moment its `status:busy`
      // arrives. This matters because a mailbox-drained prompt CONTINUES the
      // session as a fresh run after the previous turn ends — with a static
      // snapshot those run-B events were filtered out and the client hung until
      // a manual refresh re-subscribed.
      const liveRuns = new Set<string>()
      if (activeRun !== null) liveRuns.add(activeRun.runId)
      const dedup = new EidDedup()

      // Seed the client with the AUTHORITATIVE current status before the stream.
      // The per-session stream otherwise carries only TRANSIENT `status`
      // events, so a client that subscribes while a turn is already running
      // (or that locally reset its busy flag during a retry/revert) could stay
      // grey until the next transition — diverging from the session LIST,
      // which reads the run lease every frame. This snapshot is synthesized
      // here (NOT replayed from the bus), so the live-run filter never drops
      // it; `snapshot: true` tells the client it is a state seed, not a run
      // boundary (no streaming-state reset), and an empty `eid` keeps it out
      // of the dedup set. `unknown` (a lease read error) is passed through so
      // the client renders the same "no/ambiguous badge" as the list.
      const seedStatus = await readSessionStatus(deps.bus, tenant, id)
      yield create(WatchSessionResponseSchema, {
        event: 'status',
        params: toJsonObject({
          type: seedStatus,
          snapshot: true,
          ...(activeRun !== null ? { run_id: activeRun.runId } : {}),
        }),
        eid: '',
      })
      for await (const raw of agent.streamEvents(tenant, id, {
        ...(startSeq !== undefined ? { startSeq } : {}),
        ...(startTimeMs !== undefined ? { startTimeMs } : {}),
        signal: ctx.signal,
      })) {
        // Adopt a newly-started turn. `status:busy` is emitted at the top of
        // every run with its run_id, so this catches run B without polling.
        const rawRunId = fieldString(raw?.params, 'run_id') ?? ''
        if (
          raw?.event === 'status' &&
          fieldString(raw?.params, 'type') === 'busy' &&
          rawRunId !== ''
        ) {
          liveRuns.add(rawRunId)
        }
        // Only the live runs' events (a prior turn's terminal marker may fall
        // inside the same time window); never resurface finished/revoked runs.
        // EXEMPT out-of-band chain notifications (chain-changed,
        // message-added, compacted): they carry no run_id and MUST reach
        // viewers even while a turn is running, so a revert / a
        // freshly-appended user message / an overflow compaction converges.
        const isOutOfBand =
          raw?.event === 'chain-changed' ||
          raw?.event === 'message-added' ||
          raw?.event === 'compacted'
        if (!isOutOfBand && liveRuns.size > 0 && !liveRuns.has(rawRunId)) {
          continue
        }
        const eid = fieldString(raw, 'eid')
        if (dedup.duplicate(eid)) continue
        yield toWatchEvent(raw)
      }
    },

    /**
     * Real-time session-list stream. Emits an initial full snapshot, then a
     * stream of per-session upserts and removals. Driven by three sources:
     * the message-fact KV (`abc-session-meta`) for previews/seq, the
     * lifecycle subject for created/forked/renamed/deleted, and the
     * `abc.session.changed` pub for settings edits. Watchers are best-effort
     * nudged and refetch the affected session from the DB + KV, so a missed
     * nudge only delays that session's row until the next event.
     */
    async *watchSessions(_req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      // NOTE: the lifecycle/settings watchers below subscribe with
      // `deliver_policy: new` (no time anchor) and the full DB snapshot is taken
      // AFTER they are live. A change published before a watcher subscribed is
      // already reflected in the snapshot; a change after it is delivered by
      // the watcher; the (idempotent) overlap is harmless. This avoids the
      // former `by_start_time` anchor, which made every (re)subscribe scan the
      // stream from the start.
      // Runtime status per session (busy/idle), from the run lease. Seeded from
      // the initial snapshot and updated by the lease watcher; only a CHANGE is
      // re-emitted (the lease is renewed every ~10s, so emitting on every KV
      // event would flood the stream).
      const statusOf = new Map<string, 'busy' | 'idle' | 'unknown'>()
      // Reverse map for the lease key hash: `natsToken(name)` -> name, so a
      // lease event (keyed by the hash) can be resolved to a session. Built
      // from the snapshot list; a miss falls back to a DB scan (rare: only a
      // session created after the snapshot but before its lifecycle nudge).
      const tokenToName = new Map<string, string>()
      const nameOfToken = async (token: string): Promise<string | null> => {
        const hit = tokenToName.get(token)
        if (hit !== undefined) return hit
        const all = await Sessions.list(deps.db, tenant)
        if (all.isErr()) return null
        for (const s of all.value) tokenToName.set(natsToken(s.name), s.name)
        return tokenToName.get(token) ?? null
      }

      // Build one Session snapshot (facts + row + status) for a name; null if
      // gone.
      const snapshotOf = async (name: string) => {
        const r = await Sessions.get(deps.db, tenant, name)
        if (r.isErr() || r.value === null) return null
        const [facts, statuses] = await Promise.all([
          readMessageFacts(deps.bus, tenant, [name]),
          readSessionStatuses(deps.bus, tenant, [name]),
        ])
        const status = statuses.get(name) ?? 'idle'
        statusOf.set(name, status)
        return sessionToMsg(r.value, facts.get(name), status)
      }

      // A queue serializes the three independent watchers into one stream.
      const queue: WatchSessionsResponse[] = []
      let wake: (() => void) | null = null
      const push = (msg: WatchSessionsResponse) => {
        queue.push(msg)
        wake?.()
        wake = null
      }
      const pushUpsert = async (name: string) => {
        const s = await snapshotOf(name)
        if (s !== null) {
          push(create(WatchSessionsResponseSchema, { upserts: [s] }))
        }
      }

      // Start all three watchers BEFORE emitting the snapshot so no change
      // that lands during the (async) snapshot query is lost; anything they
      // observe is queued and flushed after the snapshot.
      // 1) fact changes (message landed / preview advanced).
      const factWatch = await deps.bus
        .kvWatch(BUCKET_SESSION_STATE, `t.${tenant}.>`)
        .catch(() => null)
      const factTask = (async () => {
        if (factWatch === null) return
        for await (const ev of factWatch.stream) {
          if (ev.deleted) continue
          let sid = ''
          try {
            sid = String(
              (JSON.parse(ev.value) as { session_name?: string })
                .session_name ?? '',
            )
          } catch {
            continue
          }
          if (sid !== '') await pushUpsert(sid)
        }
      })()

      // 2) structural lifecycle changes. Subscribed live (deliver_policy: new)
      //    BEFORE the snapshot: no nudge is lost between the snapshot and live.
      const lcSub = await deps.bus
        .subscribeStream(`abc.${tenant}.session.lifecycle.>`, {
          signal: ctx.signal,
        })
        .catch(() => null)
      const lcTask = (async () => {
        if (lcSub === null) return
        for await (const env of lcSub) {
          const kind = String((env.payload as { kind?: string })?.kind ?? '')
          const p = env.payload as {
            session_name?: string
            from?: string
            to?: string
          }
          if (kind === 'deleted' && p.session_name) {
            push(
              create(WatchSessionsResponseSchema, {
                removed: [p.session_name],
              }),
            )
          } else if (kind === 'renamed' && p.from && p.to) {
            push(create(WatchSessionsResponseSchema, { removed: [p.from] }))
            await pushUpsert(p.to)
          } else if (p.session_name) {
            await pushUpsert(p.session_name)
          }
        }
      })()

      // 3) settings-change nudges (setModel / updateSettings). Also live from
      //    now (durable `inboxPublish` on the same ABC_EVENTS stream).
      const chSub = await deps.bus
        .subscribeStream(`abc.${tenant}.session.changed`, {
          signal: ctx.signal,
        })
        .catch(() => null)
      const chTask = (async () => {
        if (chSub === null) return
        for await (const env of chSub) {
          const sid = String(
            (env.payload as { session_name?: string })?.session_name ?? '',
          )
          if (sid !== '') await pushUpsert(sid)
        }
      })()

      // 4) runtime status (run lock). A session is `busy` while its lease in
      //    `abc-session-state` exists AND the lease owner's heartbeat (in
      //    `abc-session-owner`) is alive. We emit a row ONLY on a status
      //    TRANSITION (the lease renews every ~10s — emitting per event would
      //    flood). Two sources: lease events (create/delete) and owner-heartbeat
      //    expiry (a crashed replica's sessions flip to idle without waiting for
      //    the lease TTL).
      const leasePrefix = `t.${tenant}.`
      // session -> owner, so an owner-heartbeat loss can find its sessions.
      const ownerToSessions = new Map<string, Set<string>>()
      const rememberOwner = async (name: string) => {
        const owner = await readLeaseOwner(deps.bus, tenant, name)
        for (const set of ownerToSessions.values()) set.delete(name)
        if (owner !== null && owner !== '') {
          const set = ownerToSessions.get(owner) ?? new Set<string>()
          set.add(name)
          ownerToSessions.set(owner, set)
        }
      }
      const leaseWatch = await deps.bus
        .kvWatch(LEASE_BUCKET, `${leasePrefix}>`)
        .catch(() => null)
      const leaseTask = (async () => {
        if (leaseWatch === null) return
        for await (const ev of leaseWatch.stream) {
          const token = ev.key.startsWith(leasePrefix)
            ? ev.key.slice(leasePrefix.length)
            : ev.key
          const name = await nameOfToken(token)
          if (name === null) continue
          // Recompute from lease + heartbeat (never trust ev.deleted alone:
          // a lease whose owner died reads idle). Track the owner for the
          // owner-bucket watcher.
          const status = await readSessionStatus(deps.bus, tenant, name)
          if (status === 'unknown') continue
          if (statusOf.get(name) === status) continue // no transition
          await pushUpsert(name)
          await rememberOwner(name)
        }
      })()
      // Owner-heartbeat watcher: when an owner's heartbeat key is deleted (its
      // TTL lapsed → the replica is gone), every session it owned flips idle.
      const ownerWatch = await deps.bus
        .kvWatch(OWNER_BUCKET, '>')
        .catch(() => null)
      const ownerTask = (async () => {
        if (ownerWatch === null) return
        for await (const ev of ownerWatch.stream) {
          if (!ev.deleted) continue
          const sessions = ownerToSessions.get(ev.key)
          if (sessions === undefined) continue
          for (const name of sessions) {
            const status = await readSessionStatus(deps.bus, tenant, name)
            if (status === 'unknown' || statusOf.get(name) === status) continue
            await pushUpsert(name)
          }
        }
      })()

      try {
        // Initial full snapshot (the client replaces its whole list). Seed the
        // status + token maps so the lease watcher only reports TRANSITIONS.
        const all = await Sessions.list(deps.db, tenant)
        if (all.isErr()) throw new Error(all.error)
        const names = all.value.map(s => s.name)
        const [facts, statuses] = await Promise.all([
          readMessageFacts(deps.bus, tenant, names),
          readSessionStatuses(deps.bus, tenant, names),
        ])
        for (const s of all.value) {
          tokenToName.set(natsToken(s.name), s.name)
          const st = statuses.get(s.name) ?? 'idle'
          statusOf.set(s.name, st)
          if (st === 'busy') await rememberOwner(s.name)
        }
        yield create(WatchSessionsResponseSchema, {
          snapshot: true,
          removed: [],
          upserts: all.value.map(s =>
            sessionToMsg(s, facts.get(s.name), statuses.get(s.name) ?? 'idle'),
          ),
        })
        for (;;) {
          while (queue.length > 0) {
            const msg = queue.shift()
            if (msg !== undefined) yield msg
          }
          await new Promise<void>(resolve => {
            wake = resolve
          })
        }
      } finally {
        await factWatch?.stop().catch(() => {})
        void factTask.catch(() => {})
        await lcSub?.close().catch(() => {})
        await chSub?.close().catch(() => {})
        await leaseWatch?.stop().catch(() => {})
        await ownerWatch?.stop().catch(() => {})
        void lcTask.catch(() => {})
        void chTask.catch(() => {})
        void leaseTask.catch(() => {})
        void ownerTask.catch(() => {})
      }
    },
  }
}

// toWatchEvent converts a bus envelope into a WatchSessionResponse message.
function toWatchEvent(raw: unknown): WatchSessionResponse {
  const env = raw as {
    event?: string
    params?: Record<string, unknown>
    eid?: string
    seq?: number
  }
  return create(WatchSessionResponseSchema, {
    event: env.event ?? 'message',
    params: toJsonObject(isRecord(env.params) ? env.params : {}),
    eid: env.eid ?? '',
    // The JetStream stream sequence: the client echoes the newest one back as
    // WatchSessionRequest.since_seq so a reconnect resumes with an O(1)
    // by_start_sequence seek (see watchSession).
    ...(typeof env.seq === 'number' ? { seq: BigInt(env.seq) } : {}),
  })
}

// ---- Struct helpers (google.protobuf.Value wrapping) ----
