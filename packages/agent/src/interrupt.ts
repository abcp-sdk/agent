/**
 * In-memory per-session abort controllers: the mid-stream interrupt signal.
 * The HTTP interrupt route (same replica) aborts directly; cross-replica
 * interrupts arrive via the durable mailbox wake signal, which the running
 * replica watches and forwards to the same controller.
 *
 * Keys are TENANT-SCOPED: two tenants may run same-named sessions on one
 * replica, and an interrupt/delete aimed at one must never abort the other's
 * in-flight turn.
 */
const controllers = new Map<string, AbortController>()

/**
 * Why a session's run was aborted. Recorded alongside the controller so the
 * turn-end outcome can distinguish a USER interrupt (`interrupted`) from a
 * lease loss (`locklost`). The distinction matters: idlewatch resumes a turn
 * that ended for an environmental reason, but never one the user stopped.
 */
export type AbortReason = 'user' | 'locklost'

/** The reason stamped by the most recent abort of a session's run. */
const reasons = new Map<string, AbortReason>()

/** Composite map key — the tenant+session pair is the run's identity. */
function keyOf(tenant: string, sid: string): string {
  return `${tenant}\n${sid}`
}

export function getAbortController(
  tenant: string,
  sid: string,
): AbortController {
  const key = keyOf(tenant, sid)
  let ctrl = controllers.get(key)
  if (ctrl === undefined || ctrl.signal.aborted) {
    // A FRESH run: drop any reason recorded for the previous (aborted) run so a
    // stale `locklost`/`user` cannot leak into this run's turn-end outcome.
    if (ctrl?.signal.aborted) reasons.delete(key)
    ctrl = new AbortController()
    controllers.set(key, ctrl)
  }
  return ctrl
}

/**
 * Abort a run, recording WHY. This is the single entry point so every abort
 * carries a reason. A no-op when no controller exists (nothing is running).
 */
export function abortRun(
  tenant: string,
  sid: string,
  reason: AbortReason,
): void {
  const key = keyOf(tenant, sid)
  const ctrl = controllers.get(key)
  if (ctrl === undefined || ctrl.signal.aborted) {
    // Record the reason even if the controller already aborted, so the FIRST
    // and most specific reason wins (e.g. a user interrupt that lands just
    // before a lock-loss sweep keeps `user`).
    if (ctrl === undefined) return
    if (!reasons.has(key)) reasons.set(key, reason)
    return
  }
  reasons.set(key, reason)
  ctrl.abort()
}

/** Abort because the USER interrupted / deleted the session. */
export function interruptRun(tenant: string, sid: string): void {
  abortRun(tenant, sid, 'user')
}

/** The reason stamped by the most recent abort, or null when not aborted. */
export function abortReason(tenant: string, sid: string): AbortReason | null {
  return reasons.get(keyOf(tenant, sid)) ?? null
}

export function clearRun(tenant: string, sid: string): void {
  const key = keyOf(tenant, sid)
  controllers.delete(key)
  reasons.delete(key)
}

export function isAborted(tenant: string, sid: string): boolean {
  return controllers.get(keyOf(tenant, sid))?.signal.aborted ?? false
}
