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
    ctrl = new AbortController()
    controllers.set(key, ctrl)
  }
  return ctrl
}

export function interruptRun(tenant: string, sid: string): void {
  controllers.get(keyOf(tenant, sid))?.abort()
}

export function clearRun(tenant: string, sid: string): void {
  controllers.delete(keyOf(tenant, sid))
}

export function isAborted(tenant: string, sid: string): boolean {
  return controllers.get(keyOf(tenant, sid))?.signal.aborted ?? false
}
