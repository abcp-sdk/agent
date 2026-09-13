import type { Bus } from './bus.js'
import { natsToken, tenantKVKey } from './bus.js'

/**
 * App-layer caches over the abc Bus (protocol-level helpers only; the SDK
 * deliberately ships no opinionated session-ids / models-dev caching):
 *   - session context id-list per session (24h TTL, recomputable by re-walk)
 *   - models.dev catalog snapshot (object store blob)
 */

const IDS_BUCKET = 'abc-session-ids'
const IDS_TTL_MS = 24 * 3600 * 1000
const MODELS_KEY = 'models-dev-catalog.json'

/** Read the cached context id list for a session, or null when absent. */
export async function getSessionIds(
  bus: Bus,
  tenant: string,
  sessionName: string,
): Promise<string[] | null> {
  const raw = await bus
    .kvGet(IDS_BUCKET, tenantKVKey(tenant, natsToken(sessionName)))
    .catch(() => null)
  if (raw === null) return null
  try {
    const v = JSON.parse(raw)
    return Array.isArray(v) && v.every(x => typeof x === 'string') ? v : null
  } catch {
    return null
  }
}

/** Overwrite the cached context id list for a session. */
export function putSessionIds(
  bus: Bus,
  tenant: string,
  sessionName: string,
  ids: string[],
): Promise<void> {
  return bus.kvPut(
    IDS_BUCKET,
    tenantKVKey(tenant, natsToken(sessionName)),
    JSON.stringify(ids),
    IDS_TTL_MS,
  )
}

/** Append one message id to the session context id list (no-op on miss). */
export async function appendSessionId(
  bus: Bus,
  tenant: string,
  sessionName: string,
  id: string,
): Promise<void> {
  const ids = await getSessionIds(bus, tenant, sessionName)
  if (ids === null) return
  ids.push(id)
  await putSessionIds(bus, tenant, sessionName, ids)
}

/** Drop the session context id-list cache (force a re-walk). */
export function deleteSessionIds(
  bus: Bus,
  tenant: string,
  sessionName: string,
): Promise<void> {
  return bus.kvDelete(IDS_BUCKET, tenantKVKey(tenant, natsToken(sessionName)))
}

/** Cache the models.dev catalog JSON in the object store. */
export function putModelsDev(bus: Bus, json: string): Promise<void> {
  return bus.objectPut(MODELS_KEY, Buffer.from(json))
}

/** Read the cached models.dev catalog, if present. */
export async function getModelsDev(bus: Bus): Promise<unknown> {
  const data = await bus.objectGet(MODELS_KEY)
  if (data === null || data.length === 0) return null
  try {
    return JSON.parse(Buffer.from(data).toString('utf8'))
  } catch {
    return null
  }
}
