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
  // Refresh the in-process parsed cache in the same step: a fresh catalog is
  // being written, so the next reader can skip the object-store round trip
  // AND the 4 MB JSON.parse.
  try {
    modelsDevCache = { value: JSON.parse(json), at: Date.now() }
  } catch {
    modelsDevCache = null
  }
  return bus.objectPut(MODELS_KEY, Buffer.from(json))
}

/**
 * In-process cache of the parsed models.dev catalog. The catalog is a ~4 MB
 * JSON blob (193 providers); reading it from the NATS object store and
 * `JSON.parse`-ing it on EVERY listModels call dominated that RPC's latency.
 * The catalog changes at most every 30 min (refreshModelsDev), so a short TTL
 * is safe and turns the hot path into an in-memory lookup.
 */
let modelsDevCache: { value: unknown; at: number } | null = null
const MODELS_DEV_TTL_MS = 60 * 1000

/** Read the cached models.dev catalog, if present. */
export async function getModelsDev(bus: Bus): Promise<unknown> {
  if (
    modelsDevCache !== null &&
    Date.now() - modelsDevCache.at < MODELS_DEV_TTL_MS
  ) {
    return modelsDevCache.value
  }
  const data = await bus.objectGet(MODELS_KEY)
  if (data === null || data.length === 0) return null
  try {
    const value = JSON.parse(Buffer.from(data).toString('utf8'))
    modelsDevCache = { value, at: Date.now() }
    return value
  } catch {
    return null
  }
}
