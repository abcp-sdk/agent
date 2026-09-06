import type { Bus } from './bus.js'
import type { Db } from './db-client.js'
import { rawAll } from './db-client.js'
import { type FileRecord, upsertFile } from './files.js'
import { Config, Presets } from './kv-store.js'
import { logger } from './logger.js'

const MARKER_KEY = '__pg_backfill__'

/**
 * One-time migration of presets/config/files-meta rows from the legacy PG
 * tables into their NATS KV buckets. Each domain is guarded by its own
 * marker key: a failure leaves the marker unset so the next boot retries.
 * Fresh installs (tables never created) skip silently.
 */
export async function backfillKvFromPg(db: Db, bus: Bus): Promise<void> {
  await backfillPresets(db, bus)
  await backfillConfig(db, bus)
  await backfillFiles(db, bus)
}

async function markerSet(bus: Bus, domain: string): Promise<boolean> {
  return (
    (await bus.kvGet('abc-agent-config', `${MARKER_KEY}.${domain}`)) !== null
  )
}

async function setMarker(bus: Bus, domain: string): Promise<void> {
  await bus.kvPut('abc-agent-config', `${MARKER_KEY}.${domain}`, '1', 0)
}

async function legacyRows(
  db: Db,
  table: string,
): Promise<Record<string, unknown>[]> {
  try {
    return await rawAll(db, `SELECT * FROM ${table}`)
  } catch {
    return [] // table absent on fresh installs
  }
}

async function backfillPresets(db: Db, bus: Bus): Promise<void> {
  if (await markerSet(bus, 'presets')) return
  const rows = await legacyRows(db, 'presets')
  let n = 0
  for (const r of rows) {
    const id = String(r.id ?? '')
    if (id === '') continue
    const res = await Presets.upsert(bus, {
      id,
      systemPrompt: String(r.system_prompt ?? ''),
      systemPromptI18n: String(r.system_prompt_i18n ?? '{}'),
      tools: String(r.tools ?? '[]'),
      maxTurns: Number(r.max_turns ?? 30),
    })
    if (res.isOk()) n++
  }
  await setMarker(bus, 'presets')
  if (n > 0) logger.info({ n }, 'backfilled presets from PG')
}

async function backfillConfig(db: Db, bus: Bus): Promise<void> {
  if (await markerSet(bus, 'config')) return
  const rows = await legacyRows(db, 'config')
  let n = 0
  for (const r of rows) {
    const key = String(r.key ?? '')
    if (key === '') continue
    const res = await Config.set(bus, key, String(r.value ?? '{}'))
    if (res.isOk()) n++
  }
  await setMarker(bus, 'config')
  if (n > 0) logger.info({ n }, 'backfilled config from PG')
}

async function backfillFiles(db: Db, bus: Bus): Promise<void> {
  if (await markerSet(bus, 'files')) return
  const rows = await legacyRows(db, 'files')
  let n = 0
  for (const r of rows) {
    const code = String(r.code ?? '')
    if (code === '') continue
    const record: FileRecord = {
      code,
      sha256: String(r.sha256 ?? ''),
      name: String(r.name ?? ''),
      mime: String(r.mime ?? ''),
      size: Number(r.size ?? 0),
      uploader_session: String(r.uploader_session ?? ''),
      created_at: String(r.created_at ?? ''),
    }
    const res = await upsertFile(bus, record)
    if (res.isOk()) n++
  }
  await setMarker(bus, 'files')
  if (n > 0) logger.info({ n }, 'backfilled files meta from PG')
}
