import { createHash } from 'node:crypto'
import {
  type AgentDeps,
  type FileRecord,
  factFromPersist,
  fileBySha,
  Messages,
  Parts,
  parse,
  randomCode,
  scheduleMediaProbe,
  Sessions,
  TextPartDataSchema,
  upsertFile,
  writeMessageFact,
} from '@abcp-agent/agent'

/**
 * Helpers shared by the Connect handler modules (they operate on AgentDeps,
 * not on a specific RPC, so they live here rather than in a handler file).
 */

/**
 * Rewrite the session-list message-fact (preview/time/role) from the CURRENT
 * tip, preserving `message_seq`. Called after an undo moves the tip backwards,
 * so the list reflects the withdrawn chain immediately. `last_message_*` are
 * cleared when the session has no tip left.
 */
export async function refreshMessageFactFromTip(
  deps: AgentDeps,
  tenant: string,
  sid: string,
): Promise<void> {
  const tipRes = await Sessions.tip(deps.db, tenant, sid)
  const tipId = tipRes.isErr() ? null : tipRes.value
  if (tipId === null || tipId === '') {
    await writeMessageFact(deps.bus, tenant, sid, factFromPersist('', '', ''))
    return
  }
  const target = await Messages.get(deps.db, tenant, tipId)
  if (target.isErr() || target.value === null) {
    await writeMessageFact(deps.bus, tenant, sid, factFromPersist('', '', ''))
    return
  }
  const partsRes = await Parts.listByMessages(deps.db, tenant, [tipId])
  let text = ''
  if (partsRes.isOk()) {
    for (const p of partsRes.value) {
      if (p.type !== 'text') continue
      const d = parse(TextPartDataSchema, p.data)
      if (d.isOk()) {
        text += d.value.text
        break
      }
    }
  }
  await writeMessageFact(
    deps.bus,
    tenant,
    sid,
    factFromPersist(
      target.value.created_at ?? '',
      target.value.role ?? '',
      text,
    ),
  )
}

function getSha(data: Uint8Array): string {
  return createHash('sha256').update(data).digest('hex')
}

/** Dedup + store a single file. Shared by ingest/upload in the Connect surface. */
export async function storeBytes(
  deps: AgentDeps,
  tenant: string,
  data: Uint8Array,
  name: string,
  mime: string,
  uploader: string,
): Promise<FileRecord> {
  const sha = getSha(data)
  const existing = await fileBySha(deps.bus, tenant, sha)
  if (existing.isOk() && existing.value !== null) {
    return existing.value
  }
  const code = randomCode()
  const record: FileRecord = {
    code,
    sha256: sha,
    name,
    mime,
    size: data.length,
    uploader_session: uploader,
    created_at: new Date().toISOString(),
  }
  await deps.files.put(tenant, code, record, data)
  await upsertFile(deps.bus, tenant, record)
  // Derive media metadata (dimensions / duration / thumbnail / thumbhash)
  // asynchronously so the store call itself never blocks on a decode.
  scheduleMediaProbe({ bus: deps.bus, files: deps.files }, tenant, record)
  return record
}
