import {
  type AgentDeps,
  type FileRecord,
  factFromPersist,
  Messages,
  Parts,
  parse,
  Sessions,
  storeFile,
  TextPartDataSchema,
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

/**
 * Store one file through the single canonical path ([storeFile]): the mime is
 * DERIVED from the bytes, never accepted from the caller. Kept as a thin
 * wrapper so the Connect handlers keep their existing call shape.
 */
export async function storeBytes(
  deps: AgentDeps,
  tenant: string,
  data: Uint8Array,
  name: string,
  uploader: string,
): Promise<FileRecord> {
  return storeFile(
    { bus: deps.bus, files: deps.files },
    { tenant, data, name, uploaderSession: uploader },
  )
}
