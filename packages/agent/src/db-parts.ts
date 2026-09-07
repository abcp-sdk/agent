import type { PartRow } from '@easylab-agent/schema'
import { inArray } from 'drizzle-orm'
import { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { q, uuid } from './db-client.js'
import { parts } from './db-schema.js'

const toRow = (r: typeof parts.$inferSelect): PartRow => ({
  id: r.id,
  message_id: r.messageId,
  type: r.type,
  seq: r.seq,
  data: r.data,
})

export const Parts = {
  insert(
    db: Db,
    messageId: string,
    type: string,
    seq: number,
    data: unknown,
  ): ResultAsync<string, string> {
    const id = uuid()
    return q(
      () =>
        db.insert(parts).values({
          id,
          messageId,
          type,
          seq,
          data: JSON.stringify(data ?? {}),
        }),
      'insert part',
    ).map(() => id)
  },

  /** Parts for a set of message ids (COW-shared chains). */
  listByMessages(db: Db, messageIds: string[]): ResultAsync<PartRow[], string> {
    if (messageIds.length === 0) {
      return ResultAsync.fromSafePromise(Promise.resolve<PartRow[]>([]))
    }
    return q(
      () =>
        db
          .select()
          .from(parts)
          .where(inArray(parts.messageId, messageIds))
          .orderBy(parts.messageId, parts.seq)
          .then(rows => rows.map(toRow)),
      'list parts by messages',
    )
  },

  /** Insert the compaction message's summary part (with the tail boundary). */
  insertSummary(
    db: Db,
    messageId: string,
    summary: string,
    tailFromId: string | null,
  ): ResultAsync<string, string> {
    return Parts.insert(db, messageId, 'summary', 0, {
      summary,
      tail_from: tailFromId,
    })
  },
}
