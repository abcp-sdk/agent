import type { ProviderRow } from '@easylab-agent/schema'
import { eq } from 'drizzle-orm'
import type { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { nowStr, q } from './db-client.js'
import { providers } from './db-schema.js'

export interface ProviderInput {
  providerId: string
  apiType: string
  baseUrl: string
  apiKey: string
  headers: unknown
  models: unknown
}

const toRow = (r: typeof providers.$inferSelect): ProviderRow => ({
  provider_id: r.providerId,
  api_type: r.apiType,
  base_url: r.baseUrl,
  api_key: r.apiKey,
  headers: r.headers,
  models: r.models,
  updated_at: r.updatedAt,
})

export const Providers = {
  list(db: Db): ResultAsync<ProviderRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(providers)
          .orderBy(providers.providerId)
          .then(rows => rows.map(toRow)),
      'list providers',
    )
  },

  upsert(db: Db, input: ProviderInput): ResultAsync<void, string> {
    return q(
      () =>
        db
          .insert(providers)
          .values({
            providerId: input.providerId,
            apiType: input.apiType,
            baseUrl: input.baseUrl,
            apiKey: input.apiKey,
            headers: JSON.stringify(input.headers ?? null),
            models: JSON.stringify(input.models ?? []),
            createdAt: nowStr(),
            updatedAt: nowStr(),
          })
          .onConflictDoUpdate({
            target: providers.providerId,
            set: {
              apiType: input.apiType,
              baseUrl: input.baseUrl,
              apiKey: input.apiKey,
              headers: JSON.stringify(input.headers ?? null),
              models: JSON.stringify(input.models ?? []),
              updatedAt: nowStr(),
            },
          }),
      'upsert provider',
    ).map(() => undefined)
  },

  delete(db: Db, id: string): ResultAsync<boolean, string> {
    return q(
      () =>
        db
          .delete(providers)
          .where(eq(providers.providerId, id))
          .returning({ id: providers.providerId })
          .then(rows => rows.length > 0),
      'delete provider',
    )
  },
}
