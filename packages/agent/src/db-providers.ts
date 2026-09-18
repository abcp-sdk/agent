import type { ProviderRow } from '@abcp-agent/schema'
import { and, eq, or } from 'drizzle-orm'
import type { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { nowStr, q } from './db-client.js'
import { providers } from './db-schema.js'

/** The reserved tenant owning shared, read-only infrastructure providers
 *  (e.g. the platform gateway). Every tenant may USE these but must not
 *  mutate them. */
export const SHARED_TENANT = 'global'

export interface ProviderInput {
  providerId: string
  /** The single modality this provider serves (semantic grouping). */
  capability: string
  apiType: string
  baseUrl: string
  apiKey: string
  headers: unknown
  models: unknown
}

const toRow = (r: typeof providers.$inferSelect): ProviderRow => ({
  provider_id: r.providerId,
  capability: r.capability,
  api_type: r.apiType,
  base_url: r.baseUrl,
  api_key: r.apiKey,
  headers: r.headers,
  models: r.models,
  updated_at: r.updatedAt,
})

export const Providers = {
  /** Providers visible to a tenant: its own rows plus the shared (global)
   *  ones. A tenant-local row with the same provider_id shadows the shared
   *  one, so a tenant may register its own credentials for a shared provider
   *  id if it wants to. */
  list(db: Db, tenant: string): ResultAsync<ProviderRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(providers)
          .where(
            or(
              eq(providers.tenant, tenant),
              eq(providers.tenant, SHARED_TENANT),
            ),
          )
          .orderBy(providers.providerId)
          .then(rows => {
            // Tenant-local rows shadow shared (global) rows with the same id.
            const byId = new Map<string, ProviderRow>()
            for (const r of rows) {
              const isShared = r.tenant === SHARED_TENANT
              const existing = byId.get(r.providerId)
              if (existing === undefined || !isShared) {
                byId.set(r.providerId, toRow(r))
              }
            }
            return [...byId.values()]
          }),
      'list providers',
    )
  },

  /** The providers of ONE modality (e.g. every `image` provider). */
  listByCapability(
    db: Db,
    tenant: string,
    capability: string,
  ): ResultAsync<ProviderRow[], string> {
    return Providers.list(db, tenant).map(rows =>
      rows.filter(r => r.capability === capability),
    )
  },

  /** The provider that owns [providerId] and serves [capability]. */
  get(
    db: Db,
    tenant: string,
    providerId: string,
    capability: string,
  ): ResultAsync<ProviderRow | null, string> {
    return Providers.list(db, tenant).map(
      rows =>
        rows.find(
          r => r.provider_id === providerId && r.capability === capability,
        ) ?? null,
    )
  },

  listTenants(db: Db, tenant: string): ResultAsync<ProviderRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(providers)
          .where(eq(providers.tenant, tenant))
          .orderBy(providers.providerId)
          .then(rows => rows.map(toRow)),
      'list tenant providers',
    )
  },

  upsert(
    db: Db,
    tenant: string,
    input: ProviderInput,
  ): ResultAsync<void, string> {
    return q(
      () =>
        db
          .insert(providers)
          .values({
            tenant,
            providerId: input.providerId,
            capability: input.capability,
            apiType: input.apiType,
            baseUrl: input.baseUrl,
            apiKey: input.apiKey,
            headers: JSON.stringify(input.headers ?? null),
            models: JSON.stringify(input.models ?? []),
            createdAt: nowStr(),
            updatedAt: nowStr(),
          })
          .onConflictDoUpdate({
            target: [providers.tenant, providers.providerId],
            set: {
              capability: input.capability,
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

  delete(db: Db, tenant: string, id: string): ResultAsync<boolean, string> {
    return q(
      () =>
        db
          .delete(providers)
          .where(
            and(eq(providers.tenant, tenant), eq(providers.providerId, id)),
          )
          .returning({ id: providers.providerId })
          .then(rows => rows.length > 0),
      'delete provider',
    )
  },
}
