import type { Provider, ProviderMap } from '@opencode-ai/models'
import { Models } from '@opencode-ai/models'
import { providers as snapshotProviders } from '@opencode-ai/models/snapshot'
import { ResultAsync } from 'neverthrow'
import type { Bus } from './bus.js'
import { putModelsDev } from './store.js'

/**
 * Fetch the models.dev provider catalog using the official typed client and
 * cache it in the NATS KV (30-minute TTL) so any replica can serve provider
 * prefill hints.
 *
 * The cluster may lack direct internet egress, so when the dynamic fetch
 * fails we fall back to the offline snapshot bundled with the SDK (refreshed
 * on each dependency bump). Either way the result is written to the KV for all
 * replicas to read. Both sources are already `ProviderMap` — no reshaping.
 */

export type CatalogProvider = Provider

const client = Models.make()

/** Fetch the catalog (or fall back to the bundled snapshot) and cache it. */
export function refreshModelsDev(bus: Bus): ResultAsync<void, string> {
  const live: ResultAsync<ProviderMap, string> = ResultAsync.fromPromise(
    client.providers(),
    e => `models.dev fetch: ${String(e)}`,
  )

  const fallback = ResultAsync.fromSafePromise(
    Promise.resolve(snapshotProviders),
  )

  return live
    .orElse(() => fallback)
    .andThen(providers =>
      ResultAsync.fromPromise(
        putModelsDev(bus, JSON.stringify(providers)),
        e => `cache models.dev: ${String(e)}`,
      ),
    )
}
