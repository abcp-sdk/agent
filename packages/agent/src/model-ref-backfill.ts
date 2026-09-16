import { z } from 'zod'
import type { Db } from './db-client.js'
import { Providers } from './db-providers.js'
import { Sessions } from './db-sessions.js'
import { parse } from './json.js'
import { modelRef } from './llm.js'
import { logger } from './logger.js'

const StringArraySchema = z.array(z.string())

/**
 * One-time, idempotent migration of legacy bare model ids to canonical
 * `provider_id/model_id` references.
 *
 * Historically a session stored only a model id and the provider was found by
 * a flat lookup. That is ambiguous when several providers offer the same model
 * id, so it has been removed. For every session whose model has no `/`:
 *   - exactly one registered provider advertises the model → rewrite to
 *     `provider/model`;
 *   - zero or multiple providers → leave the value untouched (non-destructive)
 *     so the next turn fails with a clear "must be provider_id/model_id"
 *     error and the user can pick a provider explicitly.
 *
 * Safe to run on every boot (no-op once all refs are canonical).
 */
export async function backfillModelRefs(
  db: Db,
  tenants: readonly string[],
): Promise<void> {
  for (const tenant of tenants) {
    const rows = await Sessions.list(db, tenant)
    if (rows.isErr()) {
      logger.warn(
        { tenant, err: rows.error },
        'model-ref backfill: list sessions failed',
      )
      continue
    }
    const legacy = rows.value.filter(
      s => s.model !== '' && !s.model.includes('/'),
    )
    if (legacy.length === 0) continue

    const providersRes = await Providers.list(db, tenant)
    if (providersRes.isErr()) {
      logger.warn(
        { tenant, err: providersRes.error },
        'model-ref backfill: list providers failed',
      )
      continue
    }
    const providers = providersRes.value

    let migrated = 0
    let ambiguous = 0
    for (const s of legacy) {
      const owners = providers.filter(p => {
        const models = parse(StringArraySchema, p.models)
        return models.isOk() && models.value.includes(s.model)
      })
      if (owners.length !== 1) {
        ambiguous++
        logger.warn(
          {
            tenant,
            name: s.name,
            model: s.model,
            owners: owners.map(o => o.provider_id),
          },
          'model-ref backfill: cannot disambiguate; leaving bare model',
        )
        continue
      }
      const ref = modelRef(owners[0]!.provider_id, s.model)
      await Sessions.setModel(db, tenant, s.name, ref)
      migrated++
    }
    logger.info(
      { tenant, scanned: legacy.length, migrated, ambiguous },
      'model-ref backfill complete',
    )
  }
}
