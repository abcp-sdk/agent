import { createHash, randomUUID } from 'node:crypto'
import { and, eq, isNull } from 'drizzle-orm'
import { ResultAsync } from 'neverthrow'
import type { Db } from './db-client.js'
import { nowStr, q } from './db-client.js'
import { tenantTokens, tenants } from './db-schema.js'

/**
 * Persistence for the multi-tenant identity layer: tenants + their bearer
 * tokens. Tokens are stored ONLY as sha256(plaintext); the plaintext is minted
 * here and returned once by the caller. A token authenticates to exactly one
 * tenant (`token -> tenant`).
 */

export interface TenantRow {
  id: string
  name: string
  disabled: boolean
  createdAt: string
  updatedAt: string
}

export interface TenantTokenRow {
  tokenId: string
  tenantId: string
  label: string
  createdAt: string
  lastUsedAt: string | null
  revoked: boolean
}

const toTenant = (r: typeof tenants.$inferSelect): TenantRow => ({
  id: r.id,
  name: r.name,
  disabled: Number(r.disabled) !== 0,
  createdAt: r.createdAt,
  updatedAt: r.updatedAt,
})

const toToken = (r: typeof tenantTokens.$inferSelect): TenantTokenRow => ({
  tokenId: r.tokenId,
  tenantId: r.tenantId,
  label: r.label,
  createdAt: r.createdAt,
  lastUsedAt: r.lastUsedAt,
  revoked: r.revokedAt !== null && r.revokedAt !== '',
})

function tokenSha256(data: string): string {
  return createHash('sha256').update(data, 'utf8').digest('hex')
}

/** Mint a fresh opaque token: 32 random bytes, base64url, no padding. */
export function mintToken(): string {
  return Buffer.from(crypto.getRandomValues(new Uint8Array(32)))
    .toString('base64url')
}

export const Tenants = {
  /** sha256(plaintext) — the token hash. Exposed so the auth layer hashes in
   *  lockstep with storage. */
  hashToken(plaintext: string): string {
    return tokenSha256(plaintext)
  },

  list(db: Db): ResultAsync<TenantRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(tenants)
          .orderBy(tenants.id)
          .then(rows => rows.map(toTenant)),
      'list tenants',
    )
  },

  get(db: Db, id: string): ResultAsync<TenantRow | null, string> {
    return q(
      () =>
        db
          .select()
          .from(tenants)
          .where(eq(tenants.id, id))
          .limit(1)
          .then(rows => (rows[0] === undefined ? null : toTenant(rows[0]))),
      'get tenant',
    )
  },

  create(
    db: Db,
    id: string,
    name: string,
  ): ResultAsync<TenantRow, string> {
    const now = nowStr()
    const row: TenantRow = {
      id,
      name,
      disabled: false,
      createdAt: now,
      updatedAt: now,
    }
    return q(
      () =>
        db
          .insert(tenants)
          .values({ id, name, disabled: 0, createdAt: now, updatedAt: now }),
      'create tenant',
    ).map(() => row)
  },

  update(
    db: Db,
    id: string,
    patch: { name?: string | undefined; disabled?: boolean | undefined },
  ): ResultAsync<void, string> {
    return q(async () => {
      const set: Partial<typeof tenants.$inferInsert> = { updatedAt: nowStr() }
      if (patch.name !== undefined) set.name = patch.name
      if (patch.disabled !== undefined) set.disabled = patch.disabled ? 1 : 0
      await db.update(tenants).set(set).where(eq(tenants.id, id))
    }, 'update tenant').map(() => undefined)
  },

  /**
   * Disable a tenant (soft): its tokens immediately stop authenticating and
   * its data is retained. We never hard-delete, so this is a status flip.
   */
  disable(db: Db, id: string): ResultAsync<void, string> {
    return Tenants.update(db, id, { disabled: true })
  },

  /** Issue a token for a tenant. Returns the row plus the ONE-TIME plaintext.
   *  When [plaintext] is provided (bootstrap), that exact value is stored
   *  (hashed) instead of a freshly minted one. */
  issueToken(
    db: Db,
    tenantId: string,
    label: string,
    plaintext?: string,
  ): ResultAsync<{ token: TenantTokenRow; plaintext: string }, string> {
    const value = plaintext ?? mintToken()
    const sha = tokenSha256(value)
    const row: typeof tenantTokens.$inferInsert = {
      tokenId: randomUUID(),
      tenantId,
      tokenSha256: sha,
      label,
      createdAt: nowStr(),
      lastUsedAt: null,
      revokedAt: null,
    }
    return q(
      () => db.insert(tenantTokens).values(row),
      'issue tenant token',
    ).map(() => ({
      token: {
        tokenId: row.tokenId,
        tenantId,
        label,
        createdAt: row.createdAt,
        lastUsedAt: null,
        revoked: false,
      },
      plaintext: value,
    }))
  },

  listTokens(
    db: Db,
    tenantId: string,
  ): ResultAsync<TenantTokenRow[], string> {
    return q(
      () =>
        db
          .select()
          .from(tenantTokens)
          .where(eq(tenantTokens.tenantId, tenantId))
          .orderBy(tenantTokens.createdAt)
          .then(rows => rows.map(toToken)),
      'list tenant tokens',
    )
  },

  revokeToken(db: Db, tokenId: string): ResultAsync<void, string> {
    return q(
      () =>
        db
          .update(tenantTokens)
          .set({ revokedAt: nowStr() })
          .where(eq(tenantTokens.tokenId, tokenId)),
      'revoke tenant token',
    ).map(() => undefined)
  },

  /** Rotate: revoke the old token, issue a fresh one for the same tenant. */
  rotateToken(
    db: Db,
    tokenId: string,
  ): ResultAsync<{ token: TenantTokenRow; plaintext: string }, string> {
    return q(async () => {
      const rows = await db
        .select()
        .from(tenantTokens)
        .where(eq(tenantTokens.tokenId, tokenId))
        .limit(1)
      const existing = rows[0]
      if (existing === undefined) {
        throw new Error(`token not found: ${tokenId}`)
      }
      await db
        .update(tenantTokens)
        .set({ revokedAt: nowStr() })
        .where(eq(tenantTokens.tokenId, tokenId))
      const plaintext = mintToken()
      const newRow = {
        tokenId: randomUUID(),
        tenantId: existing.tenantId,
        tokenSha256: tokenSha256(plaintext),
        label: existing.label,
        createdAt: nowStr(),
        lastUsedAt: null as string | null,
        revokedAt: null as string | null,
      }
      await db.insert(tenantTokens).values(newRow)
      return { token: toToken(newRow as typeof tenantTokens.$inferSelect), plaintext }
    }, 'rotate tenant token')
  },

  /**
   * Resolve a plaintext token to its (non-revoked, non-disabled) tenant, or
   * null. Also bumps `last_used_at` best-effort (not awaited by callers that
   * only need the identity).
   */
  resolveToken(
    db: Db,
    plaintext: string,
  ): ResultAsync<{ tenantId: string; tokenId: string } | null, string> {
    const sha = tokenSha256(plaintext)
    return q(async () => {
      const rows = await db
        .select()
        .from(tenantTokens)
        .where(
          and(
            eq(tenantTokens.tokenSha256, sha),
            isNull(tenantTokens.revokedAt),
          ),
        )
        .limit(1)
      const hit = rows[0]
      if (hit === undefined) return null
      const t = await db
        .select()
        .from(tenants)
        .where(eq(tenants.id, hit.tenantId))
        .limit(1)
      const tenant = t[0]
      if (tenant === undefined || Number(tenant.disabled) !== 0) return null
      return { tenantId: hit.tenantId, tokenId: hit.tokenId }
    }, 'resolve tenant token')
  },
}

/** Touch a token's last_used_at (best-effort, fire-and-forget). */
export function touchToken(db: Db, tokenId: string): void {
  void db
    .update(tenantTokens)
    .set({ lastUsedAt: nowStr() })
    .where(eq(tenantTokens.tokenId, tokenId))
    .catch(() => {})
}
