import type { HandlerContext, Interceptor } from '@connectrpc/connect'
import { Code, ConnectError, createContextKey } from '@connectrpc/connect'
import type { AgentDeps } from '@abcp-agent/agent'
import { Tenants, touchToken } from '@abcp-agent/agent'

/**
 * Request authentication (protocol v2 multi-tenancy).
 *
 * Every request carries `Authorization: Bearer <token>`. The token is one of:
 *
 *   - the static ADMIN token (`AGENT_ADMIN_TOKEN`) -> role=admin;
 *   - a tenant token (opaque, stored only as sha256) -> role=tenant, with the
 *     resolved tenant id.
 *
 * The identity is written into the per-call context values and read by
 * `tenantOf(ctx)`. A non-Health request without a valid token is rejected
 * with `Unauthenticated` BEFORE its body is read.
 *
 * `AGENT_AUTH_MODE=none` bypasses all of this (single-tenant / local dev) and
 * pins every request to `AGENT_DEFAULT_TENANT`.
 */

export type Role = 'admin' | 'tenant'

export interface Identity {
  role: Role
  /** The tenant id (role=tenant). Empty for admin. */
  tenant: string
}

export const kIdentity = createContextKey<Identity | undefined>(undefined, {
  description: 'abc request identity (role + tenant)',
})

/** The granted identity for a call, or an anonymous tenant identity. */
export function identityOf(ctx: HandlerContext): Identity {
  return ctx.values.get(kIdentity) ?? { role: 'tenant', tenant: '' }
}

export interface AuthConfig {
  /** 'required' (default) or 'none' (dev / single-tenant). */
  mode: 'required' | 'none'
  /** Static admin bearer token. */
  adminToken: string
  /** Tenant used when auth is disabled. */
  defaultTenant: string
}

/**
 * Bounded TTL cache for resolved tenant tokens, so the per-request auth check
 * does not hit the DB on every call. Admin mutations invalidate explicitly.
 */
class TokenCache {
  private readonly map = new Map<
    string,
    { tenantId: string; tokenId: string; expiresAt: number }
  >()
  constructor(
    private readonly ttlMs = 30_000,
    private readonly cap = 4096,
  ) {}

  get(sha: string): { tenantId: string; tokenId: string } | null {
    const hit = this.map.get(sha)
    if (hit === undefined) return null
    if (hit.expiresAt < Date.now()) {
      this.map.delete(sha)
      return null
    }
    return { tenantId: hit.tenantId, tokenId: hit.tokenId }
  }

  put(sha: string, v: { tenantId: string; tokenId: string }): void {
    if (this.map.size >= this.cap) {
      const oldest = this.map.keys().next().value
      if (oldest !== undefined) this.map.delete(oldest)
    }
    this.map.set(sha, { ...v, expiresAt: Date.now() + this.ttlMs })
  }

  invalidateAll(): void {
    this.map.clear()
  }
}

/** Bearer token of a request, or '' when absent. */
function bearerToken(req: {
  header: { get(name: string): string | null }
}): string {
  const raw = req.header.get('authorization')
  if (raw === null || raw === undefined) return ''
  const m = /^Bearer\s+(.+)$/i.exec(raw.trim())
  return m?.[1]?.trim() ?? ''
}

/**
 * Connect interceptor implementing the auth gate for BOTH unary and streaming
 * calls: it runs before the handler reads the message and throws
 * `Unauthenticated` when the credential is missing/invalid.
 *
 * Returns the interceptor plus an `invalidate()` hook the AdminService calls
 * after issuing/revoking/rotating tokens (same replica: immediate).
 */
export function makeAuth(
  deps: AgentDeps,
  auth: AuthConfig,
): { interceptor: Interceptor; invalidate: () => void } {
  const cache = new TokenCache()
  const interceptor: Interceptor = next => async req => {
    const svc = req.service.typeName
    // Health is public (probes / readiness).
    if (svc === 'agent.v1.AgentService' && req.method.name === 'Health') {
      return next(req)
    }
    const isAdmin = svc === 'agent.v1.AdminService'

    if (auth.mode === 'none') {
      req.contextValues.set(kIdentity, {
        role: 'tenant',
        tenant: auth.defaultTenant,
      })
      return next(req)
    }

    const token = bearerToken(req)
    if (token === '') {
      throw new ConnectError('missing bearer token', Code.Unauthenticated)
    }

    // Static admin token (never logged).
    if (auth.adminToken !== '' && token === auth.adminToken) {
      req.contextValues.set(kIdentity, { role: 'admin', tenant: '' })
      return next(req)
    }

    // A tenant token can NEVER reach the admin surface.
    if (isAdmin) {
      throw new ConnectError('admin token required', Code.PermissionDenied)
    }

    const sha = Tenants.hashToken(token)
    const cached = cache.get(sha)
    if (cached !== null) {
      req.contextValues.set(kIdentity, {
        role: 'tenant',
        tenant: cached.tenantId,
      })
      touchToken(deps.db, cached.tokenId)
      return next(req)
    }
    const resolved = await Tenants.resolveToken(deps.db, token)
    if (resolved.isErr()) {
      throw new ConnectError(
        `auth lookup failed: ${resolved.error}`,
        Code.Internal,
      )
    }
    if (resolved.value === null) {
      throw new ConnectError('invalid or revoked token', Code.Unauthenticated)
    }
    cache.put(sha, resolved.value)
    req.contextValues.set(kIdentity, {
      role: 'tenant',
      tenant: resolved.value.tenantId,
    })
    touchToken(deps.db, resolved.value.tokenId)
    return next(req)
  }
  return { interceptor, invalidate: () => cache.invalidateAll() }
}
