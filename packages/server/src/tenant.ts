import type { HandlerContext } from '@connectrpc/connect'
import { identityOf } from './auth.js'

/**
 * Tenant identity helpers for the Connect surface (protocol v2 multi-tenancy).
 *
 * The tenant is derived from the VERIFIED bearer token by the auth interceptor
 * (`auth.ts`) and stored in the request context. There is no client-supplied
 * tenancy header: a client cannot name a tenant, only prove one via its token.
 */

const TENANT_RE = /^[A-Za-z0-9_-]{1,64}$/

/** The deployment-wide fallback tenant (only used when AGENT_AUTH_MODE=none). */
export function defaultTenant(env: NodeJS.ProcessEnv = process.env): string {
  const v = env['AGENT_DEFAULT_TENANT']
  const value = v !== undefined && v !== '' ? v : 'default'
  if (!TENANT_RE.test(value)) {
    throw new Error(
      `AGENT_DEFAULT_TENANT ${JSON.stringify(value)} is not a valid tenant id`,
    )
  }
  return value
}

/** Validate a tenant id, or throw (mapped to InvalidArgument by the caller). */
export function validateTenant(raw: string): string {
  if (!TENANT_RE.test(raw)) {
    throw new Error(
      `invalid tenant ${JSON.stringify(raw)}: must match [A-Za-z0-9_-]{1,64}`,
    )
  }
  return raw
}

/**
 * The tenant of a request, from the verified identity. A missing identity means
 * the request was not authenticated — callers on the AgentService path are
 * already gated by the auth interceptor, so this only trips if the routes were
 * mounted without it.
 */
export function tenantOf(ctx: HandlerContext): string {
  const id = identityOf(ctx)
  if (id.role === 'admin') {
    // Admin calls the tenant surface explicitly; no implicit tenant.
    throw new Error('admin identity has no tenant; use the admin surface')
  }
  if (id.tenant === '') {
    throw new Error('unauthenticated: no tenant resolved for this request')
  }
  return validateTenant(id.tenant)
}
