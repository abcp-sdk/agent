import { Code, ConnectError } from '@connectrpc/connect'
import type { ConnectRouter, HandlerContext } from '@connectrpc/connect'
import type { AgentDeps } from '@easylab-agent/agent'
import { Tenants, type TenantRow, type TenantTokenRow } from '@easylab-agent/agent'
import {
  AdminService,
  CreateTenantRequestSchema,
  DeleteTenantRequestSchema,
  IssueTenantTokenRequestSchema,
  ListTenantsRequestSchema,
  ListTenantTokensRequestSchema,
  RevokeTenantTokenRequestSchema,
  RotateTenantTokenRequestSchema,
  UpdateTenantRequestSchema,
} from '@easylab-agent/schema'
import { identityOf } from './auth.js'

/**
 * AdminService implementation: tenant + tenant-token lifecycle. Every RPC
 * requires the static admin token (enforced by the auth interceptor, which
 * refuses tenant tokens on this service). `invalidate` drops the auth token
 * cache after any mutation so revocation/rotation takes effect immediately.
 */

const TENANT_RE = /^[A-Za-z0-9_-]{1,64}$/

function requireAdmin(ctx: HandlerContext): void {
  if (identityOf(ctx).role !== 'admin') {
    throw new ConnectError('admin token required', Code.PermissionDenied)
  }
}

function tenantMsg(t: TenantRow) {
  return {
    id: t.id,
    name: t.name,
    disabled: t.disabled,
    createdAt: t.createdAt,
    updatedAt: t.updatedAt,
  }
}

function tokenMsg(t: TenantTokenRow) {
  return {
    tokenId: t.tokenId,
    tenantId: t.tenantId,
    label: t.label,
    createdAt: t.createdAt,
    lastUsedAt: t.lastUsedAt ?? '',
    revoked: t.revoked,
  }
}

export function buildAdminRoutes(
  deps: AgentDeps,
  invalidateAuthCache: () => void,
): (router: ConnectRouter) => void {
  return router => {
    router.service(AdminService, {
      async listTenants(_req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const r = await Tenants.list(deps.db)
        if (r.isErr()) throw new Error(r.error)
        return { tenants: r.value.map(tenantMsg) }
      },

      async createTenant(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const id = req.id.trim()
        if (!TENANT_RE.test(id)) {
          throw new ConnectError(
            `invalid tenant id ${JSON.stringify(req.id)}: must match [A-Za-z0-9_-]{1,64}`,
            Code.InvalidArgument,
          )
        }
        const existing = await Tenants.get(deps.db, id)
        if (existing.isErr()) throw new Error(existing.error)
        if (existing.value !== null) {
          throw new ConnectError(`tenant already exists: ${id}`, Code.AlreadyExists)
        }
        const created = await Tenants.create(deps.db, id, req.name)
        if (created.isErr()) throw new Error(created.error)
        // Bootstrap token for the new tenant (returned once).
        const issued = await Tenants.issueToken(deps.db, id, 'bootstrap')
        if (issued.isErr()) throw new Error(issued.error)
        invalidateAuthCache()
        return {
          tenant: tenantMsg(created.value),
          token: issued.value.plaintext,
        }
      },

      async updateTenant(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const id = req.id
        const patch: { name?: string | undefined; disabled?: boolean | undefined } =
          {}
        if (req.name !== undefined) patch.name = req.name
        if (req.disabled !== undefined) patch.disabled = req.disabled
        const r = await Tenants.update(deps.db, id, patch)
        if (r.isErr()) throw new Error(r.error)
        const after = await Tenants.get(deps.db, id)
        if (after.isErr()) throw new Error(after.error)
        if (after.value === null) {
          throw new ConnectError(`tenant not found: ${id}`, Code.NotFound)
        }
        if (req.disabled !== undefined) invalidateAuthCache()
        return { tenant: tenantMsg(after.value) }
      },

      /**
       * Soft-disable a tenant. Its tokens stop authenticating; data is kept.
       * We never hard-delete tenant data (mirrors "只禁用").
       */
      async deleteTenant(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const r = await Tenants.disable(deps.db, req.id)
        if (r.isErr()) throw new Error(r.error)
        invalidateAuthCache()
        return { ok: true }
      },

      async issueTenantToken(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const exists = await Tenants.get(deps.db, req.tenantId)
        if (exists.isErr()) throw new Error(exists.error)
        if (exists.value === null) {
          throw new ConnectError(
            `tenant not found: ${req.tenantId}`,
            Code.NotFound,
          )
        }
        const r = await Tenants.issueToken(deps.db, req.tenantId, req.label)
        if (r.isErr()) throw new Error(r.error)
        return {
          token: tokenMsg(r.value.token),
          plaintext: r.value.plaintext,
        }
      },

      async listTenantTokens(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const r = await Tenants.listTokens(deps.db, req.tenantId)
        if (r.isErr()) throw new Error(r.error)
        return { tokens: r.value.map(tokenMsg) }
      },

      async revokeTenantToken(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const r = await Tenants.revokeToken(deps.db, req.tokenId)
        if (r.isErr()) throw new Error(r.error)
        invalidateAuthCache()
        return { ok: true }
      },

      async rotateTenantToken(req, ctx: HandlerContext) {
        requireAdmin(ctx)
        const r = await Tenants.rotateToken(deps.db, req.tokenId)
        if (r.isErr()) throw new Error(r.error)
        invalidateAuthCache()
        return {
          token: tokenMsg(r.value.token),
          plaintext: r.value.plaintext,
        }
      },
    })
  }
}
