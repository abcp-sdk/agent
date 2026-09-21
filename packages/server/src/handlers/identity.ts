import { type AgentDeps, Tenants } from '@abcp-agent/agent'
import type { AgentService } from '@abcp-agent/schema'
import type { HandlerContext, ServiceImpl } from '@connectrpc/connect'
import { identityOf } from '../auth.js'

/**
 * Identity handler: report the caller's resolved identity from its verified
 * bearer token. The webui uses it to show a human username instead of the
 * fixed same-origin URL. A tenant token can only ever resolve its own
 * identity; an admin token resolves role=admin with empty tenant fields.
 */
export function identityHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async getIdentity(_req, ctx: HandlerContext) {
      const id = identityOf(ctx)
      if (id.role === 'admin') {
        return { tenant: '', tenantName: '', role: 'admin' }
      }
      // tenantOf() would throw for admin; here a tenant identity always has an
      // id. A missing row (should not happen for an authenticated token) falls
      // back to the id so the client still has something to show.
      const row = await Tenants.get(deps.db, id.tenant)
      const name = row.isOk() && row.value !== null ? row.value.name : ''
      return {
        tenant: id.tenant,
        tenantName: name !== '' ? name : id.tenant,
        role: 'tenant',
      }
    },
  }
}
