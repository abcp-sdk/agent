import type { ConnectRouter, ServiceImpl } from '@connectrpc/connect'
import type { AgentDeps } from '@abcp-agent/agent'
import { AgentService } from '@abcp-agent/schema'
import { configHandlers } from './handlers/config.js'
import { filesHandlers } from './handlers/files.js'
import { identityHandlers } from './handlers/identity.js'
import { messagesHandlers } from './handlers/messages.js'
import { presetsHandlers } from './handlers/presets.js'
import { providersHandlers } from './handlers/providers.js'
import { sessionsHandlers } from './handlers/sessions.js'
import { watchHandlers } from './handlers/watch.js'

export { resolveSessionDefaults } from './session-defaults.js'
export { providerToMsg } from './views.js'

/**
 * Build the Connect v2 AgentService routes for the fetch handler. The RPC
 * surface is composed from per-domain handler modules (sessions / messages /
 * watch / providers / presets / config / files); each module receives the
 * shared AgentDeps and contributes its slice of the ServiceImpl.
 */
export function buildConnectRoutes(
  deps: AgentDeps,
): (router: ConnectRouter) => void {
  return router => {
    const impl = {
      ...sessionsHandlers(deps),
      ...messagesHandlers(deps),
      ...watchHandlers(deps),
      ...providersHandlers(deps),
      ...presetsHandlers(deps),
      ...configHandlers(deps),
      ...filesHandlers(deps),
      ...identityHandlers(deps),
      // Domain slices are individually Partial-typed for contextual inference;
      // the composition is complete (every RPC) and verified by e2e.
    } as ServiceImpl<typeof AgentService>
    router.service(AgentService, impl)
  }
}
