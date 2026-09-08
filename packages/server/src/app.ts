import { buildRoutes } from './routes/index.js'

/**
 * The REST facade router (served under /api/v1 by index.ts). RPC traffic is
 * fully separate: index.ts mounts the Connect AgentService through
 * @connectrpc/connect-node, so no routing framework is involved on the RPC
 * path at all.
 */
export function buildApp() {
  return buildRoutes()
}
