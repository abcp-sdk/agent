import { Router } from '../http.js'
import { configRoutes } from './config.js'
import { fileRoutes } from './files.js'
import { llmRoutes } from './llm.js'
import { providerRoutes } from './providers.js'
import { sessionRoutes } from './sessions.js'
import { worksheetRoutes } from './worksheets.js'

/**
 * The REST facade router. Each sub-router's paths already carry their own
 * prefix (/sessions, /providers, /models, /presets, ...), so they all mount
 * at the root; index.ts serves the combined router under /api/v1.
 */
export function buildRoutes(): Router {
  const router = new Router()
  router.get('/health', async c =>
    c.json({ ok: true, name: 'easylab-agent' }, 200),
  )
  sessionRoutes(router)
  providerRoutes(router)
  configRoutes(router)
  llmRoutes(router)
  fileRoutes(router)
  worksheetRoutes(router)
  return router
}
