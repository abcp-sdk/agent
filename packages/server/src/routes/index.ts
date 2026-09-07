import { createRoute, OpenAPIHono } from '@hono/zod-openapi'
import { z } from 'zod'
import type { AppEnv } from '../context.js'
import { configRoutes } from './config.js'
import { fileRoutes } from './files.js'
import { llmRoutes } from './llm.js'
import { providerRoutes } from './providers.js'
import { sessionRoutes } from './sessions.js'
import { worksheetRoutes } from './worksheets.js'

const healthRoute = createRoute({
  method: 'get',
  path: '/health',
  summary: 'Health check',
  responses: {
    200: {
      description: 'Health',
      content: {
        'application/json': {
          schema: z.object({ ok: z.boolean(), name: z.string() }),
        },
      },
    },
  },
})

export function buildRoutes() {
  const api = new OpenAPIHono<AppEnv>().openapi(healthRoute, async c => {
    return c.json({ ok: true, name: 'easylab-agent' }, 200)
  })

  const routed = api
    // Each sub-router's `createRoute` paths already carry their own prefix
    // (/sessions, /providers, /models, /presets, ...), so they all mount at
    // the root like the E1 framework does.
    .route('/', sessionRoutes)
    .route('/', providerRoutes)
    .route('/', configRoutes)
    .route('/', llmRoutes)
    .route('/', fileRoutes)
    .route('/', worksheetRoutes)

  return routed
}
