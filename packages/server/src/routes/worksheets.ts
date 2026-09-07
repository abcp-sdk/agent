import { createRoute, OpenAPIHono } from '@hono/zod-openapi'
import { dispatchDecision, Worksheets } from '@easylab-agent/agent'
import { WorksheetRowSchema } from '@easylab-agent/schema'
import { z } from 'zod'
import type { AppEnv } from '../context.js'

const ErrorSchema = z.object({ ok: z.boolean(), error: z.string() })

const listSessionWorksheetsRoute = createRoute({
  method: 'get',
  path: '/sessions/{id}/worksheets',
  summary: 'List worksheets for a session',
  request: {
    params: z.object({ id: z.string() }),
    query: z.object({ status: z.string().optional() }),
  },
  responses: {
    200: {
      description: 'Worksheets',
      content: {
        'application/json': {
          schema: z.object({ worksheets: z.array(WorksheetRowSchema) }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const listWorksheetsRoute = createRoute({
  method: 'get',
  path: '/worksheets',
  summary: 'List worksheets across sessions (global approval inbox)',
  request: { query: z.object({ status: z.string().optional() }) },
  responses: {
    200: {
      description: 'Worksheets',
      content: {
        'application/json': {
          schema: z.object({ worksheets: z.array(WorksheetRowSchema) }),
        },
      },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

const decideRoute = createRoute({
  method: 'post',
  path: '/sessions/{id}/worksheets/{wid}/{decision}',
  summary: 'Approve or reject a worksheet',
  request: {
    params: z.object({
      id: z.string(),
      wid: z.string(),
      decision: z.enum(['approve', 'reject']),
    }),
  },
  responses: {
    200: {
      description: 'Decided',
      content: {
        'application/json': { schema: z.object({ ok: z.boolean() }) },
      },
    },
    404: {
      description: 'Not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    409: {
      description: 'Not pending',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    502: {
      description: 'Extension dispatch failed',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

function rowToJson(w: {
  id: string
  session_name: string
  ext_id: string
  action: string
  args: string
  title: string
  origin_call_id: string | null
  status: string
  created_at: string
  decided_at: string | null
}) {
  return w
}

export const worksheetRoutes = new OpenAPIHono<AppEnv>()
  .openapi(listSessionWorksheetsRoute, async c => {
    const { db } = c.get('deps')
    const { id } = c.req.valid('param')
    const { status } = c.req.valid('query')
    const r = await Worksheets.listBySession(db, id, status)
    return r.isErr()
      ? c.json({ ok: false, error: r.error }, 500)
      : c.json({ worksheets: r.value.map(rowToJson) }, 200)
  })
  .openapi(listWorksheetsRoute, async c => {
    const { db } = c.get('deps')
    const { status } = c.req.valid('query')
    const r = await Worksheets.listByStatus(db, status ?? 'pending')
    return r.isErr()
      ? c.json({ ok: false, error: r.error }, 500)
      : c.json({ worksheets: r.value.map(rowToJson) }, 200)
  })
  .openapi(decideRoute, async c => {
    const deps = c.get('deps')
    const { id, wid, decision } = c.req.valid('param')
    const row = await Worksheets.get(deps.db, wid)
    if (row.isErr()) return c.json({ ok: false, error: row.error }, 500)
    if (row.value === null || row.value.session_name !== id) {
      return c.json({ ok: false, error: 'worksheet not found' }, 404)
    }
    // CAS claim: exactly one concurrent decision wins.
    const claimed = await Worksheets.claimForDispatch(deps.db, wid)
    if (claimed.isErr()) return c.json({ ok: false, error: claimed.error }, 500)
    if (claimed.value === null) {
      return c.json({ ok: false, error: 'worksheet is not pending' }, 409)
    }
    let args: Record<string, unknown> = {}
    try {
      const v = JSON.parse(row.value.args)
      if (typeof v === 'object' && v !== null) args = v as Record<string, unknown>
    } catch {
      args = {}
    }
    const err = await dispatchDecision(
      deps,
      wid,
      row.value.session_name,
      row.value.ext_id,
      row.value.action,
      args,
      decision,
    )
    if (err !== null && decision === 'approve') {
      // Reject stays rejected (nothing to re-execute); approve rolls back so
      // the user can retry once the extension is reachable again.
      const back = await Worksheets.rollbackToPending(deps.db, wid)
      if (back.isErr()) {
        return c.json({ ok: false, error: back.error }, 500)
      }
      return c.json({ ok: false, error: err }, 502)
    }
    return c.json({ ok: true }, 200)
  })
