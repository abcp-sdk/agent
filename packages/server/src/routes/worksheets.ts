import { dispatchDecision, Worksheets } from '@easylab-agent/agent'
import { isRecord, type Router } from '../http.js'

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

export function worksheetRoutes(r: Router): void {
  r.get('/sessions/:id/worksheets', async c => {
    const { db } = c.deps
    const id = c.req.params['id'] ?? ''
    const status = c.req.query.get('status') ?? undefined
    const res = await Worksheets.listBySession(db, id, status)
    return res.isErr()
      ? c.json({ ok: false, error: res.error }, 500)
      : c.json({ worksheets: res.value.map(rowToJson) }, 200)
  })

  r.get('/worksheets', async c => {
    const { db } = c.deps
    const status = c.req.query.get('status') ?? undefined
    const res = await Worksheets.listByStatus(db, status ?? 'pending')
    return res.isErr()
      ? c.json({ ok: false, error: res.error }, 500)
      : c.json({ worksheets: res.value.map(rowToJson) }, 200)
  })

  r.post('/sessions/:id/worksheets/:wid/:decision', async c => {
    const deps = c.deps
    const id = c.req.params['id'] ?? ''
    const wid = c.req.params['wid'] ?? ''
    const decision = c.req.params['decision'] ?? ''
    if (decision !== 'approve' && decision !== 'reject') {
      return c.json(
        { ok: false, error: 'decision must be approve|reject' },
        400,
      )
    }
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
      const v: unknown = JSON.parse(row.value.args)
      if (isRecord(v)) args = v
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
}
