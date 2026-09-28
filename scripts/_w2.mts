import { createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import { AgentService, WatchSessionsRequestSchema } from '@abcp-agent/schema'
import { create } from '@bufbuild/protobuf'
const BASE=process.env['RT_BASE']!, TOKEN=process.env['RT_TOKEN']!, SID=process.env['RT_SID']!
const t=createConnectTransport({baseUrl:BASE,httpVersion:'1.1',nodeOptions:{rejectUnauthorized:false} as never,interceptors:[n=>r=>{r.header.set('authorization',`Bearer ${TOKEN}`);return n(r)}]})
const c=createClient(AgentService,t)
const ws=c.watchSessions(create(WatchSessionsRequestSchema,{}))
const start=Date.now()
for await(const ev of ws){const e=ev as any
  for(const s of (e.upserts??[])) if(s.name===SID) console.log('t+'+((Date.now()-start)/1000).toFixed(1)+'s status='+s.status)
  if(Date.now()-start>5000) process.exit(0)
}
