import { create } from '@bufbuild/protobuf'
import { createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import { AgentService, PromptRequestSchema } from '@abcp-agent/schema'
const t = createConnectTransport({ baseUrl: 'http://agent.agent.svc.cluster.local', httpVersion: '1.1' })
const c = createClient(AgentService, t)
console.log('LONG START', Date.now())
for await (const e of c.prompt(create(PromptRequestSchema, { id: 'loctest1', prompt: 'Run exactly this one command and nothing else: sleep 25; echo LONG-DONE' }), { headers: { Authorization: 'Bearer devtenanttoken' } })) {}
console.log('LONG ACCEPTED', Date.now())
