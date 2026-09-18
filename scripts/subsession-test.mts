/**
 * Focused verifier for the two new cross-session tools (runs the REAL agent
 * binary + a mock LLM). No full e2e; boots once, drives exactly the subsession
 * flow and exits. Mirrors the style of item-test.mts:
 *
 *   1. parent session (model mock-sub) prompts with the SPAWN marker
 *   2. the model calls `subsession-create`, which O(1)-forks a child
 *      (shared tip, inherited model/preset, group = parent name)
 *   3. the child is woken via mailbox, its model calls `mail-send` back to the
 *      parent with the result, then ends its turn
 *   4. the parent is resumed and now holds the child's result
 *   5. re-spawning the same child name is refused (no duplicate session)
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer as createHttpServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { start as startNats } from '@abc-protocol/sdk/natsrun/index.js'
import { create } from '@bufbuild/protobuf'
import { type Client, createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import {
  AgentService,
  CreateSessionRequestSchema,
  GetSessionRequestSchema,
  ListMessagesRequestSchema,
  ListSessionsRequestSchema,
  PromptRequestSchema,
  RegisterProviderRequestSchema,
  ProviderModelSchema,
  ProviderSchema,
} from '@abcp-agent/schema'

const TOKEN = 'sub-tenant-token'
const TENANT = 'sub'
const PARENT = 'sub-parent'
const CHILD = 'sub-child'
let pass = 0
let fail = 0
const failures: string[] = []
const item = (n: string, ok: boolean, d = ''): void => {
  if (ok) {
    pass++
    console.log(`  ✓ ${n}`)
  } else {
    fail++
    failures.push(n)
    console.log(`  ✗ ${n}${d ? ` — ${d}` : ''}`)
  }
}
const sleep = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms))

async function main(): Promise<void> {
  const work = mkdtempSync(join(tmpdir(), 'subsess-'))
  const nats = await startNats({
    storage: 'memory',
  })

  // Mock OpenAI-compatible LLM: model `mock-sub` whose behavior is driven by
  // markers in the prompt (child sees MAIL_SEND_RESULT, parent sees
  // SUBSESSION_SPAWN). Endpoints only need /chat/completions.
  const mockPort = 47000 + Math.floor(Math.random() * 2000)
  const mock = createHttpServer((req, res) => {
    if (req.method !== 'POST' || !req.url?.endsWith('/chat/completions')) {
      res.writeHead(404)
      res.end('{}')
      return
    }
    const chunks: Buffer[] = []
    req.on('data', c => chunks.push(c as Buffer))
    req.on('end', () => {
      const body = JSON.parse(
        Buffer.concat(chunks).toString('utf8') || '{}',
      ) as Record<string, unknown>
      const msgs = (body['messages'] ?? []) as Array<{
        role?: string
        content?: unknown
      }>
      // A subsession inherits the parent's WHOLE chain, so the parent's markers
      // are still present in the child's request. Key on the LAST user message
      // only — that is unambiguously "this session's own instruction".
      const lastUser = [...msgs].reverse().find(m => m.role === 'user')
      const blob = JSON.stringify(lastUser?.content ?? '')
      const hasTool = msgs.some(m => m.role === 'tool')
      // Decide the reply: a tool call (first pass) or a final text (after the
      // tool result came back).
      let text = 'PLAIN-OK'
      let tool: { name: string; args: unknown } | null = null
      if (blob.includes('MAIL_SEND_RESULT')) {
        if (hasTool) text = 'CHILD-DONE'
        else
          tool = {
            name: 'mail-send',
            args: { to: PARENT, text: 'CHILD-RESULT-42' },
          }
      } else if (blob.includes('SUBSESSION_SPAWN')) {
        if (hasTool) text = 'PARENT-DONE'
        else
          tool = {
            name: 'subsession-create',
            args: {
              name: CHILD,
              prompt: `Child task. MAIL_SEND_RESULT to ${PARENT} when done.`,
            },
          }
      }
      // The agent always uses streamText (stream:true); answer in kind.
      if (body['stream'] !== true) {
        const message = tool
          ? {
              role: 'assistant',
              content: null,
              tool_calls: [
                {
                  id: 'call_mock_1',
                  type: 'function',
                  function: {
                    name: tool.name,
                    arguments: JSON.stringify(tool.args),
                  },
                },
              ],
            }
          : { role: 'assistant', content: text }
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ choices: [{ index: 0, message }] }))
        return
      }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      const base = {
        id: 'chatcmpl-1',
        object: 'chat.completion.chunk',
        created: 1,
        model: String(body['model'] ?? ''),
      }
      const sse = (o: unknown): void => {
        res.write(`data: ${JSON.stringify(o)}\n\n`)
      }
      sse({ ...base, choices: [{ index: 0, delta: { role: 'assistant' } }] })
      if (tool) {
        sse({
          ...base,
          choices: [
            {
              index: 0,
              delta: {
                tool_calls: [
                  {
                    index: 0,
                    id: 'call_mock_1',
                    type: 'function',
                    function: { name: tool.name, arguments: '' },
                  },
                ],
              },
            },
          ],
        })
        sse({
          ...base,
          choices: [
            {
              index: 0,
              delta: {
                tool_calls: [
                  { index: 0, function: { arguments: JSON.stringify(tool.args) } },
                ],
              },
            },
          ],
        })
        sse({
          ...base,
          choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }],
        })
      } else {
        sse({ ...base, choices: [{ index: 0, delta: { content: text } }] })
        sse({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })
      }
      sse({
        ...base,
        choices: [],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      })
      res.write('data: [DONE]\n\n')
      res.end()
    })
  })
  await new Promise<void>(r => mock.listen(mockPort, '127.0.0.1', () => r()))
  const mockUrl = `http://127.0.0.1:${mockPort}/v1`

  const httpPort = mockPort + 2000
  const alog: string[] = []
  const agent: ChildProcess = spawn('.sea/abcp-agent', [], {
    env: {
      ...process.env,
      PORT: String(httpPort),
      HTTP_PROTOCOL: 'h1',
      DB_BACKEND: 'sqlite',
      DATABASE_URL: `sqlite://${join(work, 'agent.db')}`,
      NATS_URL: nats.url,
      LOG_LEVEL: 'debug',
      AGENT_AUTH_MODE: 'required',
      AGENT_ADMIN_TOKEN: 'adm',
      AGENT_BOOTSTRAP_TENANT: TENANT,
      AGENT_BOOTSTRAP_TOKEN: TOKEN,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  agent.stdout?.on('data', d => alog.push(String(d)))
  agent.stderr?.on('data', d => alog.push(String(d)))

  const baseUrl = `http://127.0.0.1:${httpPort}`
  const client: Client<typeof AgentService> = createClient(
    AgentService,
    createConnectTransport({
      baseUrl,
      httpVersion: '1.1',
      interceptors: [
        next => async req => {
          req.header.set('authorization', `Bearer ${TOKEN}`)
          return next(req)
        },
      ],
    }),
  )

  for (let i = 0; i < 100; i++) {
    try {
      if ((await client.health({})).ok) break
    } catch {
      await sleep(200)
    }
  }
  // Wait for the bundled tools to register.
  for (let i = 0; i < 60; i++) {
    const t = await client.listTools({})
    if (t.tools.some(x => x.name === 'subsession-create')) break
    await sleep(200)
  }
  console.log('\nsubsession tools')
  const tools = await client.listTools({})
  const names = tools.tools.map(t => t.name)
  item('subsession-create registered', names.includes('subsession-create'))
  item('mail-send registered', names.includes('mail-send'))

  await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'openai',
        apiType: 'openai-compatible',
        baseUrl: mockUrl,
        apiKey: 'k',
        models: [
          create(ProviderModelSchema, {
            id: 'mock-sub',
            name: 'Mock Sub',
            contextLimit: 100000n,
          }),
        ],
      }),
    }),
  )
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: PARENT,
      model: 'openai/mock-sub',
      preset: 'default',
    }),
  )

  // 1. Spawn.
  const s1 = client.prompt(
    create(PromptRequestSchema, { id: PARENT, prompt: 'SPAWN: SUBSESSION_SPAWN' }),
  )
  for await (const e of s1) if (e.event === 'accepted') break

  // 2. Child exists, with group = parent and inherited model.
  console.log('\nsubsession fork')
  let child: import('@abcp-agent/schema').Session | undefined
  for (let i = 0; i < 80; i++) {
    try {
      const g = await client.getSession(create(GetSessionRequestSchema, { id: CHILD }))
      if (g.session) {
        child = g.session
        break
      }
    } catch {
      /* not yet */
    }
    await sleep(200)
  }
  item('child session created', child !== undefined)
  item('child.group == parent name', child?.group === PARENT, child?.group)
  item('child inherits model', child?.model === 'openai/mock-sub', child?.model)
  item('child inherits preset', child?.preset === 'default', child?.preset)

  // 3. Parent receives the child's mailed result.
  console.log('\nmail-send child -> parent')
  let sawResult = false
  for (let i = 0; i < 400; i++) {
    const m = await client.listMessages(
      create(ListMessagesRequestSchema, { id: PARENT, limit: 50 }),
    )
    if (JSON.stringify(m.messages).includes('CHILD-RESULT-42')) {
      sawResult = true
      break
    }
    await sleep(200)
  }
  if (!sawResult) {
    const pm = await client.listMessages(
      create(ListMessagesRequestSchema, { id: PARENT, limit: 50 }),
    )
    const cm = await client
      .listMessages(create(ListMessagesRequestSchema, { id: CHILD, limit: 50 }))
      .catch(() => ({ messages: [] as unknown[] }))
    console.log('  [debug] parent msgs:', JSON.stringify(pm.messages).slice(0, 600))
    console.log('  [debug] child msgs:', JSON.stringify(cm.messages).slice(0, 600))
  }
  item('parent received CHILD-RESULT-42 via mail-send', sawResult)

  // 4. Duplicate child name refused.
  console.log('\nnesting / duplicate guard')
  const s2 = client.prompt(
    create(PromptRequestSchema, { id: PARENT, prompt: 'AGAIN: SUBSESSION_SPAWN' }),
  )
  for await (const e of s2) if (e.event === 'accepted') break
  await sleep(1500)
  const all = await client.listSessions(create(ListSessionsRequestSchema, {}))
  item(
    'duplicate child name not created',
    all.sessions.filter(s => s.name === CHILD).length === 1,
  )

  agent.kill('SIGKILL')
  await new Promise<void>(r => mock.close(() => r()))
  await nats.stop()
  try {
    rmSync(work, { recursive: true, force: true })
  } catch {
    /* best-effort */
  }
  console.log(`\n[subsess] passed=${pass} failed=${fail}`)
  if (fail > 0) {
    console.log(`[subsess] failures:\n  - ${failures.join('\n  - ')}`)
    console.log(alog.join('').split('\n').filter(Boolean).slice(-15).join('\n'))
  }
  process.exit(fail === 0 ? 0 : 1)
}


void main()
