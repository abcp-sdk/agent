/**
 * Item-by-item verification (per user instruction: NO full e2e runs).
 * Boots the stack ONCE and checks each previously-failing item individually:
 *   A. nats stream pre-creation (natsrun waitReady fix)
 *   B. auth gate (401 without token)
 *   C. presets (default preset + systemPrompt)
 *   D. one LLM turn: reasoning/text streaming, turn-complete, persistence
 *   E. agent log clean of JetStreamNotEnabled
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
  ListMessagesRequestSchema,
  ListPresetsRequestSchema,
  ListSessionsRequestSchema,
  PromptRequestSchema,
  RegisterProviderRequestSchema,
  WatchSessionRequestSchema,
  ProviderModelSchema,
  ProviderSchema,
  RegisterProviderRequestSchema as _R,
} from '@abcp-agent/schema'

const ADMIN = 'item-admin-token'
const TENANT = 'item'
const TOKEN = 'item-tenant-token'
let pass = 0
let fail = 0
const failures: string[] = []
const item = (name: string, ok: boolean, detail = ''): void => {
  if (ok) {
    pass++
    console.log(`  ✓ ${name}`)
  } else {
    fail++
    failures.push(name)
    console.log(`  ✗ ${name}${detail === '' ? '' : ` — ${detail}`}`)
  }
}
const sleep = (ms: number): Promise<void> =>
  new Promise(r => setTimeout(r, ms))

async function main(): Promise<void> {
  const work = mkdtempSync(join(tmpdir(), 'agent-item-'))
  const nats = await startNats({
    storage: 'memory',
  })
  console.log(`[item] nats ${nats.url}`)

  // --- A. stream pre-creation ---
  console.log('\nA. natsrun stream pre-creation')
  {
    const { connect } = await import('@nats-io/transport-node')
    const nc = await connect({ servers: nats.url })
    const raw = await nc.request(
      '$JS.API.STREAM.NAMES',
      new TextEncoder().encode(''),
      { timeout: 3000 },
    )
    const names = (JSON.parse(new TextDecoder().decode(raw.data))
      .streams ?? []) as string[]
    item('ABC_MAILBOX pre-created', names.includes('ABC_MAILBOX'), String(names))
    item('ABC_EVENTS pre-created', names.includes('ABC_EVENTS'))
    item('ABC_DLQ pre-created', names.includes('ABC_DLQ'))
    await nc.close()
  }

  // --- minimal mock LLM (openai-compatible, model mock-text) ---
  const mockPort = 40000 + Math.floor(Math.random() * 10000)
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
      const model = String(body['model'] ?? '')
      const base = {
        id: 'chatcmpl-1',
        object: 'chat.completion.chunk',
        created: 1,
        model,
      }
      if (body['stream'] !== true) {
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(
          JSON.stringify({
            ...base,
            object: 'chat.completion',
            choices: [
              { index: 0, message: { role: 'assistant', content: 'ok' }, finish_reason: 'stop' },
            ],
            usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
          }),
        )
        return
      }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      const sse = (o: unknown): void => {
        res.write(`data: ${JSON.stringify(o)}\n\n`)
      }
      sse({ ...base, choices: [{ index: 0, delta: { reasoning_content: 'thinking about it' } }] })
      sse({ ...base, choices: [{ index: 0, delta: { reasoning_content: null } }] })
      sse({ ...base, choices: [{ index: 0, delta: { content: 'HELLO-E2E' } }] })
      sse({
        ...base,
        choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
      })
      res.write('data: [DONE]\n\n')
      res.end()
    })
  })
  await new Promise<void>(r => mock.listen(mockPort, '127.0.0.1', () => r()))
  const mockUrl = `http://127.0.0.1:${mockPort}/v1`
  console.log(`[item] mock llm ${mockUrl}`)

  // --- agent (SEA binary) ---
  const httpPort = mockPort + 1
  const agentLog: string[] = []
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
      AGENT_ADMIN_TOKEN: ADMIN,
      AGENT_BOOTSTRAP_TENANT: TENANT,
      AGENT_BOOTSTRAP_TOKEN: TOKEN,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  agent.stdout?.on('data', d => agentLog.push(String(d)))
  agent.stderr?.on('data', d => agentLog.push(String(d)))

  const baseUrl = `http://127.0.0.1:${httpPort}`
  const transport = createConnectTransport({
    baseUrl,
    httpVersion: '1.1',
    interceptors: [
      next => async req => {
        req.header.set('authorization', `Bearer ${TOKEN}`)
        return next(req)
      },
    ],
  })
  const client: Client<typeof AgentService> = createClient(
    AgentService,
    transport,
  )

  // --- boot wait ---
  let healthy = false
  for (let i = 0; i < 100; i++) {
    try {
      const h = await client.health({})
      if (h.ok) {
        healthy = true
        break
      }
    } catch {
      await sleep(200)
    }
  }
  console.log('\nB. boot + auth gate')
  item('health ok (public route, no token)', healthy)
  {
    let rejected = false
    try {
      const anon = createClient(
        AgentService,
        createConnectTransport({ baseUrl, httpVersion: '1.1' }),
      )
      await anon.listSessions(create(ListSessionsRequestSchema, {}))
    } catch (e) {
      rejected = String(e).toLowerCase().includes('unauthenticated')
    }
    item('listSessions without token -> 401', rejected)
  }

  // --- C. presets ---
  console.log('\nC. presets')
  const presets = await client.listPresets(create(ListPresetsRequestSchema, {}))
  const def = presets.presets.find(p => p.id === 'default')
  item('default preset present', def !== undefined)
  item('listPresets returns a systemPrompt', (def?.systemPrompt ?? '').length > 0)

  // --- D. one turn ---
  console.log('\nD. single LLM turn')
  const reg = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'openai',
        apiType: 'openai-compatible',
        baseUrl: mockUrl,
        apiKey: 'k',
        models: [
          create(ProviderModelSchema, {
            id: 'mock-text',
            name: 'Mock Text',
            contextLimit: 100000n,
          }),
        ],
      }),
    }),
  )
  item('registerProvider ok', reg.ok)
  const sid = `item-${Date.now()}`
  const created = await client.createSession(
    create(CreateSessionRequestSchema, {
      name: sid,
      model: 'openai/mock-text',
      preset: 'default',
    }),
  )
  item('createSession ok', created.ok)

  const events: Array<{ event: string; params: Record<string, unknown> }> = []
  const ac = new AbortController()
  const watcher = (async () => {
    try {
      const stream = client.watchSession(
        create(WatchSessionRequestSchema, { id: sid }),
        { signal: ac.signal },
      )
      for await (const ev of stream) {
        events.push({
          event: ev.event,
          params: (ev.params ?? {}) as Record<string, unknown>,
        })
      }
    } catch {
      /* aborted */
    }
  })()
  await sleep(300)
  const promptStream = client.prompt(
    create(PromptRequestSchema, { id: sid, prompt: 'say hello' }),
  )
  for await (const e of promptStream) {
    if (e.event === 'accepted') break
  }
  const deadline = Date.now() + 30000
  while (
    !events.some(e => e.event === 'turn-complete') &&
    Date.now() < deadline
  ) {
    await sleep(100)
  }
  item('reasoning-start observed', events.some(e => e.event === 'reasoning-start'))
  item('text-start observed', events.some(e => e.event === 'text-start'))
  const text = events
    .filter(e => e.event === 'text-delta')
    .map(e => String(e.params['text'] ?? ''))
    .join('')
  item('text streamed HELLO-E2E', text.includes('HELLO-E2E'), text)
  item('turn-complete observed', events.some(e => e.event === 'turn-complete'))
  ac.abort()
  await watcher.catch(() => {})

  const msgs = await client.listMessages(
    create(ListMessagesRequestSchema, { id: sid, limit: 50 }),
  )
  const assistant = msgs.messages.find(m => m.role === 'assistant')
  item('assistant message persisted', assistant !== undefined)
  const textPart = assistant?.parts.find(p => p.type === 'text')
  item(
    'persisted text part carries HELLO-E2E',
    String(textPart?.data ?? '').includes('HELLO-E2E'),
  )

  // --- E. log scan ---
  console.log('\nE. agent log')
  await sleep(300)
  const log = agentLog.join('')
  item(
    'no JetStreamNotEnabled in agent log',
    !log.includes('JetStreamNotEnabled'),
  )
  item('no mailbox purge FK mismatch in agent log', !log.includes('foreign key mismatch'))
  const errLines = log.split('\n').filter(l => /JetStreamNotEnabled|jetstream|level":50|level":60|err/i.test(l) && !/ExperimentalWarning/.test(l))
  console.log('  --- agent error lines ---')
  for (const l of errLines.slice(0, 15)) console.log('  ' + l.slice(0, 300))
  console.log('  --- agent log head ---')
  for (const l of agentLog.join('').split('\n').slice(0, 40)) console.log('  ' + l.slice(0, 250))

  // --- cleanup ---
  agent.kill('SIGKILL')
  await new Promise<void>(r => mock.close(() => r()))
  await nats.stop()
  try {
    rmSync(work, { recursive: true, force: true })
  } catch {
    /* best-effort */
  }
  console.log(`\n[item] passed=${pass} failed=${fail}`)
  if (fail > 0) console.log(`[item] failures:\n  - ${failures.join('\n  - ')}`)
  process.exit(fail === 0 ? 0 : 1)
}

void main()
