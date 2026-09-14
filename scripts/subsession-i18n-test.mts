/**
 * One-shot verifier for subsession handoff i18n. Boots the REAL agent binary
 * with a mock LLM, sets a session's locale, spawns a subsession, and asserts
 * the child's handed-off user message is written in that session's language.
 * Run with: npx tsx scripts/subsession-i18n-test.mts
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
  PromptRequestSchema,
  RegisterProviderRequestSchema,
  ProviderModelSchema,
  ProviderSchema,
  UpdateSettingsRequestSchema,
} from '@easylab-agent/schema'

const TOKEN = 'i18n-token'
const TENANT = 'i18n'
const sleep = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms))

interface Case {
  session: string
  child: string
  locale: string
  expect: string
}

const CASES: Case[] = [
  { session: 'i18n-zh', child: 'i18n-zh-c1', locale: 'zh-CN', expect: '发回父会话' },
  { session: 'i18n-en', child: 'i18n-en-c1', locale: 'en', expect: 'send your result back' },
]

async function main(): Promise<void> {
  const work = mkdtempSync(join(tmpdir(), 'subi18n-'))
  const nats = await startNats({
    storage: 'memory',
    binary: '/tmp/opencode/nats/bin/nats-server',
  })
  const mockPort = 49000 + Math.floor(Math.random() * 900)
  const mock = createHttpServer((req, res) => {
    if (req.method !== 'POST' || !req.url?.endsWith('/chat/completions')) {
      res.writeHead(404)
      res.end('{}')
      return
    }
    const chunks: Buffer[] = []
    req.on('data', c => chunks.push(c as Buffer))
    req.on('end', () => {
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}') as
        Record<string, unknown>
      const msgs = (body['messages'] ?? []) as Array<{ role?: string; content?: unknown }>
      const lastUser = [...msgs].reverse().find(m => m.role === 'user')
      const blob = JSON.stringify(lastUser?.content ?? '')
      const hasTool = msgs.some(m => m.role === 'tool')
      let text = 'PLAIN-OK'
      let tool: { name: string; args: unknown } | null = null
      const m = blob.match(/SUBSESSION_SPAWN:([\w-]+)/)
      if (m && !hasTool) {
        tool = {
          name: 'subsession-create',
          args: { name: m[1], prompt: 'Child task.' },
        }
      }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      const base = { id: 'c', object: 'chat.completion.chunk', created: 1, model: '' }
      const sse = (o: unknown): void => {
        res.write(`data: ${JSON.stringify(o)}\n\n`)
      }
      sse({ ...base, choices: [{ index: 0, delta: { role: 'assistant' } }] })
      if (tool) {
        sse({ ...base, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, id: 'x', type: 'function', function: { name: tool.name, arguments: '' } }] } }] })
        sse({ ...base, choices: [{ index: 0, delta: { tool_calls: [{ index: 0, function: { arguments: JSON.stringify(tool.args) } }] } }] })
        sse({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }] })
      } else {
        sse({ ...base, choices: [{ index: 0, delta: { content: text } }] })
        sse({ ...base, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })
      }
      res.write('data: [DONE]\n\n')
      res.end()
    })
  })
  await new Promise<void>(r => mock.listen(mockPort, '127.0.0.1', () => r()))

  const httpPort = mockPort + 1000
  const agent: ChildProcess = spawn('.sea/easylab-agent', [], {
    env: {
      ...process.env,
      PORT: String(httpPort),
      HTTP_PROTOCOL: 'h1',
      DB_BACKEND: 'sqlite',
      DATABASE_URL: `sqlite://${join(work, 'agent.db')}`,
      NATS_URL: nats.url,
      LOG_LEVEL: 'warn',
      AGENT_AUTH_MODE: 'required',
      AGENT_ADMIN_TOKEN: 'adm',
      AGENT_BOOTSTRAP_TENANT: TENANT,
      AGENT_BOOTSTRAP_TOKEN: TOKEN,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const client: Client<typeof AgentService> = createClient(
    AgentService,
    createConnectTransport({
      baseUrl: `http://127.0.0.1:${httpPort}`,
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
  for (let i = 0; i < 60; i++) {
    const t = await client.listTools({})
    if (t.tools.some(x => x.name === 'subsession-create')) break
    await sleep(200)
  }
  await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'openai',
        apiType: 'openai-compatible',
        baseUrl: `http://127.0.0.1:${mockPort}/v1`,
        apiKey: 'k',
        models: [
          create(ProviderModelSchema, { id: 'mock-i18n', name: 'Mock', contextLimit: 100000n }),
        ],
      }),
    }),
  )

  let pass = 0
  let fail = 0
  for (const c of CASES) {
    await client.createSession(
      create(CreateSessionRequestSchema, { name: c.session, model: 'openai/mock-i18n', preset: 'default' }),
    )
    await client.updateSettings(
      create(UpdateSettingsRequestSchema, { id: c.session, locale: c.locale }),
    )
    const s = client.prompt(
      create(PromptRequestSchema, { id: c.session, prompt: `SPAWN SUBSESSION_SPAWN:${c.child}` }),
    )
    for await (const e of s) if (e.event === 'accepted') break
    let handoff = ''
    for (let i = 0; i < 100; i++) {
      const cm = await client
        .listMessages(create(ListMessagesRequestSchema, { id: c.child, limit: 100 }))
        .catch(() => null)
      if (cm) {
        const blob = JSON.stringify(cm.messages)
        if (blob.includes('mail-send') && blob.includes(c.expect)) {
          handoff = blob
          break
        }
      }
      await sleep(200)
    }
    const ok = handoff.includes(c.expect)
    console.log(`  ${ok ? '✓' : '✗'} ${c.locale} handoff contains "${c.expect}"`)
    if (ok) pass++
    else {
      fail++
      const cm = await client
        .listMessages(create(ListMessagesRequestSchema, { id: c.child, limit: 100 }))
        .catch(() => null)
      console.log('    child msgs:', JSON.stringify(cm?.messages ?? []).slice(0, 500))
    }
  }

  agent.kill('SIGKILL')
  await new Promise<void>(r => mock.close(() => r()))
  await nats.stop()
  try {
    rmSync(work, { recursive: true, force: true })
  } catch {
    /* best-effort */
  }
  console.log(`\n[subi18n] passed=${pass} failed=${fail}`)
  process.exit(fail === 0 ? 0 : 1)
}

void main()
