/**
 * Postgres smoke suite: boots the REAL agent binary against an external
 * Postgres (PG_SMOKE_DATABASE_URL) with an embedded NATS + a mock LLM and
 * drives the core chat flow — the sqlite e2e covers RPC breadth; this covers
 * the PG-specific code paths (postgres.js driver, SQL dialect, advisory
 * semantics in db-client) that unit tests never touch.
 *
 * Skips (exit 0) when PG_SMOKE_DATABASE_URL is unset, so `npm test`/local
 * runs stay sqlite-only while CI runs it against a postgres service.
 *
 * Env: PG_SMOKE_DATABASE_URL (required to run), E2E_AGENT_BIN,
 * ABC_NATS_SERVER_BIN / ABC_NATS_URL.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { start as startNats } from '@abc-protocol/sdk/natsrun/index.js'
import { create } from '@bufbuild/protobuf'
import { type Client, createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import {
  AgentService,
  CreateSessionRequestSchema,
  DeleteSessionRequestSchema,
  ListMessagesRequestSchema,
  PromptRequestSchema,
  RegisterProviderRequestSchema,
  ProviderModelSchema,
  ProviderSchema,
  UndoRequestSchema,
} from '@easylab-agent/schema'

const DB_URL = process.env.PG_SMOKE_DATABASE_URL ?? ''
let pass = 0
let fail = 0
const failures: string[] = []
const check = (n: string, ok: boolean, d = ''): void => {
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
  if (DB_URL === '') {
    console.log('[pg-smoke] PG_SMOKE_DATABASE_URL unset — skipping')
    return
  }
  const root = process.cwd()
  const bin = process.env.E2E_AGENT_BIN ?? join(root, '.sea', 'easylab-agent')
  if (!process.env.E2E_AGENT_BIN && !existsSync(bin)) {
    throw new Error(`agent binary not built: ${bin} (npm run build first)`)
  }

  const nats = await startNats({
    storage: 'memory',
    binary: process.env.ABC_NATS_SERVER_BIN ?? 'nats-server',
  })

  // Minimal OpenAI-compatible mock: every chat completion replies "PG-OK".
  const mockPort = 47000 + Math.floor(Math.random() * 2000)
  const mock = createServer((req, res) => {
    if (req.method !== 'POST' || !req.url?.endsWith('/chat/completions')) {
      res.writeHead(404)
      res.end('{}')
      return
    }
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(
      JSON.stringify({
        choices: [
          {
            index: 0,
            message: { role: 'assistant', content: 'PG-OK' },
            finish_reason: 'stop',
          },
        ],
      }),
    )
  })
  await new Promise<void>(r => mock.listen(mockPort, '127.0.0.1', () => r()))

  const httpPort = mockPort + 2000
  const sid = `pgsmoke-${Date.now().toString(36)}`
  const agent: ChildProcess = spawn(bin, [], {
    env: {
      ...process.env,
      PORT: String(httpPort),
      HTTP_PROTOCOL: 'h1',
      DB_BACKEND: 'pg',
      DATABASE_URL: DB_URL,
      NATS_URL: nats.url,
      LOG_LEVEL: 'warn',
      AGENT_AUTH_MODE: 'none',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  const alog: string[] = []
  agent.stdout?.on('data', d => alog.push(String(d)))
  agent.stderr?.on('data', d => alog.push(String(d)))

  const client: Client<typeof AgentService> = createClient(
    AgentService,
    createConnectTransport({ baseUrl: `http://127.0.0.1:${httpPort}`, httpVersion: '1.1' }),
  )

  try {
    for (let i = 0; i < 100; i++) {
      try {
        if ((await client.health({})).ok) break
      } catch {
        await sleep(200)
      }
    }
    check('health ok against PG', (await client.health({})).ok)

    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'openai',
          apiType: 'openai-compatible',
          baseUrl: `http://127.0.0.1:${mockPort}/v1`,
          apiKey: 'pg-smoke',
          models: [
            create(ProviderModelSchema, { id: 'pg-mock', name: 'PG Mock', contextLimit: 100000n }),
          ],
        }),
      }),
    )
    check('registerProvider (PG row)', true)

    await client.createSession(
      create(CreateSessionRequestSchema, { name: sid, model: 'openai/pg-mock', preset: 'default' }),
    )
    check('createSession (PG row)', true)

    const s = client.prompt(create(PromptRequestSchema, { id: sid, prompt: 'hello pg' }))
    for await (const e of s) if (e.event === 'accepted') break

    let reply = ''
    for (let i = 0; i < 60; i++) {
      const m = await client.listMessages(create(ListMessagesRequestSchema, { id: sid, limit: 50 }))
      const blob = JSON.stringify(m.messages)
      if (blob.includes('PG-OK')) {
        reply = 'PG-OK'
        break
      }
      await sleep(500)
    }
    check('prompt turn completed (chain + parts on PG)', reply === 'PG-OK')

    const undo = await client.undo(create(UndoRequestSchema, { id: sid }))
    check('undo moves the tip (PG)', undo.ok)
  } finally {
    try {
      await client.deleteSession(create(DeleteSessionRequestSchema, { id: sid }))
    } catch {
      /* best-effort cleanup */
    }
    agent.kill('SIGKILL')
    await new Promise<void>(r => mock.close(() => r()))
    await nats.stop()
  }

  console.log(`\n[pg-smoke] passed=${pass} failed=${fail}`)
  if (fail > 0) {
    console.log(`[pg-smoke] failures:\n  - ${failures.join('\n  - ')}`)
    console.log(alog.join('').split('\n').filter(Boolean).slice(-10).join('\n'))
    process.exit(1)
  }
}

void main()
