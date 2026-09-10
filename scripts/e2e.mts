/**
 * End-to-end test for the standalone agent.
 *
 * Boots the REAL agent binary (`.sea/easylab-agent`, built by `npm run build`)
 * against an ephemeral NATS + sqlite DB, with a local mock OpenAI-compatible
 * LLM upstream, then drives EVERY agent.v1 RPC over Connect and asserts the
 * observable behaviour:
 *
 *   - health / session CRUD (create/get/list/rename/fork/undo/delete)
 *   - a full turn: reasoning + text streaming, persistence, message_seq
 *   - tool calling (bundled `todo-write`) + tool-result persistence
 *   - watchSession (live ordered stream) + watchSessions (snapshot/upsert/remove)
 *   - providers (register/list/models+variants/test/delete)
 *   - presets (list/upsert/preview/delete)
 *   - config (get/set), tool config, extension config
 *   - tools discovery + i18n
 *   - files (upload/ingest/get/meta)
 *   - state / mailbox / interrupt / compact / getAgentConfig
 *
 * Run: `npm run e2e` (which builds first) or `tsx scripts/e2e.mts`.
 * Env: ABC_NATS_SERVER_BIN / ABC_NATS_URL, E2E_AGENT_BIN (default
 * `.sea/easylab-agent`), E2E_KEEP=1 to keep the temp dir for debugging.
 */
import { type ChildProcess, spawn } from 'node:child_process'
import { existsSync, mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { createServer as createNetServer } from 'node:net'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { start as startNats } from '@abc-protocol/sdk/natsrun/index.js'
import type { JsonObject } from '@bufbuild/protobuf'
import { create } from '@bufbuild/protobuf'
import { ValueSchema } from '@bufbuild/protobuf/wkt'
import { type Client, createClient } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import {
  AgentService,
  CompactRequestSchema,
  CreateSessionRequestSchema,
  DeletePresetRequestSchema,
  DeleteProviderRequestSchema,
  DeleteSessionRequestSchema,
  FileRefSchema,
  ForkRequestSchema,
  GetConfigRequestSchema,
  GetFileMetaRequestSchema,
  GetFileRequestSchema,
  GetSessionRequestSchema,
  IngestFileRequestSchema,
  InterruptRequestSchema,
  ListMessagesRequestSchema,
  ListModelsRequestSchema,
  ListPresetsRequestSchema,
  ListProvidersRequestSchema,
  ListSessionsRequestSchema,
  ListToolsRequestSchema,
  MailboxRequestSchema,
  PresetSchema,
  PreviewPresetRequestSchema,
  PromptRequestSchema,
  ProviderModelSchema,
  ProviderSchema,
  RegisterProviderRequestSchema,
  RenameRequestSchema,
  SetConfigRequestSchema,
  SetExtensionConfigRequestSchema,
  SetModelRequestSchema,
  SetToolConfigRequestSchema,
  StateRequestSchema,
  TestProviderRequestSchema,
  UndoRequestSchema,
  UpdateSettingsRequestSchema,
  UploadFileRequestSchema,
  UpsertPresetRequestSchema,
  WatchSessionRequestSchema,
  WatchSessionsRequestSchema,
} from '@easylab-agent/schema'

// ---------------------------------------------------------------------------
// tiny test harness
// ---------------------------------------------------------------------------
let passed = 0
let failed = 0
const failures: string[] = []
function ok(name: string): void {
  passed++
  console.log(`  \u2713 ${name}`)
}
function bad(name: string, detail?: unknown): void {
  failed++
  failures.push(name)
  console.log(
    `  \u2717 ${name}${detail === undefined ? '' : ` — ${String(detail)}`}`,
  )
}
function check(name: string, cond: boolean, detail?: unknown): void {
  if (cond) ok(name)
  else bad(name, detail)
}
function section(title: string): void {
  console.log(`\n${title}`)
}
const sleep = (ms: number) => new Promise<void>(r => setTimeout(r, ms))

async function freePort(): Promise<number> {
  return new Promise((res, rej) => {
    const s = createNetServer()
    s.once('error', rej)
    s.listen(0, '127.0.0.1', () => {
      const addr = s.address()
      const port = typeof addr === 'object' && addr ? addr.port : 0
      s.close(() => res(port))
    })
  })
}

// ---------------------------------------------------------------------------
// mock OpenAI-compatible LLM upstream
// ---------------------------------------------------------------------------
interface MockState {
  /** Last `reasoning_effort` observed on a chat request (undefined if none). */
  lastReasoningEffort: string | undefined
  requests: number
  /** Number of /images/generations calls (image toolchain coverage). */
  imageRequests: number
}

function sseChunk(res: import('node:http').ServerResponse, obj: unknown): void {
  res.write(`data: ${JSON.stringify(obj)}\n\n`)
}

/**
 * Decides the response for a chat request from the body:
 *   - a request that already carries a `tool` message ⇒ final tool follow-up.
 *   - model `mock-tool` ⇒ reasoning + a single `todo-write` tool call.
 *   - model `mock-slow` ⇒ stall (for the interrupt test).
 *   - `mock-text` / `gpt-5.4` ⇒ reasoning + a text reply.
 */
function mockResponse(body: Record<string, unknown>): {
  reasoning: string
  text: string
  toolCall: boolean
  /** Which tool to call when toolCall is true. */
  toolName: string
  slowMs: number
} {
  const model = String(body['model'] ?? '')
  const hasTool = Array.isArray(body['messages'])
    ? (body['messages'] as Array<{ role?: string }>).some(
        m => m.role === 'tool',
      )
    : false
  if (model === 'mock-slow')
    return { reasoning: '', text: 'SLOW-OK', toolCall: false, toolName: '', slowMs: 8000 }
  if (model === 'mock-tool' && !hasTool) {
    return {
      reasoning: 'let me think',
      text: '',
      toolCall: true,
      toolName: 'todo-write',
      slowMs: 0,
    }
  }
  if (model === 'mock-tool')
    return { reasoning: '', text: 'TOOL-OK', toolCall: false, toolName: '', slowMs: 0 }
  if (model === 'mock-image') {
    // The image-turn request: call image-generate once, then wrap up.
    return hasTool
      ? { reasoning: '', text: 'IMAGE-OK', toolCall: false, toolName: '', slowMs: 0 }
      : { reasoning: 'drawing', text: '', toolCall: true, toolName: 'image-generate', slowMs: 0 }
  }
  if (model === 'mock-text')
    return {
      reasoning: 'thinking about it',
      text: 'HELLO-E2E',
      toolCall: false,
      toolName: '',
      slowMs: 0,
    }
  return {
    reasoning: 'gpt thinking',
    text: 'GPT-OK',
    toolCall: false,
    toolName: '',
    slowMs: 0,
  }
}

async function startMockLlm(
  state: MockState,
): Promise<{ url: string; stop: () => Promise<void> }> {
  const port = await freePort()
  // A 1x1 transparent PNG — enough for the image toolchain round-trip.
  const pngB64 =
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=='
  const server = createServer((req, res) => {
    // Image generation endpoint (openai-compatible imageModel path).
    if (req.method === 'POST' && req.url?.endsWith('/images/generations')) {
      const chunks: Buffer[] = []
      req.on('data', c => chunks.push(c as Buffer))
      req.on('end', () => {
        state.requests++
        let body: Record<string, unknown> = {}
        try {
          body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<
            string,
            unknown
          >
        } catch {
          /* ignore */
        }
        state.imageRequests++
        const n = typeof body['n'] === 'number' ? body['n'] : 1
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(
          JSON.stringify({
            created: 1,
            data: Array.from({ length: n }, () => ({
              b64_json: pngB64,
            })),
          }),
        )
      })
      return
    }
    if (req.method !== 'POST' || !req.url?.endsWith('/chat/completions')) {
      res.writeHead(404, { 'content-type': 'application/json' })
      res.end('{"error":"not found"}')
      return
    }
    const chunks: Buffer[] = []
    req.on('data', c => chunks.push(c as Buffer))
    req.on('end', () => {
      state.requests++
      let body: Record<string, unknown> = {}
      try {
        body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as Record<
          string,
          unknown
        >
      } catch {
        /* ignore */
      }
      const eff = body['reasoning_effort']
      state.lastReasoningEffort = typeof eff === 'string' ? eff : undefined
      const plan = mockResponse(body)
      const id = `chatcmpl-${state.requests}`
      const base = {
        id,
        object: 'chat.completion.chunk',
        created: 1,
        model: String(body['model'] ?? ''),
      }

      // `generateText` (testProvider) is non-streaming: answer with a plain
      // chat completion JSON. Only stream when the request asked for it.
      if (body['stream'] !== true) {
        const toolArgs =
          plan.toolName === 'image-generate'
            ? { prompt: 'a cat' }
            : {
                todos: [
                  { content: 'e2e todo', status: 'pending', priority: 'high' },
                ],
              }
        const message = plan.toolCall
          ? {
              role: 'assistant',
              content: null,
              tool_calls: [
                {
                  id: 'call_e2e_1',
                  type: 'function',
                  function: {
                    name: plan.toolName,
                    arguments: JSON.stringify(toolArgs),
                  },
                },
              ],
            }
          : { role: 'assistant', content: plan.text || 'ok' }
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(
          JSON.stringify({
            id,
            object: 'chat.completion',
            created: 1,
            model: String(body['model'] ?? ''),
            choices: [
              {
                index: 0,
                message,
                finish_reason: plan.toolCall ? 'tool_calls' : 'stop',
              },
            ],
            usage: {
              prompt_tokens: 11,
              completion_tokens: 22,
              total_tokens: 33,
            },
          }),
        )
        return
      }

      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
      })
      const send = (delta: unknown, finish: string | null = null) =>
        sseChunk(res, {
          ...base,
          choices: [{ index: 0, delta, finish_reason: finish }],
        })

      void (async () => {
        send({ role: 'assistant' })
        if (plan.slowMs > 0) {
          await sleep(plan.slowMs)
        }
        for (const piece of plan.reasoning
          ? [plan.reasoning.slice(0, 5), plan.reasoning.slice(5)]
          : []) {
          if (piece) send({ reasoning_content: piece })
          await sleep(5)
        }
        if (plan.toolCall) {
          const streamedArgs =
            plan.toolName === 'image-generate'
              ? { prompt: 'a cat' }
              : {
                  todos: [
                    {
                      content: 'e2e todo',
                      status: 'pending',
                      priority: 'high',
                    },
                  ],
                }
          send({
            tool_calls: [
              {
                index: 0,
                id: 'call_e2e_1',
                type: 'function',
                function: { name: plan.toolName, arguments: '' },
              },
            ],
          })
          await sleep(5)
          send({
            tool_calls: [
              {
                index: 0,
                function: { arguments: JSON.stringify(streamedArgs) },
              },
            ],
          })
          send({}, 'tool_calls')
        } else {
          for (const piece of plan.text.match(/.{1,4}/g) ?? []) {
            send({ content: piece })
            await sleep(5)
          }
          send({}, 'stop')
        }
        sseChunk(res, {
          ...base,
          choices: [],
          usage: {
            prompt_tokens: 11,
            completion_tokens: 22,
            total_tokens: 33,
            completion_tokens_details: { reasoning_tokens: 7 },
          },
        })
        res.write('data: [DONE]\n\n')
        res.end()
      })()
    })
  })
  await new Promise<void>(r => server.listen(port, '127.0.0.1', () => r()))
  return {
    url: `http://127.0.0.1:${port}/v1`,
    stop: () =>
      new Promise<void>(r => {
        server.close(() => r())
      }),
  }
}

// ---------------------------------------------------------------------------
// watch-stream collectors
// ---------------------------------------------------------------------------
interface WatchEv {
  event: string
  params: Record<string, unknown>
}

function collectSession(events: WatchEv[]): {
  waitFor: (
    pred: (e: WatchEv) => boolean,
    ms?: number,
  ) => Promise<WatchEv | null>
  text: () => string
  reasoning: () => string
} {
  const waitFor = async (
    pred: (e: WatchEv) => boolean,
    ms = 20000,
  ): Promise<WatchEv | null> => {
    const deadline = Date.now() + ms
    for (;;) {
      for (const e of events) {
        if (pred(e)) return e
      }
      if (Date.now() > deadline) return null
      await sleep(40)
    }
  }
  const joinText = (ev: 'text-delta' | 'reasoning-delta', key: string) =>
    events
      .filter(e => e.event === ev)
      .map(e => String(e.params[key] ?? ''))
      .join('')
  return {
    waitFor,
    text: () => joinText('text-delta', 'text'),
    reasoning: () => joinText('reasoning-delta', 'text'),
  }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
interface Env {
  NATS_URL?: string
  ABC_NATS_URL?: string
}

async function main(): Promise<void> {
  const root = resolve(process.cwd())
  const bin =
    process.env['E2E_AGENT_BIN'] ?? join(root, '.sea', 'easylab-agent')
  if (!process.env['E2E_AGENT_BIN'] && !existsSync(bin)) {
    console.error(`agent binary not found at ${bin}. Run: npm run build`)
    process.exit(2)
  }

  const work = mkdtempSync(join(tmpdir(), 'agent-e2e-'))
  console.log(`[e2e] workdir ${work}`)

  const env: Env = process.env
  let natsStop: (() => Promise<void>) | null = null
  let natsUrl = env['ABC_NATS_URL'] ?? env['NATS_URL']
  if (!natsUrl) {
    const natsBin =
      process.env['ABC_NATS_SERVER_BIN'] ??
      (existsSync('/tmp/opencode/nats/bin/nats-server')
        ? '/tmp/opencode/nats/bin/nats-server'
        : 'nats-server')
    const nats = await startNats({ storage: 'memory', binary: natsBin })
    natsUrl = nats.url
    natsStop = nats.stop
    console.log(`[e2e] nats ${natsUrl}`)
  }

  const state: MockState = {
    lastReasoningEffort: undefined,
    requests: 0,
    imageRequests: 0,
  }
  const mock = await startMockLlm(state)

  const httpPort = await freePort()
  const dbFile = join(work, 'agent.db')
  let agent: ChildProcess | null = null
  let agentStopped = false
  const agentLog: string[] = []

  const startAgentProcess = (): void => {
    // The default target is the self-contained SEA binary (executed directly).
    // E2E_AGENT_BIN may point at that binary or, for debugging, a JS entrypoint
    // (run via the current node with the same env).
    const isJs = /\.[cm]?js$/.test(bin)
    agent = spawn(isJs ? process.execPath : bin, isJs ? [bin] : [], {
      env: {
        ...process.env,
        PORT: String(httpPort),
        HTTP_PROTOCOL: 'h1',
        DB_BACKEND: 'sqlite',
        DATABASE_URL: `sqlite://${dbFile}`,
        NATS_URL: natsUrl,
        LOG_LEVEL: process.env['E2E_DEBUG'] === '1' ? 'debug' : 'warn',
      },
      stdio: ['ignore', 'ignore', 'pipe'],
    })
    agent.stderr?.on('data', d => {
      const s = String(d)
      agentLog.push(s)
      if (process.env['E2E_DEBUG'] === '1') process.stderr.write(`[agent] ${s}`)
    })
    agent.on('exit', code => {
      if (!agentStopped && code !== 0 && code !== null) {
        console.error(`[e2e] agent exited early (code ${code})`)
      }
    })
  }
  startAgentProcess()

  const baseUrl = `http://127.0.0.1:${httpPort}`
  const transport = createConnectTransport({ baseUrl, httpVersion: '1.1' })
  const client: Client<typeof AgentService> = createClient(
    AgentService,
    transport,
  )

  // Wait for health.
  let healthy = false
  for (let i = 0; i < 100; i++) {
    try {
      const h = await client.health({})
      if (h.ok) {
        healthy = true
        break
      }
    } catch {
      /* retry */
    }
    await sleep(200)
  }
  if (!healthy) {
    console.error('[e2e] agent did not become healthy')
    await cleanup()
    process.exit(1)
  }

  // Wait until the bundled extension has registered its tools.
  for (let i = 0; i < 60; i++) {
    const r = await client.listTools({})
    if (r.tools.length >= 11) break
    await sleep(200)
  }

  try {
    await run(client, state, mock.url)
  } catch (err) {
    bad('suite ran to completion', err)
    console.error(String(err))
    console.error('[e2e] agent log tail:')
    console.error(agentLog.join('').split('\n').slice(-20).join('\n'))
  } finally {
    await cleanup()
  }

  console.log(`\n[e2e] passed=${passed} failed=${failed}`)
  if (failed > 0) {
    console.log(`[e2e] failures:\n  - ${failures.join('\n  - ')}`)
    const tail = agentLog.join('').split('\n').filter(Boolean).slice(-30)
    if (tail.length > 0)
      console.log(`[e2e] agent log tail:\n${tail.join('\n')}`)
  }
  process.exit(failed === 0 ? 0 : 1)

  async function cleanup(): Promise<void> {
    agentStopped = true
    agent?.kill('SIGKILL')
    await mock.stop().catch(() => {})
    if (natsStop) await natsStop().catch(() => {})
    if (process.env['E2E_KEEP'] !== '1') {
      rmSync(work, { recursive: true, force: true })
    } else {
      console.log(`[e2e] kept workdir ${work}`)
    }
  }
}

async function run(
  client: Client<typeof AgentService>,
  state: MockState,
  mockUrl: string,
): Promise<void> {
  const uniq = Date.now()

  // -------------------------------------------------------------------------
  section('health / provider registration')
  const health = await client.health({})
  check('health ok', health.ok, health)
  check('health name', health.name.length > 0, health.name)

  const reg = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'openai',
        apiType: 'openai-compatible',
        baseUrl: mockUrl,
        apiKey: 'test-key',
        models: [
          create(ProviderModelSchema, {
            id: 'gpt-5.4',
            name: 'GPT 5.4',
            contextLimit: 400000n,
          }),
          create(ProviderModelSchema, {
            id: 'mock-text',
            name: 'Mock Text',
            contextLimit: 100000n,
          }),
          create(ProviderModelSchema, {
            id: 'mock-tool',
            name: 'Mock Tool',
            contextLimit: 100000n,
          }),
          create(ProviderModelSchema, {
            id: 'mock-slow',
            name: 'Mock Slow',
            contextLimit: 100000n,
          }),
          // Generation models: no context_limit, capability-tagged.
          create(ProviderModelSchema, {
            id: 'mock-image',
            name: 'Mock Image',
            capability: 'image',
          }),
          create(ProviderModelSchema, {
            id: 'mock-video',
            name: 'Mock Video',
            capability: 'video',
          }),
          create(ProviderModelSchema, {
            id: 'mock-tts',
            name: 'Mock TTS',
            capability: 'speech',
          }),
        ],
      }),
    }),
  )
  check('registerProvider', reg.ok)

  // context_limit is required > 0.
  let rejectedBadLimit = false
  try {
    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'bad',
          apiType: 'openai-compatible',
          baseUrl: mockUrl,
          models: [create(ProviderModelSchema, { id: 'x', contextLimit: 0n })],
        }),
      }),
    )
  } catch {
    rejectedBadLimit = true
  }
  check('registerProvider rejects context_limit=0', rejectedBadLimit)

  const providers = await client.listProviders({})
  check(
    'listProviders includes our provider',
    providers.providers.some(p => p.providerId === 'openai'),
    providers.providers.map(p => p.providerId),
  )

  const models = await client.listModels(
    create(ListModelsRequestSchema, { providerId: 'openai' }),
  )
  const gpt = models.models.find(m => m.id === 'gpt-5.4')
  check('listModels returns gpt-5.4', gpt !== undefined)
  check(
    'listModels contextLimit echoed',
    gpt?.contextLimit === 400000n,
    gpt?.contextLimit,
  )
  check(
    'listModels derives variants',
    (gpt?.variants.length ?? 0) > 0,
    gpt?.variants.map(v => v.id),
  )
  check(
    'listModels variant ids include high',
    (gpt?.variants ?? []).some(v => v.id === 'high'),
    gpt?.variants.map(v => v.id),
  )

  let listModelsRejectsEmpty = false
  try {
    await client.listModels(create(ListModelsRequestSchema, { providerId: '' }))
  } catch {
    listModelsRejectsEmpty = true
  }
  check('listModels rejects empty provider_id', listModelsRejectsEmpty)

  // Generation models must NOT surface as session models (text-only list).
  check(
    'listModels hides generation models',
    !models.models.some(m => ['mock-image', 'mock-video', 'mock-tts'].includes(m.id)),
    models.models.map(m => m.id),
  )
  // And a generation model may register with context_limit = 0.
  let genZeroCtxOk = false
  try {
    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'genzero',
          apiType: 'openai-compatible',
          baseUrl: mockUrl,
          models: [
            create(ProviderModelSchema, {
              id: 'img-x',
              capability: 'image',
            }),
          ],
        }),
      }),
    )
    genZeroCtxOk = true
  } catch {
    genZeroCtxOk = false
  }
  check('generation model with context_limit=0 accepted', genZeroCtxOk)
  let badCapability = false
  try {
    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'badcap',
          apiType: 'openai-compatible',
          baseUrl: mockUrl,
          models: [
            create(ProviderModelSchema, {
              id: 'x',
              capability: 'hologram',
            }),
          ],
        }),
      }),
    )
  } catch {
    badCapability = true
  }
  check('unknown capability rejected', badCapability)

  const test = await client.testProvider(
    create(TestProviderRequestSchema, {
      providerId: 'openai',
      apiType: 'openai-compatible',
      baseUrl: mockUrl,
      apiKey: 'test-key',
      model: 'openai/gpt-5.4',
      variant: 'high',
    }),
  )
  check('testProvider ok', test.ok, test.result)
  check(
    'testProvider applied variant reasoning_effort',
    state.lastReasoningEffort === 'high',
    state.lastReasoningEffort,
  )

  // -------------------------------------------------------------------------
  section('tools discovery + i18n')
  const toolsEn = await client.listTools(
    create(ListToolsRequestSchema, { locale: 'en' }),
  )
  const toolNames = toolsEn.tools.map(t => t.name)
  check(
    'listTools returns all bundled tools',
    toolsEn.tools.length === 11,
    `got ${toolsEn.tools.length}: ${toolNames.join(',')}`,
  )
  check(
    'tool names are unique',
    new Set(toolNames).size === toolNames.length,
    toolNames,
  )
  const zh = await client.listTools(
    create(ListToolsRequestSchema, { locale: 'zh' }),
  )
  const fetchEn = toolsEn.tools.find(t => t.name === 'web-fetch')
  const fetchZh = zh.tools.find(t => t.name === 'web-fetch')
  check('web-fetch has en description', (fetchEn?.description ?? '').length > 0)
  check(
    'tools i18n differs (zh)',
    fetchZh !== undefined && fetchZh.description !== fetchEn?.description,
  )
  const todoInfo = toolsEn.tools.find(t => t.name === 'todo-write')
  check(
    'todo-write parameters schema present',
    todoInfo !== undefined && Object.keys(todoInfo.parameters ?? {}).length > 0,
  )
  const brave = toolsEn.tools.find(t => t.name === 'brave-search')
  check(
    'brave-search requires config',
    (brave?.requiredConfig ?? []).includes('brave_api_key'),
  )
  for (const gen of ['image-generate', 'image-edit', 'video-generate', 'tts-generate']) {
    const t = toolsEn.tools.find(t => t.name === gen)
    check(`${gen} discovered`, t !== undefined, toolNames)
  }
  const imageGen = toolsEn.tools.find(t => t.name === 'image-generate')
  check(
    'image-generate requires image_model only',
    (imageGen?.requiredConfig ?? []).join(',') === 'image_model',
    imageGen?.requiredConfig,
  )

  // -------------------------------------------------------------------------
  section('presets')
  const presets = await client.listPresets(create(ListPresetsRequestSchema, {}))
  check(
    'default preset present',
    presets.presets.some(p => p.id === 'default'),
  )
  const previewBefore = presets.presets.find(p => p.id === 'default')
  check(
    'listPresets returns a systemPrompt',
    (previewBefore?.systemPrompt ?? '').length > 0,
    previewBefore?.systemPrompt,
  )
  const up = await client.upsertPreset(
    create(UpsertPresetRequestSchema, {
      preset: create(PresetSchema, {
        id: `e2e-preset-${uniq}`,
        systemPrompt: 'you are e2e',
        maxTurns: 3,
      }),
    }),
  )
  check('upsertPreset', up.ok)
  const preview = await client.previewPreset(
    create(PreviewPresetRequestSchema, { id: `e2e-preset-${uniq}` }),
  )
  check(
    'previewPreset returns template',
    preview.template.includes('e2e'),
    preview.template,
  )
  let deletedSystemPreset = false
  try {
    await client.deletePreset(
      create(DeletePresetRequestSchema, { id: 'default' }),
    )
  } catch {
    deletedSystemPreset = true
  }
  check('system preset is immutable', deletedSystemPreset)
  const delPreset = await client.deletePreset(
    create(DeletePresetRequestSchema, { id: `e2e-preset-${uniq}` }),
  )
  check('deletePreset', delPreset.ok)

  // -------------------------------------------------------------------------
  section('config (global / tool / extension)')
  await client.setConfig(
    create(SetConfigRequestSchema, { key: 'e2e-key', value: 'e2e-val' }),
  )
  const cfg = await client.getConfig(
    create(GetConfigRequestSchema, { key: 'e2e-key' }),
  )
  check('config set/get round-trip', cfg.value === 'e2e-val', cfg.value)

  await client.setToolConfig(
    create(SetToolConfigRequestSchema, {
      config: { 'e2e-tool': { knob: true } } as JsonObject,
    }),
  )
  const toolCfg = await client.getToolConfig({})
  check(
    'getToolConfig returns stored struct',
    toolCfg.config?.values !== undefined,
  )

  await client.setExtensionConfig(
    create(SetExtensionConfigRequestSchema, {
      extId: 'bundled',
      name: 'brave_api_key',
      value: create(ValueSchema, {
        kind: { case: 'stringValue', value: 'e2e-brave' },
      }),
    }),
  )
  const toolsAfter = await client.listTools(create(ListToolsRequestSchema, {}))
  check('setExtensionConfig accepted', toolsAfter.tools.length === 11)

  // Configure the image tool to point at the mock image model, then drive a
  // full image-generate tool call through the turn loop.
  await client.setExtensionConfig(
    create(SetExtensionConfigRequestSchema, {
      extId: 'bundled',
      name: 'image_model',
      value: create(ValueSchema, {
        kind: { case: 'stringValue', value: 'openai/mock-image' },
      }),
    }),
  )
  const imgSid = `e2e-image-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: imgSid,
      model: 'openai/mock-image',
      preset: 'default',
    }),
  )
  const imgEvents: WatchEv[] = []
  const iac = new AbortController()
  const imgWatcher = (async () => {
    try {
      const stream = client.watchSession(
        create(WatchSessionRequestSchema, { id: imgSid }),
        { signal: iac.signal },
      )
      for await (const ev of stream) {
        imgEvents.push({
          event: ev.event,
          params: (ev.params ?? {}) as Record<string, unknown>,
        })
      }
    } catch {
      /* aborted */
    }
  })()
  const ic = collectSession(imgEvents)
  await sleep(300)
  const imgPrompt = client.prompt(
    create(PromptRequestSchema, {
      id: imgSid,
      prompt: 'make me a picture',
    }),
  )
  for await (const e of imgPrompt) {
    if (e.event === 'accepted') break
  }
  const imgDone = await ic.waitFor(e => e.event === 'turn-complete', 40000)
  check('image tool turn complete', imgDone !== null)
  const imgResult = imgEvents.find(e => e.event === 'tool-result')
  check(
    'image-generate tool ran',
    imgResult !== undefined &&
      String(imgResult.params['formatted'] ?? '').includes('Generated'),
    imgResult?.params['formatted'],
  )
  check(
    'mock /images/generations was hit',
    state.imageRequests >= 1,
    state.imageRequests,
  )
  const imgMsgs = await client.listMessages(
    create(ListMessagesRequestSchema, { id: imgSid, limit: 50 }),
  )
  const imgPart = imgMsgs.messages
    .flatMap(m => m.parts)
    .find(p => p.type === 'tool')
  check('image tool part persisted', imgPart !== undefined)
  iac.abort()
  await imgWatcher.catch(() => {})
  await client.deleteSession(create(DeleteSessionRequestSchema, { id: imgSid }))

  const agentCfg = await client.getAgentConfig({})
  check(
    'getAgentConfig returns providers struct',
    agentCfg.config !== undefined,
  )

  // -------------------------------------------------------------------------
  section('session CRUD')
  const sid = `e2e-main-${uniq}`
  const created = await client.createSession(
    create(CreateSessionRequestSchema, {
      name: sid,
      model: 'openai/mock-text',
      preset: 'default',
    }),
  )
  check('createSession', created.ok && created.sessionName === sid, created)

  let dupRejected = false
  try {
    await client.createSession(
      create(CreateSessionRequestSchema, { name: sid }),
    )
  } catch {
    dupRejected = true
  }
  check('createSession rejects duplicate', dupRejected)

  const got = await client.getSession(
    create(GetSessionRequestSchema, { id: sid }),
  )
  check(
    'getSession model ref',
    got.session?.model === 'openai/mock-text',
    got.session?.model,
  )

  const ren = await client.rename(
    create(RenameRequestSchema, { id: sid, name: `${sid}-renamed` }),
  )
  check('rename', ren.session?.name === `${sid}-renamed`, ren.session?.name)
  const sid2 = `${sid}-renamed`
  const renamed = await client.getSession(
    create(GetSessionRequestSchema, { id: sid2 }),
  )
  check('renamed session reachable', renamed.session?.name === sid2)
  let oldGone = false
  try {
    await client.getSession(create(GetSessionRequestSchema, { id: sid }))
  } catch {
    oldGone = true
  }
  check('old name gone after rename', oldGone)

  // -------------------------------------------------------------------------
  section('turn: reasoning + text streaming + persistence')
  const events: WatchEv[] = []
  const ac = new AbortController()
  const watcher = (async () => {
    try {
      const stream = client.watchSession(
        create(WatchSessionRequestSchema, { id: sid2 }),
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

  const collector = collectSession(events)
  // give the watcher a moment to attach
  await sleep(300)
  const promptStream = client.prompt(
    create(PromptRequestSchema, { id: sid2, prompt: 'say hello' }),
  )
  for await (const e of promptStream) {
    if (e.event === 'accepted') break
  }
  const done = await collector.waitFor(e => e.event === 'turn-complete', 30000)
  check('turn-complete observed', done !== null)
  check(
    'reasoning-start observed',
    events.some(e => e.event === 'reasoning-start'),
  )
  check(
    'text-start observed',
    events.some(e => e.event === 'text-start'),
  )
  check(
    'reasoning text streamed',
    collector.reasoning().includes('thinking'),
    collector.reasoning(),
  )
  check(
    'text streamed',
    collector.text().includes('HELLO-E2E'),
    collector.text(),
  )
  ac.abort()
  await watcher.catch(() => {})

  const msgs = await client.listMessages(
    create(ListMessagesRequestSchema, { id: sid2, limit: 50 }),
  )
  const assistant = msgs.messages.find(m => m.role === 'assistant')
  check('assistant message persisted', assistant !== undefined)
  const textPart = assistant?.parts.find(p => p.type === 'text')
  check(
    'persisted text part',
    (textPart?.data ?? '').includes('HELLO-E2E'),
    textPart?.data,
  )
  const user = msgs.messages.find(m => m.role === 'user')
  check('user message persisted', user !== undefined)

  // The lease is released a beat after `turn-complete` is published, so poll.
  let idleAfterTurn = false
  for (let i = 0; i < 40; i++) {
    const st = await client.state(create(StateRequestSchema, { id: sid2 }))
    if (st.state?.['status'] === 'idle') {
      idleAfterTurn = true
      break
    }
    await sleep(150)
  }
  check('state idle after turn', idleAfterTurn)

  const sessAfter = await client.getSession(
    create(GetSessionRequestSchema, { id: sid2 }),
  )
  check(
    'message_seq advanced',
    (sessAfter.session?.messageSeq ?? 0) >= 2,
    sessAfter.session?.messageSeq,
  )
  check(
    'last message preview is a user/assistant turn',
    (sessAfter.session?.lastMessagePreview ?? '').length > 0,
    sessAfter.session?.lastMessagePreview,
  )

  const mailbox = await client.mailbox(
    create(MailboxRequestSchema, { id: sid2 }),
  )
  check('mailbox call ok', mailbox.ok)
  check(
    'mailbox recorded the user prompt',
    mailbox.mailbox.some(m => m.msgType === 'user_prompt'),
    mailbox.mailbox.map(m => m.msgType),
  )

  // -------------------------------------------------------------------------
  section('tool calling (bundled todo-write)')
  const toolSid = `e2e-tool-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: toolSid,
      model: 'openai/mock-tool',
      preset: 'default',
    }),
  )
  const toolEvents: WatchEv[] = []
  const tac = new AbortController()
  const toolWatcher = (async () => {
    try {
      const stream = client.watchSession(
        create(WatchSessionRequestSchema, { id: toolSid }),
        { signal: tac.signal },
      )
      for await (const ev of stream) {
        toolEvents.push({
          event: ev.event,
          params: (ev.params ?? {}) as Record<string, unknown>,
        })
      }
    } catch {
      /* aborted */
    }
  })()
  const tc = collectSession(toolEvents)
  await sleep(300)
  const ts = client.prompt(
    create(PromptRequestSchema, { id: toolSid, prompt: 'add a todo' }),
  )
  for await (const e of ts) {
    if (e.event === 'accepted') break
  }
  const toolDone = await tc.waitFor(e => e.event === 'turn-complete', 40000)
  check('tool turn complete', toolDone !== null)
  check(
    'tool-call observed',
    toolEvents.some(e => e.event === 'tool-call'),
  )
  const toolResult = toolEvents.find(e => e.event === 'tool-result')
  check('tool-result observed', toolResult !== undefined)
  check(
    'tool-result has content',
    String(toolResult?.params['formatted'] ?? '').includes('todo'),
    toolResult?.params['formatted'],
  )
  check('final text after tool', tc.text().includes('TOOL-OK'), tc.text())
  tac.abort()
  await toolWatcher.catch(() => {})

  const toolMsgs = await client.listMessages(
    create(ListMessagesRequestSchema, { id: toolSid, limit: 50 }),
  )
  const assistantWithTool = toolMsgs.messages.find(m =>
    m.parts.some(p => p.type === 'tool'),
  )
  check('tool part persisted', assistantWithTool !== undefined)

  // -------------------------------------------------------------------------
  section('undo / fork')
  const beforeUndo = await client.listMessages(
    create(ListMessagesRequestSchema, { id: toolSid, limit: 50 }),
  )
  const beforeCount = beforeUndo.messages.length
  const undo = await client.undo(create(UndoRequestSchema, { id: toolSid }))
  check('undo ok', undo.session !== undefined)
  const afterUndo = await client.listMessages(
    create(ListMessagesRequestSchema, { id: toolSid, limit: 50 }),
  )
  check(
    'undo removed the tip message',
    afterUndo.messages.length < beforeCount,
    `${beforeCount} -> ${afterUndo.messages.length}`,
  )

  const forkName = `e2e-fork-${uniq}`
  try {
    const fork = await client.fork(
      create(ForkRequestSchema, { id: sid2, name: forkName }),
    )
    check(
      'fork creates session',
      fork.session?.name === forkName,
      fork.session?.name,
    )
    const forkSess = await client.getSession(
      create(GetSessionRequestSchema, { id: forkName }),
    )
    check(
      'fork shares parent tip',
      forkSess.session?.tipId === sessAfter.session?.tipId,
      forkSess.session?.tipId,
    )
  } catch (e) {
    bad('fork creates session', e)
  }

  // -------------------------------------------------------------------------
  section('watchSessions (snapshot / upsert / remove)')
  const wsFrames: Array<{
    snapshot: boolean
    upserts: string[]
    removed: string[]
    seq: number
  }> = []
  const wac = new AbortController()
  const wsDone = (async () => {
    try {
      const stream = client.watchSessions(
        create(WatchSessionsRequestSchema, {}),
        {
          signal: wac.signal,
        },
      )
      for await (const ev of stream) {
        wsFrames.push({
          snapshot: ev.snapshot,
          upserts: ev.upserts.map(s => s.name),
          removed: [...ev.removed],
          seq: ev.upserts.find(s => s.name === sid2)?.messageSeq ?? 0,
        })
      }
    } catch {
      /* aborted */
    }
  })()
  const waitFrame = async (
    pred: (f: (typeof wsFrames)[number]) => boolean,
    ms = 15000,
  ): Promise<boolean> => {
    const deadline = Date.now() + ms
    for (;;) {
      if (wsFrames.some(pred)) return true
      if (Date.now() > deadline) return false
      await sleep(40)
    }
  }
  check(
    'watchSessions first frame is snapshot',
    await waitFrame(f => f.snapshot),
  )
  check(
    'snapshot includes main session',
    wsFrames.some(f => f.snapshot && f.upserts.includes(sid2)),
  )

  const streamSid = `e2e-stream-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: streamSid,
      model: 'openai/mock-text',
      preset: 'default',
    }),
  )
  check(
    'watchSessions upserts a new session',
    await waitFrame(f => !f.snapshot && f.upserts.includes(streamSid)),
  )
  await client.deleteSession(
    create(DeleteSessionRequestSchema, { id: streamSid }),
  )
  check(
    'watchSessions removes a deleted session',
    await waitFrame(f => f.removed.includes(streamSid)),
  )
  check(
    'watchSessions carries message_seq',
    wsFrames.some(f => f.seq > 0),
    Math.max(...wsFrames.map(f => f.seq)),
  )
  wac.abort()
  await wsDone.catch(() => {})

  // -------------------------------------------------------------------------
  section('update settings / set model')
  const upd = await client.updateSettings(
    create(UpdateSettingsRequestSchema, {
      id: sid2,
      maxTurns: 5,
      systemPrompt: 'e2e system',
    }),
  )
  check(
    'updateSettings maxTurns',
    upd.session?.maxTurns === 5,
    upd.session?.maxTurns,
  )
  check(
    'updateSettings systemPrompt',
    upd.session?.systemPrompt === 'e2e system',
  )
  let zeroRejected = false
  try {
    await client.updateSettings(
      create(UpdateSettingsRequestSchema, { id: sid2, maxTurns: 0 }),
    )
  } catch {
    zeroRejected = true
  }
  check('updateSettings rejects maxTurns=0', zeroRejected)

  const setModel = await client.setModel(
    create(SetModelRequestSchema, {
      id: sid2,
      model: 'openai/gpt-5.4',
      variant: 'low',
    }),
  )
  check(
    'setModel canonical ref',
    setModel.session?.model === 'openai/gpt-5.4',
    setModel.session?.model,
  )
  check(
    'setModel variant',
    setModel.session?.variant === 'low',
    setModel.session?.variant,
  )

  // -------------------------------------------------------------------------
  section('files')
  const fileBytes = Buffer.from('hello e2e file', 'utf8')
  const code = `e2e${uniq}`
  const upFile = await client.uploadFile(
    create(UploadFileRequestSchema, {
      file: create(FileRefSchema, {
        code,
        name: 'hello.txt',
        mime: 'text/plain',
        size: fileBytes.length,
      }),
      data: fileBytes.toString('base64'),
    }),
  )
  check('uploadFile', upFile.ok && upFile.code === code, upFile.code)
  const gotFile = await client.getFile(create(GetFileRequestSchema, { code }))
  check(
    'getFile round-trips bytes',
    Buffer.from(gotFile.data).toString('utf8') === 'hello e2e file',
  )
  check('getFile name', gotFile.name === 'hello.txt', gotFile.name)
  const meta = await client.getFileMeta(
    create(GetFileMetaRequestSchema, { code }),
  )
  check('getFileMeta size', meta.size === fileBytes.length, meta.size)

  const ingest = await client.ingestFile(
    create(IngestFileRequestSchema, {
      data: new Uint8Array(Buffer.from('ingested body')),
      name: 'ingested.txt',
      mime: 'text/plain',
    }),
  )
  check(
    'ingestFile returns code',
    ingest.ok && ingest.code.length > 0,
    ingest.code,
  )
  const ingested = await client.getFile(
    create(GetFileRequestSchema, { code: ingest.code }),
  )
  check(
    'ingestFile bytes retrievable',
    Buffer.from(ingested.data).toString('utf8') === 'ingested body',
  )

  // -------------------------------------------------------------------------
  section('interrupt / compact')
  const slowSid = `e2e-slow-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: slowSid,
      model: 'openai/mock-slow',
      preset: 'default',
    }),
  )
  const slowStream = client.prompt(
    create(PromptRequestSchema, { id: slowSid, prompt: 'go slow' }),
  )
  for await (const e of slowStream) {
    if (e.event === 'accepted') break
  }
  await sleep(500)
  const intr = await client.interrupt(
    create(InterruptRequestSchema, { id: slowSid }),
  )
  check('interrupt ok', intr.ok)
  let slowIdle = false
  for (let i = 0; i < 40; i++) {
    const st = await client.state(create(StateRequestSchema, { id: slowSid }))
    if (st.state?.['status'] === 'idle') {
      slowIdle = true
      break
    }
    await sleep(250)
  }
  check('interrupted turn converges to idle', slowIdle)

  const compact = await client.compact(
    create(CompactRequestSchema, { id: sid2 }),
  )
  check(
    'compact returns a boolean',
    typeof compact.ok === 'boolean',
    compact.ok,
  )

  // -------------------------------------------------------------------------
  section('listSessions / delete')
  const listed = await client.listSessions(
    create(ListSessionsRequestSchema, {}),
  )
  check(
    'listSessions includes main',
    listed.sessions.some(s => s.name === sid2),
  )
  check(
    'listSessions carries message_seq',
    listed.sessions.some(s => s.name === sid2 && s.messageSeq >= 1),
  )

  await client.deleteSession(
    create(DeleteSessionRequestSchema, { id: slowSid }),
  )
  let slowGone = false
  try {
    await client.getSession(create(GetSessionRequestSchema, { id: slowSid }))
  } catch {
    slowGone = true
  }
  check('deleteSession removes session', slowGone)

  await client.deleteSession(
    create(DeleteSessionRequestSchema, { id: toolSid }),
  )
  await client.deleteSession(
    create(DeleteSessionRequestSchema, { id: forkName }),
  )

  // -------------------------------------------------------------------------
  section('provider delete')
  const delProv = await client.deleteProvider(
    create(DeleteProviderRequestSchema, { providerId: 'openai' }),
  )
  check('deleteProvider', delProv.ok)
  const providers2 = await client.listProviders(
    create(ListProvidersRequestSchema, {}),
  )
  check(
    'provider removed',
    !providers2.providers.some(p => p.providerId === 'openai'),
  )
}

void main()
