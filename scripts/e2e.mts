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
  AdminService,
  CreateTenantRequestSchema,
  DeleteTenantRequestSchema,
  IssueTenantTokenRequestSchema,
  ListTenantsRequestSchema,
  ListTenantTokensRequestSchema,
  RevokeTenantTokenRequestSchema,
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
  ListProvidersCatalogRequestSchema,
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

// ---- multi-tenant identity for the e2e run ----
const E2E_ADMIN_TOKEN = 'e2e-admin-token'
const E2E_TENANT = 'e2e'
const E2E_TENANT_TOKEN = 'e2e-tenant-token'

/** Attach a bearer token to every Connect client call (unary + stream). */
function bearerInterceptor(token: string = E2E_TENANT_TOKEN) {
  return (next: (req: { header: { set(k: string, v: string): void } }) => unknown) =>
    (req: { header: { set(k: string, v: string): void } }) => {
      req.header.set('authorization', `Bearer ${token}`)
      return next(req) as never
    }
}

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
  embeddingRequests: number
  imageRequests: number
  speechRequests: number
  transcriptionRequests: number
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
  /** Explicit tool arguments (otherwise a per-tool default is used). */
  args?: Record<string, unknown>
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
  if (model === 'mock-vlm' && !hasTool) {
    // The user text is the uploaded image code; echo it into the image-read
    // call so the VLM tool resolves a real blob.
    const blob = JSON.stringify(body['messages'] ?? [])
    const m = blob.match(/[0-9a-f]{16}/)
    return {
      reasoning: '',
      text: '',
      toolCall: true,
      toolName: 'image-read',
      args: { code: m?.[0] ?? '', prompt: 'what is this?' },
      slowMs: 0,
    }
  }
  if (model === 'mock-vlm')
    return { reasoning: '', text: 'VLM-OK', toolCall: false, toolName: '', slowMs: 0 }
  if (model === 'mock-sub') {
    // Drives the subsession flow. The two roles are distinguished by markers
    // that only ONE side ever sees:
    //   - MAIL_SEND_RESULT  => the CHILD (the handoff prompt carries it)
    //   - SUBSESSION_SPAWN  => the PARENT (the user prompt carries it)
    // `hasTool` ends each role's tool loop with a plain text reply.
    const blob = JSON.stringify(body['messages'] ?? [])
    if (blob.includes('MAIL_SEND_RESULT')) {
      if (hasTool)
        return { reasoning: '', text: 'CHILD-DONE', toolCall: false, toolName: '', slowMs: 0 }
      return {
        reasoning: '',
        text: '',
        toolCall: true,
        toolName: 'mail-send',
        args: { to: 'mailsend-parent', text: 'CHILD-RESULT-42' },
        slowMs: 0,
      }
    }
    if (blob.includes('SUBSESSION_SPAWN')) {
      if (hasTool)
        return { reasoning: '', text: 'PARENT-DONE', toolCall: false, toolName: '', slowMs: 0 }
      return {
        reasoning: '',
        text: '',
        toolCall: true,
        toolName: 'subsession-create',
        args: {
          name: 'mailsend-child',
          prompt:
            'Do the child task. MAIL_SEND_RESULT to mailsend-parent when done.',
        },
        slowMs: 0,
      }
    }
    return { reasoning: '', text: 'SUB-OK', toolCall: false, toolName: '', slowMs: 0 }
  }
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
): Promise<{ url: string; gatewayUrl: string; stop: () => Promise<void> }> {
  const port = await freePort()
  // A 1x1 transparent PNG — enough for the image toolchain round-trip.
  const pngB64 =
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=='
  const server = createServer((req, res) => {
    const url = req.url ?? ''
    // ---- Realtime client-secret mint (AI SDK gateway + openai protocols) ----
    // The gateway factory POSTs to {origin}/v1/realtime/client-secrets and the
    // OpenAI factory to {baseURL}/realtime/client_secrets; both return an
    // ephemeral token the browser uses to open the WebSocket.
    if (
      req.method === 'POST' &&
      (url.endsWith('/realtime/client-secrets') ||
        url.endsWith('/realtime/client_secrets'))
    ) {
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ token: 'vcst_e2e_token', expiresAt: 4102444800 }))
      return
    }
    // ---- Vercel-AI-SDK-compatible gateway (v4 wire) ----
    if (url.startsWith('/v4/ai/')) {
      const chunks: Buffer[] = []
      req.on('data', c => chunks.push(c as Buffer))
      req.on('end', () => {
        const raw = Buffer.concat(chunks).toString('utf8')
        const body = raw === '' ? {} : (JSON.parse(raw) as Record<string, unknown>)
        const sendJson = (obj: unknown) => {
          res.writeHead(200, { 'content-type': 'application/json' })
          res.end(JSON.stringify(obj))
        }
        if (url.endsWith('/image-model')) {
          state.imageRequests++
          sendJson({ images: [pngB64] })
          return
        }
        if (url.endsWith('/speech-model')) {
          state.speechRequests++
          sendJson({ audio: Buffer.from('RIFF....WAVE').toString('base64') })
          return
        }
        if (url.endsWith('/transcription-model')) {
          state.transcriptionRequests++
          sendJson({ text: 'e2e transcript' })
          return
        }
        if (url.endsWith('/video-model/start')) {
          sendJson({ operation: { id: 'op-1' } })
          return
        }
        if (url.endsWith('/video-model/status')) {
          sendJson({
            status: 'completed',
            videos: [
              { type: 'base64', data: pngB64, mediaType: 'video/mp4' },
            ],
          })
          return
        }
        if (url.endsWith('/language-model')) {
          const model = String(
            (req.headers['ai-language-model-id'] as string) ?? '',
          )
          const text = model === 'gw-mock-text' ? 'GW-HELLO' : 'GW-OK'
          sendJson({
            content: [{ type: 'text', text }],
            finishReason: { unified: 'stop', raw: 'stop' },
            usage: {
              inputTokens: { total: 3, noCache: 3, cacheRead: 0, cacheWrite: 0 },
              outputTokens: { total: 2, text: 2, reasoning: 0 },
            },
            warnings: [],
          })
          return
        }
        if (url.endsWith('/config')) {
          // Model discovery: the gateway advertises each model's kind.
          sendJson({
            models: [
              {
                id: 'gw-mock-text',
                name: 'GW Mock Text',
                specification: {
                  specificationVersion: 'v4',
                  provider: 'mock',
                  modelId: 'gw-mock-text',
                },
                modelType: 'language',
              },
              {
                id: 'gw-image',
                name: 'GW Image',
                specification: {
                  specificationVersion: 'v4',
                  provider: 'mock',
                  modelId: 'gw-image',
                },
                modelType: 'image',
              },
              {
                id: 'gw-video',
                name: 'GW Video',
                specification: {
                  specificationVersion: 'v4',
                  provider: 'mock',
                  modelId: 'gw-video',
                },
                modelType: 'video',
              },
              {
                id: 'gw-tts',
                name: 'GW TTS',
                specification: {
                  specificationVersion: 'v4',
                  provider: 'mock',
                  modelId: 'gw-tts',
                },
                modelType: 'speech',
              },
              {
                id: 'gw-asr',
                name: 'GW ASR',
                specification: {
                  specificationVersion: 'v4',
                  provider: 'mock',
                  modelId: 'gw-asr',
                },
                modelType: 'transcription',
              },
            ],
          })
          return
        }
        res.writeHead(404, { 'content-type': 'application/json' })
        res.end('{"error":"unknown gateway path"}')
      })
      return
    }
    // ---- OpenAI-compatible text/image/speech/transcription ----
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
    // Speech (TTS) endpoint: the OpenAI speech model reads the body as raw
    // audio bytes (any content-type works).
    if (req.method === 'POST' && req.url?.endsWith('/audio/speech')) {
      state.speechRequests++
      res.writeHead(200, { 'content-type': 'audio/wav' })
      res.end(Buffer.from('RIFF....WAVEfmt '))
      return
    }
    // Embeddings endpoint: JSON {input:[..]} -> vector list.
    if (req.method === 'POST' && req.url?.endsWith('/embeddings')) {
      state.embeddingRequests++
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(
        JSON.stringify({
          object: 'list',
          data: [
            { object: 'embedding', index: 0, embedding: [0.1, 0.2, 0.3] },
            { object: 'embedding', index: 1, embedding: [0.4, 0.5, 0.6] },
          ],
        }),
      )
      return
    }
    // Transcription (ASR) endpoint: multipart in, JSON {text} out.
    if (req.method === 'POST' && req.url?.endsWith('/audio/transcriptions')) {
      state.transcriptionRequests++
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ text: 'e2e transcript' }))
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
          plan.args ??
          (plan.toolName === 'image-generate'
            ? { prompt: 'a cat' }
            : {
                todos: [
                  { content: 'e2e todo', status: 'pending', priority: 'high' },
                ],
              })
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
            plan.args ??
            (plan.toolName === 'image-generate'
              ? { prompt: 'a cat' }
              : {
                  todos: [
                    {
                      content: 'e2e todo',
                      status: 'pending',
                      priority: 'high',
                    },
                  ],
                })
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
    gatewayUrl: `http://127.0.0.1:${port}/v4/ai`,
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
    // Binary resolution is the SDK's: ABC_NATS_SERVER_BIN -> PATH lookup.
    const natsBin = process.env['ABC_NATS_SERVER_BIN'] ?? 'nats-server'
    const nats = await startNats({ storage: 'memory', binary: natsBin })
    natsUrl = nats.url
    natsStop = nats.stop
    console.log(`[e2e] nats ${natsUrl}`)
  }

  const state: MockState = {
    lastReasoningEffort: undefined,
    requests: 0,
    embeddingRequests: 0,
    imageRequests: 0,
    speechRequests: 0,
    transcriptionRequests: 0,
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
        // First-boot identity: an admin token + one tenant/token pair.
        AGENT_AUTH_MODE: 'required',
        AGENT_ADMIN_TOKEN: E2E_ADMIN_TOKEN,
        AGENT_BOOTSTRAP_TENANT: E2E_TENANT,
        AGENT_BOOTSTRAP_TOKEN: E2E_TENANT_TOKEN,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    agent.stderr?.on('data', d => {
      const s = String(d)
      agentLog.push(s)
      if (process.env['E2E_DEBUG'] === '1') process.stderr.write(`[agent] ${s}`)
    })
    agent.stdout?.on('data', d => {
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
  const transport = createConnectTransport({
    baseUrl,
    httpVersion: '1.1',
    interceptors: [bearerInterceptor()],
  })
  const client: Client<typeof AgentService> = createClient(
    AgentService,
    transport,
  )
  const adminTransport = createConnectTransport({
    baseUrl,
    httpVersion: '1.1',
    interceptors: [bearerInterceptor(E2E_ADMIN_TOKEN)],
  })
  const admin: Client<typeof AdminService> = createClient(
    AdminService,
    adminTransport,
  )
  void admin

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

  // ---- CORS (browser / Flutter Web clients) ----
  {
    const pre = await fetch(`${baseUrl}/agent.v1.AgentService/Health`, {
      method: 'OPTIONS',
      headers: {
        origin: 'https://agent-web.example',
        'access-control-request-method': 'POST',
        'access-control-request-headers': 'content-type',
      },
    })
    check('CORS preflight returns 204', pre.status === 204, pre.status)
    check(
      'CORS preflight allows origin',
      pre.headers.get('access-control-allow-origin') !== null,
      pre.headers.get('access-control-allow-origin'),
    )
    const post = await fetch(`${baseUrl}/agent.v1.AgentService/Health`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        origin: 'https://agent-web.example',
      },
      body: '{}',
    })
    check(
      'CORS headers on actual response',
      post.headers.get('access-control-allow-origin') !== null,
      post.headers.get('access-control-allow-origin'),
    )
  }

  // Wait until the bundled extension has registered its tools.
  for (let i = 0; i < 60; i++) {
    const r = await client.listTools({})
    if (r.tools.length >= 12) break
    await sleep(200)
  }

  try {
    await run(client, admin, state, mock.url, mock.gatewayUrl, baseUrl)
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
  admin: Client<typeof AdminService>,
  state: MockState,
  mockUrl: string,
  gatewayUrl: string,
  baseUrl: string,
): Promise<void> {
  const uniq = Date.now()

  // -------------------------------------------------------------------------
  section('health / provider registration')
  const health = await client.health({})
  check('health ok', health.ok, health)
  check('health name', health.name.length > 0, health.name)

  // Reusable: the mock text provider (deleted in the provider-delete
  // section, re-registered where later sections still need its models).
  const registerMockText = () =>
    client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'openai',
          capability: 'text',
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
            // A model that drives the subsession / mail-send flow.
            create(ProviderModelSchema, {
              id: 'mock-sub',
              name: 'Mock Subsession',
              contextLimit: 100000n,
            }),
            create(ProviderModelSchema, {
              id: 'mock-slow',
              name: 'Mock Slow',
              contextLimit: 100000n,
            }),
            // A text model whose mock response triggers the image-generate
            // tool call (the tool resolves via the `model.image` knob).
            create(ProviderModelSchema, {
              id: 'mock-image',
              name: 'Mock Image Turn',
              contextLimit: 100000n,
            }),
            // A text model that calls image-read (VLM) against an uploaded blob.
            create(ProviderModelSchema, {
              id: 'mock-vlm',
              name: 'Mock VLM Turn',
              contextLimit: 100000n,
            }),
          ],
        }),
      }),
    )
  const reg = await registerMockText()
  check('registerProvider (text)', reg.ok)

  // The Vercel-compatible gateway protocol serves every modality, but a
  // PROVIDER carries exactly ONE (semantic grouping): register one provider
  // per modality, all pointed at the same gateway endpoint.
  const regGw = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway',
        capability: 'image',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [
          create(ProviderModelSchema, { id: 'gw-image', name: 'GW Image' }),
        ],
      }),
    }),
  )
  check('registerProvider (gateway image)', regGw.ok)
  const regGwText = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway-text',
        capability: 'text',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [
          create(ProviderModelSchema, {
            id: 'gw-mock-text',
            name: 'GW Mock Text',
            contextLimit: 100000n,
          }),
        ],
      }),
    }),
  )
  check('registerProvider (gateway text)', regGwText.ok)
  const regGwVideo = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway-video',
        capability: 'video',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [create(ProviderModelSchema, { id: 'gw-video', name: 'GW Video' })],
      }),
    }),
  )
  check('registerProvider (gateway video)', regGwVideo.ok)
  const regGwSpeech = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway-speech',
        capability: 'speech',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [create(ProviderModelSchema, { id: 'gw-tts', name: 'GW TTS' })],
      }),
    }),
  )
  check('registerProvider (gateway speech)', regGwSpeech.ok)
  const regGwAsr = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway-asr',
        capability: 'transcription',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [create(ProviderModelSchema, { id: 'gw-asr', name: 'GW ASR' })],
      }),
    }),
  )
  check('registerProvider (gateway transcription)', regGwAsr.ok)
  const regGwRealtime = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway-realtime',
        capability: 'realtime',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [create(ProviderModelSchema, { id: 'gw-rt', name: 'GW Realtime' })],
      }),
    }),
  )
  check('registerProvider (gateway realtime)', regGwRealtime.ok)

  // context_limit is required > 0 for a text provider.
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
  check('text provider rejects context_limit=0', rejectedBadLimit)

  const providers = await client.listProviders({})
  check(
    'listProviders includes our provider',
    providers.providers.some(p => p.providerId === 'openai'),
    providers.providers.map(p => p.providerId),
  )
  // A provider echoes its single modality + the model's mirrored model_type.
  const gwRow = providers.providers.find(p => p.providerId === 'gateway')
  check(
    'listProviders echoes the provider capability',
    gwRow?.capability === 'image',
    gwRow?.capability,
  )
  const gwImage = gwRow?.models.find(m => m.id === 'gw-image')
  check(
    'listProviders echoes the model kind',
    gwImage?.modelType === 'image',
    gwImage?.modelType,
  )
  const gwVideoRow = providers.providers.find(
    p => p.providerId === 'gateway-video',
  )
  check(
    'listProviders echoes a video provider',
    gwVideoRow?.capability === 'video' &&
      gwVideoRow?.models.find(m => m.id === 'gw-video')?.modelType === 'video',
    gwVideoRow?.capability,
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

  // A text provider carries only text models (all context_limit > 0), so its
  // list equals the registered models.
  check(
    'listModels lists the text provider models',
    models.models.map(m => m.id).sort().join(',') ===
      'gpt-5.4,mock-image,mock-slow,mock-sub,mock-text,mock-tool,mock-vlm',
    models.models.map(m => m.id),
  )

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
  check('testProvider text ok', test.ok, test.result)
  check(
    'testProvider applied variant reasoning_effort',
    state.lastReasoningEffort === 'high',
    state.lastReasoningEffort,
  )

  // Per-capability test probes through the GATEWAY (real smallest-possible
  // generations).
  const gwTest = (providerId: string, model: string, capability: string) =>
    client.testProvider(
      create(TestProviderRequestSchema, {
        providerId,
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        model: `${providerId}/${model}`,
        capability,
      }),
    )
  const tText = await gwTest('gateway-text', 'gw-mock-text', 'text')
  check('testProvider gateway text ok', tText.ok, tText.result)
  const tImage = await gwTest('gateway', 'gw-image', 'image')
  check('testProvider gateway image ok', tImage.ok && tImage.result.includes('image ok'), tImage.result)
  const tSpeech = await gwTest('gateway-speech', 'gw-tts', 'speech')
  check('testProvider gateway speech ok', tSpeech.ok && tSpeech.result.includes('speech ok'), tSpeech.result)
  const tAsr = await gwTest('gateway-asr', 'gw-asr', 'transcription')
  check(
    'testProvider gateway transcription ok',
    tAsr.ok && tAsr.result.includes('transcription ok'),
    tAsr.result,
  )
  const tVideo = await gwTest('gateway-video', 'gw-video', 'video')
  check(
    'testProvider gateway video ok',
    tVideo.ok && tVideo.result.includes('video ok'),
    tVideo.result,
  )
  // realtime has no one-shot generation: the probe mints a short-lived client
  // secret (the mock answers the mint endpoint).
  const tRealtime = await gwTest('gateway-realtime', 'gw-rt', 'realtime')
  check(
    'testProvider gateway realtime ok',
    tRealtime.ok && tRealtime.result.includes('realtime ok'),
    tRealtime.result,
  )
  check(
    'gateway image/speech/transcription endpoints hit',
    state.imageRequests >= 1 &&
      state.speechRequests >= 1 &&
      state.transcriptionRequests >= 1,
    `${state.imageRequests}/${state.speechRequests}/${state.transcriptionRequests}`,
  )
  // The capability matrix: an openai-protocol provider has no VIDEO
  // endpoint — the probe must be rejected before any network call.
  const tBadVideo = await client.testProvider(
    create(TestProviderRequestSchema, {
      providerId: 'openai',
      apiType: 'openai-compatible',
      baseUrl: mockUrl,
      apiKey: 'test-key',
      model: 'openai/mock-slow',
      capability: 'video',
    }),
  )
  check(
    'testProvider rejects video on an openai provider',
    tBadVideo.ok === false && tBadVideo.result.includes('cannot serve'),
    tBadVideo.result,
  )

  // The catalog RPC serves the capability matrix (single source of truth for
  // client registration forms).
  const catalog = await client.listProvidersCatalog(
    create(ListProvidersCatalogRequestSchema, {}),
  )
  check(
    'providers catalog serves the capability matrix',
    (catalog.apiTypes['openai-compatible']?.capabilities ?? []).includes(
      'speech',
    ) &&
      (catalog.apiTypes['vercel-compatible-gateway']?.capabilities ?? [])
        .length === 8 &&
      (catalog.apiTypes['cohere']?.capabilities ?? []).includes('rerank'),
    JSON.stringify(Object.keys(catalog.apiTypes)),
  )

  // -------------------------------------------------------------------------
  section('per-modality provider registration (semantic grouping)')
  // The OpenAI protocol serves embedding/image/speech/transcription too, but
  // each modality is its OWN provider.
  const regCap = (providerId: string, capability: string, id: string) =>
    client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId,
          capability,
          apiType: 'openai-compatible',
          baseUrl: mockUrl,
          apiKey: 'test-key',
          models: [create(ProviderModelSchema, { id, name: id })],
        }),
      }),
    )
  const [regEmb, regImg, regTts, regAsr2] = await Promise.all([
    regCap('oa-embed', 'embedding', 'oa-embed'),
    regCap('oa-image', 'image', 'oa-image'),
    regCap('oa-tts', 'speech', 'oa-tts'),
    regCap('oa-asr', 'transcription', 'oa-asr'),
  ])
  check(
    'per-modality openai providers register',
    regEmb.ok && regImg.ok && regTts.ok && regAsr2.ok,
  )
  const oaTest = (providerId: string, model: string, capability: string) =>
    client.testProvider(
      create(TestProviderRequestSchema, {
        providerId,
        apiType: 'openai-compatible',
        baseUrl: mockUrl,
        apiKey: 'test-key',
        model: `${providerId}/${model}`,
        capability,
      }),
    )
  const oaEmbed = await oaTest('oa-embed', 'oa-embed', 'embedding')
  check(
    'testProvider openai embedding ok',
    oaEmbed.ok && oaEmbed.result.includes('embedding ok'),
    oaEmbed.result,
  )
  const oaImage = await oaTest('oa-image', 'oa-image', 'image')
  check(
    'testProvider openai image ok',
    oaImage.ok && oaImage.result.includes('image ok'),
    oaImage.result,
  )
  const oaSpeech = await oaTest('oa-tts', 'oa-tts', 'speech')
  check(
    'testProvider openai speech ok',
    oaSpeech.ok && oaSpeech.result.includes('speech ok'),
    oaSpeech.result,
  )
  const oaAsr = await oaTest('oa-asr', 'oa-asr', 'transcription')
  check(
    'testProvider openai transcription ok',
    oaAsr.ok && oaAsr.result.includes('transcription ok'),
    oaAsr.result,
  )
  check(
    'openai embeddings endpoint hit',
    state.embeddingRequests >= 1,
    String(state.embeddingRequests),
  )
  // A SECOND gateway provider with an arbitrary id coexists with 'gateway'.
  const regGw2 = await client.registerProvider(
    create(RegisterProviderRequestSchema, {
      provider: create(ProviderSchema, {
        providerId: 'gateway-two',
        capability: 'image',
        apiType: 'vercel-compatible-gateway',
        baseUrl: gatewayUrl,
        apiKey: 'EMPTY',
        models: [create(ProviderModelSchema, { id: 'gw2-image', name: 'GW2 Image' })],
      }),
    }),
  )
  check('a second gateway provider registers', regGw2.ok)
  const gw2Image = await client.testProvider(
    create(TestProviderRequestSchema, {
      providerId: 'gateway-two',
      apiType: 'vercel-compatible-gateway',
      baseUrl: gatewayUrl,
      apiKey: 'EMPTY',
      model: 'gateway-two/gw2-image',
      capability: 'image',
    }),
  )
  check(
    'testProvider second-gateway image ok',
    gw2Image.ok && gw2Image.result.includes('image ok'),
    gw2Image.result,
  )
  // Registration-time matrix rejection: video on an openai provider.
  let videoRejected = false
  try {
    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'oa-bad',
          capability: 'video',
          apiType: 'openai-compatible',
          baseUrl: mockUrl,
          apiKey: 'k',
          models: [create(ProviderModelSchema, { id: 'v', name: 'V' })],
        }),
      }),
    )
  } catch (e) {
    videoRejected = String(e).includes('cannot serve')
  }
  check('registerProvider rejects video on openai', videoRejected)
  // context_limit rules: text requires > 0, non-text requires 0.
  let ctxRule = false
  try {
    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId: 'oa-bad-ctx',
          capability: 'text',
          apiType: 'openai-compatible',
          baseUrl: mockUrl,
          apiKey: 'k',
          models: [create(ProviderModelSchema, { id: 't', name: 'T' })],
        }),
      }),
    )
  } catch (e) {
    ctxRule = String(e).includes('context_limit')
  }
  check('registerProvider enforces context_limit rules', ctxRule)

  // -------------------------------------------------------------------------
  section('tools discovery + i18n')
  const toolsEn = await client.listTools(
    create(ListToolsRequestSchema, { locale: 'en' }),
  )
  const toolNames = toolsEn.tools.map(t => t.name)
  // The bundled toolset grows over time (tts/video/asr/subsession/mail were
  // added after this suite was written); assert the STABLE CORE as a subset
  // instead of pinning the full list.
  const coreTools = [
    'file-info',
    'history-range',
    'history-search',
    'todo-write',
    'web-fetch',
  ]
  check(
    'listTools returns all bundled tools',
    coreTools.every(t => toolNames.includes(t)),
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
    'image-generate requires model.image only',
    (imageGen?.requiredConfig ?? []).join(',') === 'model.image',
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

  // REGRESSION: SetExtensionConfig must actually APPLY (ok:true) and be
  // READABLE back. It used to return an opaque internal error because the
  // handler built a throwaway `new AbcAgent(bus)` per request (empty manifest
  // cache, unstarted config authority) instead of the long-lived agent role.
  const setBrave = await client.setExtensionConfig(
    create(SetExtensionConfigRequestSchema, {
      extId: 'bundled',
      name: 'brave_api_key',
      value: create(ValueSchema, {
        kind: { case: 'stringValue', value: 'e2e-brave' },
      }),
    }),
  )
  check('setExtensionConfig returns ok', setBrave.ok === true, setBrave)
  const readBack = await client.getToolConfig({})
  check(
    'setExtensionConfig value is readable back',
    JSON.stringify(readBack).includes('e2e-brave'),
    JSON.stringify(readBack.config?.values?.['brave-search']),
  )
  // The manifest declares model-reference knobs (kind=model + capability), so
  // a client can render a modality-scoped picker without name heuristics.
  const toolsAfter = await client.listTools(create(ListToolsRequestSchema, {}))
  const imgGen = toolsAfter.tools.find(t => t.name === 'image-generate')
  const imgKnob = imgGen?.configFields.find(c => c.name === 'model.image')
  check(
    'listTools carries kind=model + capability on model knobs',
    imgKnob?.kind === 'model' && imgKnob?.capability === 'image',
    { kind: imgKnob?.kind, capability: imgKnob?.capability },
  )
  // Count is not the assertion target anymore (the toolset grows); the call
  // simply must succeed and keep the brave tool enabled.
  check(
    'setExtensionConfig accepted',
    toolsAfter.tools.some(t => t.name === 'brave-search'),
  )

  // Configure the image tool to point at the mock image model, then drive a
  // full image-generate tool call through the turn loop.
  await client.setExtensionConfig(
    create(SetExtensionConfigRequestSchema, {
      extId: 'bundled',
      name: 'model.image',
      value: create(ValueSchema, {
        kind: { case: 'stringValue', value: 'gateway/gw-image' },
      }),
    }),
  )
  // `image-read` (VLM) resolves its vision model via resolveModel — this
  // reproduces the `Cannot read properties of null (reading 'select')` crash
  // when the resolver received the extension's null db. Configure a vision
  // model and run the tool end-to-end.
  await client.setExtensionConfig(
    create(SetExtensionConfigRequestSchema, {
      extId: 'bundled',
      name: 'model.text',
      value: create(ValueSchema, {
        kind: { case: 'stringValue', value: 'openai/mock-text' },
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
  // The live tool-result event carries the structured `data` (media refs) so a
  // client can render generated images/audio/video directly.
  const imgData = imgResult?.params['data'] as
    | { images?: Array<{ code?: string }> }
    | undefined
  const imgCode = imgData?.images?.[0]?.code ?? ''
  check(
    'image tool-result carries data.images[].code',
    /^[0-9a-f]{16}$/.test(imgCode),
    imgCode,
  )
  check(
    'gateway /image-model was hit',
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
  // First materialize the context-id cache (a prompt does loadHistory →
  // putSessionIds). Then undo must AWAIT deleteSessionIds; otherwise the cache
  // still holds the withdrawn messages and the next prompt would re-read them.
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
  // The undo must have refreshed the session-list projection immediately. The
  // new tip is the assistant TOOL-CALL step (no text part), so the preview is
  // empty — but it must no longer be the withdrawn 'TOOL-OK' text, and the
  // timestamp must still be present.
  const undoneRow = (await client.listSessions({})).sessions.find(
    s => s.name === toolSid,
  )
  check(
    'undo refreshes the message fact (time present, preview no longer withdrawn)',
    undoneRow !== undefined &&
      undoneRow.lastMessageAt !== '' &&
      undoneRow.lastMessagePreview !== 'TOOL-OK',
    `${undoneRow?.lastMessageAt} | ${JSON.stringify(undoneRow?.lastMessagePreview)}`,
  )
  const seqBefore = undoneRow?.messageSeq ?? 0
  // A follow-up prompt right after the undo must NOT resurrect the withdrawn
  // messages: the model only ever sees the new user text (+ earlier chain).
  const afterUndoPrompt = client.prompt(
    create(PromptRequestSchema, { id: toolSid, prompt: 'after undo' }),
  )
  for await (const e of afterUndoPrompt) {
    if (e.event === 'accepted') break
  }
  await sleep(500)
  const seqAfter = (
    await client.getSession(create(GetSessionRequestSchema, { id: toolSid }))
  ).session?.messageSeq
  check(
    'undo preserves message_seq (withdraw is not a new message)',
    seqAfter !== undefined && seqAfter >= seqBefore,
    `${seqBefore} -> ${seqAfter}`,
  )

  // ---- pin-based incremental listing (ListMessages.after/resync) ----------
  // A fresh session with a couple of turns: baseline, then an incremental read
  // from an anchor, then a resync after the anchor is withdrawn.
  const incSid = `e2e-inc-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: incSid,
      model: 'openai/mock-text',
      preset: 'default',
    }),
  )
  const incPrompt = async (text: string) => {
    const s = client.prompt(create(PromptRequestSchema, { id: incSid, prompt: text }))
    for await (const e of s) {
      if (e.event === 'accepted') break
    }
    await sleep(400)
  }
  await incPrompt('first')
  const base = await client.listMessages(
    create(ListMessagesRequestSchema, { id: incSid, limit: 50 }),
  )
  const anchor = base.messages[base.messages.length - 1]?.id ?? ''
  check('incremental: baseline has tip id', base.tipId !== '' && anchor !== '')

  // No new messages → delta is empty, anchor reached, no resync.
  const emptyDelta = await client.listMessages(
    create(ListMessagesRequestSchema, { id: incSid, after: anchor, limit: 50 }),
  )
  check(
    'incremental: same anchor yields empty delta, no resync',
    emptyDelta.resync === false && emptyDelta.messages.length === 0,
    `${emptyDelta.resync} ${emptyDelta.messages.length}`,
  )

  // A new turn → delta contains only the appended messages.
  await incPrompt('second')
  const delta = await client.listMessages(
    create(ListMessagesRequestSchema, { id: incSid, after: anchor, limit: 50 }),
  )
  check(
    'incremental: delta returns appended messages',
    delta.resync === false && delta.messages.length > 0,
    `${delta.messages.length}`,
  )
  check(
    'incremental: delta excludes the anchor message',
    delta.messages.every(m => m.id !== anchor),
  )
  check(
    'incremental: echo tip id advances',
    delta.tipId !== '' && delta.tipId !== base.tipId,
    `${base.tipId} -> ${delta.tipId}`,
  )

  // Withdraw past the anchor → the anchor is no longer on the chain ⇒ resync.
  await client.undo(create(UndoRequestSchema, { id: incSid, messageId: anchor }))
  const afterUndoDelta = await client.listMessages(
    create(ListMessagesRequestSchema, { id: incSid, after: anchor, limit: 50 }),
  )
  check(
    'incremental: withdrawn anchor signals resync',
    afterUndoDelta.resync === true,
    afterUndoDelta.resync,
  )
  await client.deleteSession(create(DeleteSessionRequestSchema, { id: incSid }))

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
  const clientCode = `e2e${uniq}`
  const upFile = await client.uploadFile(
    create(UploadFileRequestSchema, {
      file: create(FileRefSchema, {
        code: clientCode,
        name: 'hello.txt',
        mime: 'text/plain',
        size: fileBytes.length,
      }),
      data: fileBytes.toString('base64'),
    }),
  )
  // The server MINTS the code (16 hex); the client-supplied code is ignored so
  // every file code in the system is uniform.
  const code = upFile.code
  check(
    'uploadFile mints a 16-hex code (ignores client code)',
    upFile.ok && /^[0-9a-f]{16}$/.test(code) && code !== clientCode,
    code,
  )
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

  // A prompt carrying attachment refs must persist a `file` part per code
  // (this is the path the Flutter client uses for picked images/files/audio).
  const attSid = `e2e-att-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: attSid,
      model: 'openai/mock-text',
      preset: 'default',
    }),
  )
  const attStream = client.prompt(
    create(PromptRequestSchema, {
      id: attSid,
      prompt: '',
      attachments: [create(FileRefSchema, { code: ingest.code })],
    }),
  )
  for await (const e of attStream) {
    if (e.event === 'accepted') break
  }
  const attMsgs = await client.listMessages(
    create(ListMessagesRequestSchema, { id: attSid, limit: 20 }),
  )
  const attPart = attMsgs.messages
    .flatMap(m => m.parts)
    .find(p => p.type === 'file')
  check(
    'prompt attachment persists a file part',
    attPart !== undefined && attPart.data.includes(ingest.code),
    attPart?.data,
  )
  // The persisted file part must carry real metadata resolved from the stored
  // blob (name/mime/size), otherwise history renders no thumbnail. The client
  // sends only the code, so this proves the server filled the rest in.
  const attData = attPart === undefined
    ? {}
    : (JSON.parse(attPart.data) as Record<string, unknown>)
  check(
    'prompt attachment persists resolved name/mime/size',
    attData['name'] === 'ingested.txt' &&
      attData['mime'] === 'text/plain' &&
      Number(attData['size']) > 0,
    attPart?.data,
  )
  await client.deleteSession(create(DeleteSessionRequestSchema, { id: attSid }))

  // ---- image-read (VLM): resolveModel with the extension's null db --------
  // Reproduces the `Cannot read properties of null (reading 'select')` crash:
  // the extension passes null as its structural `db`, but the resolver must
  // use the server's real handle.
  const vlmSid = `e2e-vlm-${uniq}`
  await client.createSession(
    create(CreateSessionRequestSchema, {
      name: vlmSid,
      model: 'openai/mock-vlm',
      preset: 'default',
    }),
  )
  const vlmEvents: WatchEv[] = []
  const vac = new AbortController()
  const vlmWatcher = (async () => {
    try {
      const stream = client.watchSession(
        create(WatchSessionRequestSchema, { id: vlmSid }),
        { signal: vac.signal },
      )
      for await (const ev of stream) {
        vlmEvents.push({
          event: ev.event,
          params: (ev.params ?? {}) as Record<string, unknown>,
        })
      }
    } catch {
      /* aborted */
    }
  })()
  const vc = collectSession(vlmEvents)
  await sleep(300)
  const vlmPrompt = client.prompt(
    create(PromptRequestSchema, { id: vlmSid, prompt: ingest.code }),
  )
  for await (const e of vlmPrompt) {
    if (e.event === 'accepted') break
  }
  const vlmDone = await vc.waitFor(e => e.event === 'turn-complete', 30000)
  check('image-read turn complete', vlmDone !== null)
  const vlmResult = vlmEvents.find(e => e.event === 'tool-result')
  check(
    'image-read tool ran (resolveModel did not crash)',
    vlmResult !== undefined &&
      String(vlmResult.params['toolName'] ?? '').includes('image-read'),
    vlmResult?.params,
  )
  vac.abort()
  await vlmWatcher.catch(() => {})
  await client.deleteSession(create(DeleteSessionRequestSchema, { id: vlmSid }))

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

  // -------------------------------------------------------------------------
  section('admin: tenants + tokens')
  const t0 = await admin.listTenants(create(ListTenantsRequestSchema, {}))
  check(
    'bootstrap tenant present',
    t0.tenants.some(t => t.id === E2E_TENANT),
    t0.tenants.map(t => t.id).join(','),
  )
  const createdT = await admin.createTenant(
    create(CreateTenantRequestSchema, { id: 'acme', name: 'Acme' }),
  )
  check('createTenant returns tenant', createdT.tenant?.id === 'acme')
  check(
    'createTenant mints a bootstrap token',
    (createdT.token ?? '').length > 0,
  )
  // The freshly minted token authenticates as its own tenant, isolated from e2e.
  const acmeClient: Client<typeof AgentService> = createClient(
    AgentService,
    createConnectTransport({
      baseUrl,
      httpVersion: '1.1',
      interceptors: [bearerInterceptor(createdT.token ?? '')],
    }),
  )
  const acmeSessions = await acmeClient.listSessions(
    create(ListSessionsRequestSchema, {}),
  )
  check(
    'tenant token is isolated (no e2e sessions)',
    acmeSessions.sessions.length === 0,
    String(acmeSessions.sessions.length),
  )
  // Providers are tenant-scoped (v2): acme has none of its own and must not
  // see e2e's, so the session is created WITHOUT a model (a session may exist
  // without one until a turn needs it) and, symmetrically, referencing e2e's
  // model from acme must FAIL.
  const acmeCreate = await acmeClient.createSession(
    create(CreateSessionRequestSchema, {
      name: 'acme-only',
      preset: 'default',
    }),
  )
  check('tenant token can create its own session', acmeCreate.ok)
  let acmeCrossModel = false
  try {
    await acmeClient.createSession(
      create(CreateSessionRequestSchema, {
        name: 'acme-cross',
        model: 'openai/gpt-5.4',
        preset: 'default',
      }),
    )
  } catch {
    acmeCrossModel = true
  }
  check('tenant cannot use another tenant\'s provider model', acmeCrossModel)

  // Issue + revoke a token: the issued one works, then fails after revocation.
  const issued = await admin.issueTenantToken(
    create(IssueTenantTokenRequestSchema, { tenantId: 'acme', label: 'extra' }),
  )
  check('issueTenantToken returns plaintext', (issued.plaintext ?? '').length > 0)
  const before = await admin.listTenantTokens(
    create(ListTenantTokensRequestSchema, { tenantId: 'acme' }),
  )
  check(
    'acme has 2 active tokens',
    before.tokens.filter(t => !t.revoked).length === 2,
    String(before.tokens.length),
  )
  await admin.revokeTenantToken(
    create(RevokeTenantTokenRequestSchema, { tokenId: issued.token!.tokenId }),
  )
  const revokedClient: Client<typeof AgentService> = createClient(
    AgentService,
    createConnectTransport({
      baseUrl,
      httpVersion: '1.1',
      interceptors: [bearerInterceptor(issued.plaintext ?? '')],
    }),
  )
  let revokedRejected = false
  try {
    await revokedClient.listSessions(create(ListSessionsRequestSchema, {}))
  } catch (e) {
    revokedRejected = String(e).includes('unauthenticated') ||
      String(e).includes('invalid or revoked')
  }
  check('revoked token is rejected', revokedRejected)

  // Disable the tenant (soft): its live token stops working.
  await admin.deleteTenant(create(DeleteTenantRequestSchema, { id: 'acme' }))
  let disabledRejected = false
  try {
    await acmeClient.listSessions(create(ListSessionsRequestSchema, {}))
  } catch (e) {
    disabledRejected = String(e).includes('unauthenticated') ||
      String(e).includes('invalid or revoked')
  }
  check('disabled tenant token is rejected', disabledRejected)
  const t1 = await admin.listTenants(create(ListTenantsRequestSchema, {}))
  check(
    'tenant is disabled not deleted (data kept)',
    t1.tenants.find(t => t.id === 'acme')?.disabled === true,
  )

  // A tenant token can never reach the admin surface. listTenants moved to
  // AdminService in v2, so the probe must go through an AdminService client
  // authenticated with a TENANT token (the AgentService client would fail
  // with method-not-found instead of permission_denied).
  const tenantAdmin: Client<typeof AdminService> = createClient(
    AdminService,
    createConnectTransport({
      baseUrl,
      httpVersion: '1.1',
      interceptors: [bearerInterceptor()],
    }),
  )
  let tenantBlocked = false
  try {
    await tenantAdmin.listTenants(create(ListTenantsRequestSchema, {}))
  } catch (e) {
    tenantBlocked = String(e).includes('permission_denied') ||
      String(e).includes('admin token')
  }
  check('tenant token cannot call admin RPCs', tenantBlocked)

  // -------------------------------------------------------------------------
  // subsession: parent spawns a child (O(1) fork), the child does its work and
  // mails the result BACK to the parent, which is then resumed. Exercises BOTH
  // new tools (subsession-create, mail-send) end-to-end through the turn loop.
  // -------------------------------------------------------------------------
  section('subsession + mail-send')
  // The provider-delete section removed 'openai'; the subsession flow drives
  // through its mock-sub model, so re-register the (mock) provider first.
  const subReg = await registerMockText()
  check('subsession: mock provider re-registered', subReg.ok)
  // FIXED name: the mock's mail-send reply targets 'mailsend-parent'
  // verbatim, so the parent must carry exactly that name (the run DB is a
  // throwaway sqlite file, no cross-run collision to avoid).
  const subParent = 'mailsend-parent'
  const subChild = 'mailsend-child'
  const subParentCreate = await client.createSession(
    create(CreateSessionRequestSchema, {
      name: subParent,
      model: 'openai/mock-sub',
      preset: 'default',
    }),
  )
  check('subsession: parent session created', subParentCreate.ok)

  // Watch BOTH sessions so we can observe the spawn and the child's reply.
  const subEvents: WatchEv[] = []
  const subAc = new AbortController()
  const watchOne = async (sid: string) => {
    try {
      const stream = client.watchSession(
        create(WatchSessionRequestSchema, { id: sid }),
        { signal: subAc.signal },
      )
      for await (const ev of stream) {
        subEvents.push({
          event: `${sid}:${ev.event}`,
          params: (ev.params ?? {}) as Record<string, unknown>,
        })
      }
    } catch {
      /* aborted */
    }
  }
  const subWatchParent = watchOne(subParent)
  const subWatcherChild = watchOne(subChild).catch(() => {})
  void subWatcherChild
  await sleep(300)

  // 1. Parent prompts with the SPAWN marker → the mock model calls
  //    subsession-create, which forks a child and wakes it.
  const spawnStream = client.prompt(
    create(PromptRequestSchema, {
      id: subParent,
      prompt: 'Go: SUBSESSION_SPAWN a child to do the work.',
    }),
  )
  for await (const e of spawnStream) {
    if (e.event === 'accepted') break
  }

  // 2. Wait for the child session to appear on the server (the fork committed).
  let childExists = false
  for (let i = 0; i < 60; i++) {
    try {
      const g = await client.getSession(
        create(GetSessionRequestSchema, { id: subChild }),
      )
      if (g.session !== undefined) {
        childExists = true
        // The child is a fork of the parent: it must carry group = parent and
        // inherit the parent's model (same preset → prompt-cache reuse).
        check(
          'subsession: child.group == parent name',
          g.session.group === subParent,
          g.session.group,
        )
        check(
          'subsession: child inherits parent model',
          g.session.model === 'openai/mock-sub',
          g.session.model,
        )
        break
      }
    } catch {
      /* not yet */
    }
    await sleep(200)
  }
  check('subsession: child session created', childExists)

  // 3. Wait for the CHILD to finish (its turn ends with a text reply after it
  //    mails the parent) — observable as a turn-complete on the child stream.
  let childDone = false
  for (let i = 0; i < 150; i++) {
    if (subEvents.some(e => e.event === `${subChild}:turn-complete`)) {
      childDone = true
      break
    }
    await sleep(200)
  }
  check('subsession: child ran a turn', childDone)

  // 4. The child mailed the result to the parent; the parent was resumed and
  //    now holds CHILD-RESULT-42 in its history.
  let parentSawResult = false
  for (let i = 0; i < 150; i++) {
    const msgs = await client.listMessages(
      create(ListMessagesRequestSchema, { id: subParent, limit: 50 }),
    )
    const body = JSON.stringify(msgs.messages)
    if (body.includes('CHILD-RESULT-42')) {
      parentSawResult = true
      break
    }
    await sleep(200)
  }
  check('subsession: parent received child result via mail-send', parentSawResult)

  // 5. Re-spawning the SAME child name is refused (the tool reports it and no
  //    duplicate session appears). The child itself also cannot nest: its
  //    group is non-empty (checked above), which is the nesting guard.
  const respawn = client.prompt(
    create(PromptRequestSchema, {
      id: subParent,
      prompt: 'Again: SUBSESSION_SPAWN the same child name.',
    }),
  )
  for await (const e of respawn) {
    if (e.event === 'accepted') break
  }
  await sleep(1500)
  const subSessions = await client.listSessions(create(ListSessionsRequestSchema, {}))
  const childCopies = subSessions.sessions.filter(s => s.name === subChild)
  check(
    'subsession: duplicate child name not created',
    childCopies.length === 1,
    String(childCopies.length),
  )

  subAc.abort()
  await subWatchParent.catch(() => {})
}

void main()
