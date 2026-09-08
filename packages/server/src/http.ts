import { type IncomingHttpHeaders, ServerResponse } from 'node:http'
import { Http2ServerResponse } from 'node:http2'
import type { ConnectNodeAdapterOptions } from '@connectrpc/connect-node'
import type { AgentDeps } from '@easylab-agent/agent'
import type { ZodType, z } from 'zod'

/**
 * A tiny native-node HTTP micro-framework replacing Hono for the REST facade.
 *
 * The whole server speaks HTTP/2 (h2c prior knowledge, see index.ts); the
 * connect-node adapter hands its fallback the union node request/response
 * types it supports (http1 | http2). Instead of casting those unions away we
 * declare minimal structural "port" interfaces both concrete types satisfy —
 * assignability is verified by the compiler at every return, so the code is
 * fully strongly typed with zero `as` casts.
 *
 * Handlers keep the fetch `Response` ergonomics (`c.json(...)`, SSE streams
 * as plain `Response` bodies). RPC traffic never touches this file — it is
 * served by connectNodeAdapter directly (trailers included).
 */

/** Extract a zod schema's output type (zod's own inference helper). */

/** The node request shape the connect-node fallback passes down. */
export type NodeReq = Parameters<
  NonNullable<ConnectNodeAdapterOptions['fallback']>
>[0]
/** The node response shape the connect-node fallback passes down. */
export type NodeRes = Parameters<
  NonNullable<ConnectNodeAdapterOptions['fallback']>
>[1]

/**
 * Structural view of a node response both http1 ServerResponse and http2
 * Http2ServerResponse satisfy. Declaring the port lets every helper accept
 * the adapter's union type without a single cast.
 */
interface ResPort {
  writeHead(status: number, headers: Record<string, string>): void
  write(chunk: Uint8Array): boolean
  end(data?: string | Uint8Array): void
  once(event: 'drain', listener: () => void): void
  readonly destroyed: boolean
  readonly headersSent: boolean
}

/** Structural view of a node request (http1 or http2) the bridge consumes. */
interface ReqPort extends AsyncIterable<string | Buffer> {
  readonly method?: string | undefined
  readonly url?: string | undefined
  readonly headers: IncomingHttpHeaders
}

/**
 * Adapt the adapter's union node response onto the structural port. Each
 * branch's return is assignability-checked against ResPort by the compiler —
 * this compiles only when the concrete node type really satisfies the port,
 * so it is fully typed with no casts.
 */
function resPort(res: NodeRes): ResPort {
  if (res instanceof ServerResponse) return res
  if (res instanceof Http2ServerResponse) return res
  throw new Error('unsupported node response type')
}

/** Adapt the adapter's union node request onto the structural port. */
function reqPort(req: NodeReq): ReqPort {
  return req satisfies ReqPort
}

export interface Ctx<S = unknown> {
  deps: AgentDeps
  req: {
    method: string
    path: string
    params: Record<string, string>
    query: URLSearchParams
    raw: Request
  }
  /** Parsed + schema-validated JSON body (typed by the route's schema). */
  body: S
  json(data: unknown, status?: number): Response
}

export type Handler<S = unknown> = (c: Ctx<S>) => Promise<Response> | Response

/** Everything a route needs to serve a request, before body parsing. */
interface RouteArgs {
  deps: AgentDeps
  method: string
  path: string
  params: Record<string, string>
  query: URLSearchParams
  raw: Request
}

interface Route {
  method: string
  segments: string[]
  run(args: RouteArgs): Promise<Response> | Response
}

function makeCtx<S>(args: RouteArgs, body: S): Ctx<S> {
  return {
    deps: args.deps,
    req: {
      method: args.method,
      path: args.path,
      params: args.params,
      query: args.query,
      raw: args.raw,
    },
    body,
    json: (data, status = 200) => Response.json(data, { status }),
  }
}

/** Path segments: a leading ':' marks a named parameter segment. */
function segmentsOf(pattern: string): string[] {
  return pattern.split('/').filter(s => s !== '')
}

function matchSegments(
  pattern: string[],
  path: string[],
): Record<string, string> | null {
  if (pattern.length !== path.length) return null
  const params: Record<string, string> = {}
  for (let i = 0; i < pattern.length; i++) {
    const p = pattern[i]
    const v = path[i]
    if (p === undefined || v === undefined) return null
    if (p.startsWith(':')) {
      params[p.slice(1)] = decodeURIComponent(v)
    } else if (p !== v) {
      return null
    }
  }
  return params
}

export class Router {
  readonly #routes: Route[] = []

  /** Register a route with a JSON body validated (and typed) by `bodySchema`. */
  add<S extends ZodType>(
    method: string,
    pattern: string,
    handler: Handler<z.output<S>>,
    bodySchema: S,
  ): this

  /** Register a route with no request body. */
  add(method: string, pattern: string, handler: Handler<unknown>): this

  add(
    method: string,
    pattern: string,
    handler: Handler<unknown>,
    bodySchema?: ZodType,
  ): this {
    const segments = segmentsOf(pattern)
    const upper = method.toUpperCase()
    if (bodySchema === undefined) {
      this.#routes.push({
        method: upper,
        segments,
        run: args => handler(makeCtx(args, undefined)),
      })
      return this
    }
    const schema = bodySchema
    this.#routes.push({
      method: upper,
      segments,
      run: async args => {
        let json: unknown
        try {
          json = await args.raw.clone().json()
        } catch {
          return Response.json(
            { ok: false, error: 'invalid json body' },
            { status: 400 },
          )
        }
        const parsed = schema.safeParse(json)
        if (!parsed.success) {
          return Response.json(
            { ok: false, error: `invalid body: ${parsed.error.message}` },
            { status: 400 },
          )
        }
        return handler(makeCtx(args, parsed.data))
      },
    })
    return this
  }

  get(pattern: string, handler: Handler<unknown>): this
  get<S extends ZodType>(
    pattern: string,
    handler: Handler<z.output<S>>,
    bodySchema: S,
  ): this
  get(pattern: string, handler: Handler<unknown>, bodySchema?: ZodType): this {
    return bodySchema === undefined
      ? this.add('GET', pattern, handler)
      : this.add('GET', pattern, handler, bodySchema)
  }

  post(pattern: string, handler: Handler<unknown>): this
  post<S extends ZodType>(
    pattern: string,
    handler: Handler<z.output<S>>,
    bodySchema: S,
  ): this
  post(pattern: string, handler: Handler<unknown>, bodySchema?: ZodType): this {
    return bodySchema === undefined
      ? this.add('POST', pattern, handler)
      : this.add('POST', pattern, handler, bodySchema)
  }

  put(pattern: string, handler: Handler<unknown>): this
  put<S extends ZodType>(
    pattern: string,
    handler: Handler<z.output<S>>,
    bodySchema: S,
  ): this
  put(pattern: string, handler: Handler<unknown>, bodySchema?: ZodType): this {
    return bodySchema === undefined
      ? this.add('PUT', pattern, handler)
      : this.add('PUT', pattern, handler, bodySchema)
  }

  patch(pattern: string, handler: Handler<unknown>): this
  patch<S extends ZodType>(
    pattern: string,
    handler: Handler<z.output<S>>,
    bodySchema: S,
  ): this
  patch(
    pattern: string,
    handler: Handler<unknown>,
    bodySchema?: ZodType,
  ): this {
    return bodySchema === undefined
      ? this.add('PATCH', pattern, handler)
      : this.add('PATCH', pattern, handler, bodySchema)
  }

  delete(pattern: string, handler: Handler<unknown>): this
  delete<S extends ZodType>(
    pattern: string,
    handler: Handler<z.output<S>>,
    bodySchema: S,
  ): this
  delete(
    pattern: string,
    handler: Handler<unknown>,
    bodySchema?: ZodType,
  ): this {
    return bodySchema === undefined
      ? this.add('DELETE', pattern, handler)
      : this.add('DELETE', pattern, handler, bodySchema)
  }

  /** Match a request; returns the route + decoded path params or null. */
  match(
    method: string,
    path: string,
  ): { route: Route; params: Record<string, string> } | null {
    const parts = segmentsOf(path)
    for (const route of this.#routes) {
      if (route.method !== method.toUpperCase()) continue
      const params = matchSegments(route.segments, parts)
      if (params !== null) return { route, params }
    }
    return null
  }
}

/** Runtime type guard: a plain string-keyed object (not an array). */
export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

// ---- SSE (Server-Sent Events) as a plain streaming Response ----

export interface SseStream {
  writeSSE(message: { data: string }): Promise<void>
  onAbort(cb: () => void): void
}

/**
 * Run `fn` with a Server-Sent-Events writer and return a streaming Response.
 * Mirrors the surface the hono/streaming handlers used (writeSSE/onAbort) so
 * the replay/live SSE logic ports unchanged.
 */
export function sseResponse(fn: (s: SseStream) => Promise<void>): Response {
  let abort: (() => void) | undefined
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      const enc = new TextEncoder()
      const s: SseStream = {
        writeSSE: async m => {
          controller.enqueue(enc.encode(`data: ${m.data}\n\n`))
        },
        onAbort: cb => {
          abort = cb
        },
      }
      try {
        await fn(s)
      } finally {
        try {
          controller.close()
        } catch {
          // already closed by the consumer going away
        }
      }
    },
    cancel() {
      abort?.()
    },
  })
  return new Response(stream, {
    headers: {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
    },
  })
}

// ---- node <-> fetch bridging (structural, no casts) ----

/** Flatten node's incoming headers into a plain string record. */
function flattenHeaders(headers: IncomingHttpHeaders): Record<string, string> {
  const out: Record<string, string> = {}
  for (const [key, value] of Object.entries(headers)) {
    // HTTP/2 pseudo-headers (:method/:scheme/:authority/:path) are transport
    // metadata, not real headers — fetch's Headers API rejects those names.
    if (key.startsWith(':')) continue
    if (value === undefined) continue
    out[key] = Array.isArray(value) ? value.join(', ') : value
  }
  return out
}

/** Resolve the request authority (h2 :authority, h1 host, fallback). */
function authorityOf(headers: IncomingHttpHeaders): string {
  const raw = headers[':authority'] ?? headers.host
  if (raw === undefined) return 'localhost'
  return Array.isArray(raw) ? raw.join(',') : raw
}

async function portToRequest(req: ReqPort): Promise<Request> {
  const chunks: Buffer[] = []
  for await (const chunk of req) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk)
  }
  const body = Buffer.concat(chunks)
  const url = `http://${authorityOf(req.headers)}${req.url ?? '/'}`
  const method = req.method ?? 'GET'
  const init: RequestInit = {
    method,
    headers: flattenHeaders(req.headers),
  }
  if (body.length > 0 && method !== 'GET' && method !== 'HEAD') {
    init.body = new Uint8Array(body)
  }
  return new Request(url, init)
}

async function sendToPort(res: ResPort, response: Response): Promise<void> {
  const headers: Record<string, string> = {}
  response.headers.forEach((value, key) => {
    headers[key] = value
  })
  res.writeHead(response.status, headers)
  const body = response.body
  if (body === null) {
    res.end()
    return
  }
  const reader = body.getReader()
  for (;;) {
    const { done, value } = await reader.read()
    if (done === true) break
    if (value === undefined) continue
    if (res.destroyed) {
      await reader.cancel()
      return
    }
    if (!res.write(value)) {
      await new Promise<void>(resolve => {
        res.once('drain', () => {
          resolve()
        })
      })
    }
  }
  res.end()
}

function portJsonError(res: ResPort, status: number, message: string): void {
  if (!res.headersSent) {
    res.writeHead(status, { 'content-type': 'application/json' })
  }
  res.end(JSON.stringify({ ok: false, error: message }))
}

/**
 * Dispatch a node request against the REST router. `path` is the
 * router-relative pathname (the /api/v1 prefix already stripped).
 */
export async function handleRest(
  router: Router,
  deps: AgentDeps,
  path: string,
  req: NodeReq,
  res: NodeRes,
): Promise<void> {
  const raw = reqPort(req)
  const out = resPort(res)
  const method = raw.method ?? 'GET'
  const matched = router.match(method, path)
  if (matched === null) {
    portJsonError(out, 404, 'not found')
    return
  }
  try {
    const request = await portToRequest(raw)
    const url = new URL(request.url)
    const response = await matched.route.run({
      deps,
      method,
      path,
      params: matched.params,
      query: url.searchParams,
      raw: request,
    })
    await sendToPort(out, response)
  } catch (err) {
    portJsonError(out, 500, String(err))
  }
}

/** Serve the SPA / static bytes for a non-API path. */
export function serveStatic(
  res: NodeRes,
  getAsset: (key: string) => ArrayBuffer | null,
  pathname: string,
): void {
  const out = resPort(res)
  // Route non-asset paths to the SPA entry.
  const asset =
    pathname === '/' || !pathname.includes('.')
      ? 'index.html'
      : pathname.slice(1)
  const data = getAsset(asset)
  if (data === null) {
    // Fall back to index.html for client-side routing.
    const index = getAsset('index.html')
    if (index === null) {
      portJsonError(out, 404, 'not found')
      return
    }
    out.writeHead(200, { 'content-type': 'text/html' })
    out.end(new Uint8Array(index))
    return
  }
  const ext = asset.slice(asset.lastIndexOf('.'))
  out.writeHead(200, {
    'content-type': MIME[ext] ?? 'application/octet-stream',
  })
  out.end(new Uint8Array(data))
}

const MIME: Record<string, string> = {
  '.html': 'text/html',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.json': 'application/json',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
}
