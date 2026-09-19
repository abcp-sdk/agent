import { Agent as AbcAgent, isSessionRunning } from '@abc-protocol/sdk'
import type { JsonObject, JsonValue } from '@bufbuild/protobuf'
import { create, fromJson, toJson } from '@bufbuild/protobuf'
import { StructSchema, type Value, ValueSchema } from '@bufbuild/protobuf/wkt'
import {
  Code,
  ConnectError,
  type ConnectRouter,
  type HandlerContext,
  type ServiceImpl,
} from '@connectrpc/connect'
import {
  type AgentDeps,
  appendSessionId,
  BUCKET_SESSION_STATE,
  type ChainMessage,
  CONFIG_DEFAULT_MODEL,
  CONFIG_DEFAULT_PRESET,
  Config,
  catalogModel,
  clearActiveRun,
  compactSession,
  DEFAULT_PRESET,
  deleteSessionIds,
  discoverTools,
  factFromPersist,
  fileByCode,
  findVariant,
  fireAndForget,
  GATEWAY_API_TYPE,
  getModelsDev,
  interruptRun,
  localizeSchema,
  Mailbox,
  Messages,
  mailboxSubject,
  maskSecret,
  Parts,
  Presets,
  Providers,
  parse,
  parseCapability,
  parseProviderModelRef,
  pickDescription,
  pickLocalized,
  projectMessageFact,
  publishLifecycle,
  publishSessionChanged,
  pushChainChanged,
  readActiveRun,
  readMessageFacts,
  renderTemplate,
  resolveLocale,
  Sessions,
  TextPartDataSchema,
  toModelVariant,
  toolConfigMap,
  validateApiType,
  variantsForApiType,
  writeMessageFact,
} from '@abcp-agent/agent'
import {
  type AgentService,
  FileChunkSchema,
  GetAgentConfigResponseSchema,
  GetFileResponseSchema,
  IngestFileResponseSchema,
  ListToolsResponseSchema,
  type WatchSessionResponse,
  WatchSessionResponseSchema,
  type WatchSessionsResponse,
  WatchSessionsResponseSchema,
} from '@abcp-agent/schema'
import { EidDedup } from '../context.js'
import {
  fieldString,
  isRecord,
  toJsonObject,
  toJsonValue,
  toStructFields,
  toStructValue,
  toValue,
  valueToRaw,
} from '../proto-json.js'
import { runProviderTest } from '../provider-test.js'
import { resolveSessionDefaults } from '../session-defaults.js'
import { tenantOf } from '../tenant.js'
import {
  parseProviderModels,
  presetToMsg,
  providerToMsg,
  sessionToMsg,
} from '../views.js'
import { refreshMessageFactFromTip, storeBytes } from './helpers.js'

/**
 * File handlers: upload/ingest/get/meta.
 */

export function filesHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async uploadFile(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const file = req.file
      const data = req.data
      if (data === undefined) throw new Error('data required')
      // The SERVER mints the code (16-hex, deduped by sha): a client-supplied
      // code is ignored so every file code in the system is uniform. This
      // closes the only path that could mint non-canonical codes.
      const bytes = new Uint8Array(Buffer.from(data, 'base64'))
      const record = await storeBytes(
        deps,
        tenant,
        bytes,
        file?.name ?? 'artifact',
        '',
      )
      return { ok: true, code: record.code, mime: record.mime }
    },

    async ingestFile(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { data: raw, name } = req
      if (raw === undefined) throw new Error('data required')
      const bytes = new Uint8Array(raw)
      if (bytes.length === 0) {
        throw new ConnectError('file data is empty', Code.InvalidArgument)
      }
      // Only the NAME is caller-supplied; the mime is DERIVED from the bytes
      // (storeFile → sniffMime). An empty name is still rejected — history
      // renders the name — but a name-less upload is completed from the
      // sniffed extension when possible.
      const cleanName = (name ?? '').trim()
      if (cleanName === '') {
        throw new ConnectError('file name is required', Code.InvalidArgument)
      }
      const record = await storeBytes(deps, tenant, bytes, cleanName, '')
      return create(IngestFileResponseSchema, {
        ok: true,
        code: record.code,
        mime: record.mime,
      })
    },

    async getFile(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const code = req.code
      const row = await fileByCode(deps.bus, tenant, code)
      if (row.isErr()) throw new Error(row.error)
      if (row.value === null) throw new Error('file not found')
      const got = await deps.files.get(tenant, code)
      return create(GetFileResponseSchema, {
        data: new Uint8Array(got.data),
        name: row.value.name ?? '',
        mime: row.value.mime ?? 'application/octet-stream',
      })
    },

    async getFileMeta(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const code = req.code
      const row = await fileByCode(deps.bus, tenant, code)
      if (row.isErr()) throw new Error(row.error)
      if (row.value === null) throw new Error('file not found')
      const r = row.value
      return {
        name: r.name ?? '',
        mime: r.mime ?? '',
        size: r.size ?? 0,
        ...(r.width != null ? { width: r.width } : {}),
        ...(r.height != null ? { height: r.height } : {}),
        ...(r.duration_ms != null ? { durationMs: BigInt(r.duration_ms) } : {}),
        ...(r.thumb_code != null ? { thumbCode: r.thumb_code } : {}),
        ...(r.thumbhash != null ? { thumbhash: r.thumbhash } : {}),
      }
    },

    // Streaming counterpart of getFile: forward-only chunks. A Connect
    // server-streaming handler is an async generator; yielding the chunks in
    // order delivers them progressively without buffering the whole file.
    async *getFileStream(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const code = req.code
      const row = await fileByCode(deps.bus, tenant, code)
      if (row.isErr()) throw new Error(row.error)
      if (row.value === null) throw new Error('file not found')
      const total = row.value.size ?? 0
      let offset = 0
      for await (const chunk of deps.files.getStream(tenant, code)) {
        const last = offset + chunk.length >= total && total > 0
        yield create(FileChunkSchema, {
          data: chunk,
          offset: BigInt(offset),
          total: BigInt(total),
          last,
        })
        offset += chunk.length
      }
      // Empty file: still terminate the stream with a single last chunk.
      if (offset === 0) {
        yield create(FileChunkSchema, {
          data: new Uint8Array(0),
          offset: 0n,
          total: 0n,
          last: true,
        })
      }
    },
  }
}
