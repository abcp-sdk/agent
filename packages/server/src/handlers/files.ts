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
  discoverGatewayModels,
  discoverTools,
  factFromPersist,
  fileByCode,
  findVariant,
  fireAndForget,
  GATEWAY_API_TYPE,
  GATEWAY_PROVIDER_ID,
  getModelsDev,
  interruptRun,
  isGatewayApiType,
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
} from '@easylab-agent/agent'
import {
  type AgentService,
  GetAgentConfigResponseSchema,
  GetFileResponseSchema,
  IngestFileResponseSchema,
  ListToolsResponseSchema,
  type WatchSessionResponse,
  WatchSessionResponseSchema,
  type WatchSessionsResponse,
  WatchSessionsResponseSchema,
} from '@easylab-agent/schema'
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
        file?.mime ?? 'application/octet-stream',
        '',
      )
      return { ok: true, code: record.code }
    },

    async ingestFile(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { data: raw, name, mime } = req
      if (raw === undefined) throw new Error('data required')
      const bytes = new Uint8Array(raw)
      if (bytes.length === 0) {
        throw new ConnectError('file data is empty', Code.InvalidArgument)
      }
      // A stored blob MUST carry a usable name + a non-empty mime so history
      // can render it (image thumbnail / audio player). A concrete
      // `application/octet-stream` is allowed (unknown extension); an EMPTY
      // name/mime is not — reject the upload rather than store an
      // unrenderable record.
      const cleanName = (name ?? '').trim()
      const cleanMime = (mime ?? '').trim()
      if (cleanName === '') {
        throw new ConnectError('file name is required', Code.InvalidArgument)
      }
      if (cleanMime === '') {
        throw new ConnectError('file mime is required', Code.InvalidArgument)
      }
      const record = await storeBytes(
        deps,
        tenant,
        bytes,
        cleanName,
        cleanMime,
        '',
      )
      return create(IngestFileResponseSchema, { ok: true, code: record.code })
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
      return {
        name: row.value.name ?? '',
        mime: row.value.mime ?? '',
        size: row.value.size ?? 0,
      }
    },
  }
}
