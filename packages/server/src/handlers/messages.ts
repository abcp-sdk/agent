import { randomUUID } from 'node:crypto'
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
  fileByCode,
  findVariant,
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
 * Message handlers: listMessages (chain hydration) and prompt (persist + wake).
 */

export function messagesHandlers(
  deps: AgentDeps,
): Partial<ServiceImpl<typeof AgentService>> {
  return {
    async listMessages(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { id, limit: l, before, after } = req
      const limit = l && l > 0 ? l : 50
      const tipRes = await Sessions.tip(deps.db, tenant, id)
      const tipId = tipRes.isErr() ? null : tipRes.value
      const tip = tipId ?? ''
      // proto3 string defaults to "" (not null): treat empty as "no cursor"
      // so the chain walks from the tip instead of looking up prev_id of "".
      const beforeId = before !== undefined && before !== '' ? before : null
      const afterId = after !== undefined && after !== '' ? after : null

      // Hydrate a list of chain messages into the proto shape.
      const toMsgs = async (chain: ChainMessage[]) => {
        const msgIds = chain.map(m => m.id)
        const partsRes = await Parts.listByMessages(deps.db, tenant, msgIds)
        const partsByMsg = new Map<
          string,
          Array<{
            id: string
            message_id: string
            type: string
            seq: number
            data: string
          }>
        >()
        if (partsRes.isOk()) {
          for (const p of partsRes.value) {
            const list = partsByMsg.get(p.message_id) ?? []
            list.push(p)
            partsByMsg.set(p.message_id, list)
          }
        }
        return chain.map(m => ({
          id: m.id,
          role: m.role,
          prevId: m.prev_id ?? '',
          createdAt: m.created_at ?? '',
          parts: (partsByMsg.get(m.id) ?? []).map(p => ({
            id: p.id,
            messageId: p.message_id,
            type: p.type,
            seq: p.seq,
            data: p.data,
          })),
        }))
      }

      // Incremental mode: anchored on a client-known pin. The delta is the
      // segment appended since that anchor; if the anchor is gone (undo /
      // re-pointed chain) we return `resync` so the client drops its cache.
      if (afterId !== null) {
        const r = await Messages.deltaSince(
          deps.db,
          tenant,
          tipId,
          afterId,
          limit,
        )
        if (r.isErr()) throw new Error(r.error)
        const resync = !r.value.anchorReached && r.value.reachedRoot
        let messages: Awaited<ReturnType<typeof toMsgs>> = []
        if (!resync) messages = await toMsgs(r.value.messages)
        return { ok: true, messages, resync, tipId: tip }
      }

      const r = await Messages.chain(deps.db, tenant, tipId, limit, beforeId)
      if (r.isErr()) throw new Error(r.error)
      const messages = await toMsgs(r.value)
      return { ok: true, messages, resync: false, tipId: tip }
    },

    async *prompt(req, ctx: HandlerContext) {
      const tenant = tenantOf(ctx)
      const { id, prompt, attachments } = req
      const session = await Sessions.get(deps.db, tenant, id)
      if (session.isErr()) throw new Error(session.error)
      if (session.value === null) throw new Error('session not found')

      // Resolve + validate attachments here (so a bad ref fails the RPC with
      // the right ConnectError code), then hand the resolved metadata to the
      // mailbox consumer, which owns the actual chain write. The client sends
      // ONLY the code; name / mime / size come from the stored record whose
      // mime the agent DERIVED at ingest. A file part must carry real metadata,
      // otherwise the prompt is refused (a blank file part cannot be rendered).
      const resolved: Array<{
        code: string
        name: string
        mime: string
        size: number
      }> = []
      for (const att of attachments ?? []) {
        if (att.code === '') {
          throw new ConnectError(
            'attachment code is required',
            Code.InvalidArgument,
          )
        }
        const rec = await fileByCode(deps.bus, tenant, att.code)
        if (rec.isErr()) throw new Error(rec.error)
        if (rec.value === null) {
          throw new ConnectError(
            `attachment not found: file:${att.code}`,
            Code.InvalidArgument,
          )
        }
        const name = rec.value.name
        const mime = rec.value.mime
        const size = rec.value.size
        if ((name ?? '') === '' || (mime ?? '') === '' || (size ?? 0) <= 0) {
          throw new ConnectError(
            `attachment file:${att.code} has incomplete metadata ` +
              `(name=${JSON.stringify(name)} mime=${JSON.stringify(mime)} size=${size})`,
            Code.FailedPrecondition,
          )
        }
        resolved.push({ code: att.code, name, mime, size })
      }

      // The MAILBOX is the single writer of the prompt chain: the route mints
      // the logical message id and publishes it, and the agent persists the
      // row/parts/tip under the session's run lease. This is what serializes
      // concurrent prompts (and prompt-vs-turn appends) — the old double-write
      // left `tip -> insert -> setTip` here exposed to interleaving.
      const messageId = randomUUID()
      await new AbcAgent(deps.bus).publishMailbox(tenant, id, 'user_prompt', {
        message_id: messageId,
        text: prompt,
        attachments: resolved,
      })
      yield {
        event: 'accepted',
        params: { message_id: messageId },
        eid: '',
      }
    },
  }
}
