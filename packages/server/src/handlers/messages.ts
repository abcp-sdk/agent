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

      const tipRes = await Sessions.tip(deps.db, tenant, id)
      const tipId = tipRes.isErr() ? null : tipRes.value
      const insert = await Messages.insert(deps.db, tenant, 'user', tipId)
      if (insert.isErr()) throw new Error(insert.error)
      let seq = 0
      for (const att of attachments ?? []) {
        if (att.code === '') {
          throw new ConnectError(
            'attachment code is required',
            Code.InvalidArgument,
          )
        }
        // The client sends ONLY the code; name/mime/size are resolved from
        // the stored blob. proto3 gives ''/0 (not undefined) for omitted
        // fields, so treat empty as missing. There is NO silent fallback: a
        // file part must carry a real name, mime and size, otherwise the
        // prompt is refused (a blank metadata file part can't be rendered).
        let name = att.name
        let mime = att.mime
        let size = att.size
        const missing =
          (name ?? '') === '' || (mime ?? '') === '' || (size ?? 0) <= 0
        if (missing) {
          const rec = await fileByCode(deps.bus, tenant, att.code)
          if (rec.isErr()) throw new Error(rec.error)
          if (rec.value === null) {
            throw new ConnectError(
              `attachment not found: file:${att.code}`,
              Code.InvalidArgument,
            )
          }
          name = rec.value.name
          mime = rec.value.mime
          size = rec.value.size
        }
        if ((name ?? '') === '' || (mime ?? '') === '' || (size ?? 0) <= 0) {
          throw new ConnectError(
            `attachment file:${att.code} has incomplete metadata ` +
              `(name=${JSON.stringify(name)} mime=${JSON.stringify(mime)} size=${size})`,
            Code.FailedPrecondition,
          )
        }
        await Parts.insert(deps.db, tenant, insert.value, 'file', seq++, {
          code: att.code,
          name,
          mime,
          size,
        })
      }
      if (prompt !== '') {
        await Parts.insert(deps.db, tenant, insert.value, 'text', seq++, {
          text: prompt,
        })
      }
      await Sessions.setTip(deps.db, tenant, id, insert.value)
      fireAndForget(
        appendSessionId(deps.bus, tenant, id, insert.value),
        'appendSessionIds',
      )
      // Mirror the newest-message fact immediately so the chat-list preview
      // shows the user's message right away (assistant steps overwrite it as
      // the turn progresses).
      const previewText =
        prompt !== ''
          ? prompt
          : attachments.length > 0
            ? `[${attachments.length} attachment(s)]`
            : ''
      projectMessageFact(
        deps.bus,
        tenant,
        id,
        factFromPersist(new Date().toISOString(), 'user', previewText),
      )
      // The route owns the row for this logical message (id = insert.value):
      // it writes the parts/tip/fact above for an immediate preview, then
      // wakes the turn with the SAME id in the payload. The agent's mailbox
      // handlers persist idempotently, so the message is never inserted
      // twice even though both paths "persist".
      await new AbcAgent(deps.bus).publishMailbox(tenant, id, 'user_prompt', {
        message_id: insert.value,
        text: prompt,
        attachments: attachments ?? [],
      })
      yield {
        event: 'accepted',
        params: { message_id: insert.value },
        eid: '',
      }
    },
  }
}
