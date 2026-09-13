import { createHash } from 'node:crypto'
import type {
  Bus as AbcBus,
  Envelope as AbcEnvelope,
  InboxMsg as AbcInboxMsg,
  Subscription as AbcSubscription,
} from '@abc-protocol/sdk'
import { Agent as AbcAgent, connectNatsBus } from '@abc-protocol/sdk'
import { CH, GLOBAL_TENANT } from '@abc-protocol/sdk'
import { subjectTenant, tenantKVKey, tenantObjectName } from '@abc-protocol/sdk'
import { ResultAsync } from 'neverthrow'

/**
 * Compatibility shim: the legacy `Bus` (native NATS subjects) is the
 * @abc-protocol/sdk NATS transport. Wire subjects use the abc protocol
 * prefix (`abc.`) and are shared verbatim with the extension SDKs.
 */
export type Bus = AbcBus
export type Subscription = AbcSubscription
export type Envelope = AbcEnvelope
export type InboxMsg = AbcInboxMsg

export async function connectBus(
  natsUrl: string,
): Promise<ResultAsync<Bus, string>> {
  return ResultAsync.fromPromise(
    connectNatsBus(natsUrl),
    e => `abc connect: ${String(e)}`,
  )
}

/** The agent-side role over the abc transport. */
export const Agent = AbcAgent

// wire subject helpers (v2 abc protocol subjects: abc.<tenant>.<...>).
export const mailboxSubject = (tenant: string, sid: string) =>
  CH.mailbox(tenant, sid)
export const sseSubject = (tenant: string, sid: string) =>
  CH.sessionEvents(tenant, sid)

// Re-export the v2 tenant helpers so the rest of the agent imports them from
// one place.
export { GLOBAL_TENANT, subjectTenant, tenantKVKey, tenantObjectName }

// stream/bucket names on the abc wire (session events share the mailbox
// stream; the object bucket carries tool payloads)
export const STREAM_MAILBOX = 'ABC_MAILBOX'
// Message-fact projection bucket. Deliberately DIFFERENT from the SDK's
// lease bucket 'abc-session-state' (LEASE_BUCKET in lease.ts): the lease
// needs per-key expiry while facts persist, and both key a session by
// sha256(sid)[:22] — sharing a bucket would let a fact overwrite the run
// lease (claimSession kvs-create fails => the session is never processed).
export const BUCKET_SESSION_STATE = 'abc-session-meta'
// Active-turn marker: session -> run_id of the currently-running turn. Used by
// replay to hand only the LIVE turn back (never finished/revoked ones).
export const BUCKET_SESSION_RUN = 'abc-session-run'
export const BUCKET_TOOL = 'ABC_TOOL'
export const BUCKET_CONFIG = 'abc-agent-config'
export const BUCKET_PRESETS = 'abc-presets'
export const BUCKET_FILES_META = 'abc-files-meta'
export const SESSION_LEASE_MS = 30_000
export const MODELS_DEV_KEY = 'models-dev-catalog.json'

export function natsToken(sid: string): string {
  return createHash('sha256')
    .update(sid, 'utf8')
    .digest('base64url')
    .slice(0, 22)
}
