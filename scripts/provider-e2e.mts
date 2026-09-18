/**
 * Live provider e2e against a DEPLOYED agent (default: the `temp` namespace
 * standalone agent) using a DEDICATED test tenant.
 *
 * Why a dedicated tenant: provider registration + tool knobs are tenant-scoped.
 * This run REWRITES only its own tenant's providers/knobs, so it can never
 * disturb `default` (the real data) or any other tenant. Every run is
 * idempotent — it clears the tenant's providers first, then re-registers from
 * the CURRENT gateway catalog, so drift (models disappearing/renaming upstream)
 * shows up here instead of silently breaking image/video/speech tools.
 *
 * IMPORTANT: this is an operator/test tool. It reads the gateway catalog's
 * `modelType` to group models into per-modality providers. The gateway's
 * private `capabilities` array is deliberately NEVER read, and the AGENT
 * service never probes the gateway — discovery was removed from the product.
 *
 * Run:
 *   E2E_AGENT_BASE=https://standalone-agent.temp.10.199.64.20.nip.io \
 *   E2E_ADMIN_TOKEN=dev-admin-token \
 *   npx tsx scripts/provider-e2e.mts
 *
 * Env:
 *   E2E_AGENT_BASE        agent base URL (default: the temp standalone agent)
 *   E2E_ADMIN_TOKEN       admin token (default dev-admin-token)
 *   E2E_PROVIDER_TENANT   test tenant id (default e2e-providers)
 *   E2E_GATEWAY_URL       gateway /v4/ai base (default the dev gateway)
 *   E2E_GATEWAY_KEY       gateway api key (default the dev key)
 *   E2E_TLS_INSECURE=0    verify TLS (default: accept the dev CA chain)
 *   E2E_KEEP=1            do not clear the test tenant at the end
 *   E2E_PROBE=0           skip the real TestProvider probes
 */
import { createClient, type Client } from '@connectrpc/connect'
import type { Interceptor } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-node'
import { create } from '@bufbuild/protobuf'
import { ValueSchema } from '@bufbuild/protobuf/wkt'
import {
  AdminService,
  AgentService,
  CreateTenantRequestSchema,
  DeleteProviderRequestSchema,
  IssueTenantTokenRequestSchema,
  ListProvidersRequestSchema,
  ListTenantsRequestSchema,
  ProviderModelSchema,
  ProviderSchema,
  RegisterProviderRequestSchema,
  SetConfigRequestSchema,
  SetExtensionConfigRequestSchema,
  TestProviderRequestSchema,
  UpdateTenantRequestSchema,
} from '@abcp-agent/schema'

const BASE = process.env['E2E_AGENT_BASE'] ?? 'https://standalone-agent.temp.10.199.64.20.nip.io'
const ADMIN_TOKEN = process.env['E2E_ADMIN_TOKEN'] ?? 'dev-admin-token'
const TENANT = process.env['E2E_PROVIDER_TENANT'] ?? 'e2e-providers'
const GW_URL = process.env['E2E_GATEWAY_URL'] ?? 'https://ai-gateway-dev004.develop.10.199.64.20.nip.io/v4/ai'
const GW_KEY = process.env['E2E_GATEWAY_KEY'] ?? 'gw-0c53f638b62edf7275d9443a7282123d3a9a7f7a96ce9ee0'
const INSECURE = process.env['E2E_TLS_INSECURE'] !== '0'
const KEEP = process.env['E2E_KEEP'] === '1'
const PROBE = process.env['E2E_PROBE'] !== '0'

let passed = 0
let failed = 0
const failures: string[] = []
const ok = (name: string, extra = ''): void => {
  passed++
  console.log(`  \u2713 ${name}${extra === '' ? '' : ` — ${extra}`}`)
}
const bad = (name: string, detail?: unknown): void => {
  failed++
  failures.push(name)
  console.log(`  \u2717 ${name}${detail === undefined ? '' : ` — ${String(detail)}`}`)
}
const check = (name: string, cond: boolean, detail?: unknown): void => {
  if (cond) ok(name)
  else bad(name, detail)
}
const section = (t: string): void => console.log(`\n${t}`)

const bearer = (token: string): Interceptor =>
  (next) => (req) => {
    req.header.set('authorization', `Bearer ${token}`)
    return next(req)
  }

function transport(token: string) {
  return createConnectTransport({
    baseUrl: BASE,
    httpVersion: '1.1',
    ...(INSECURE ? { nodeOptions: { rejectUnauthorized: false } as never } : {}),
    interceptors: [bearer(token)],
  })
}

const admin: Client<typeof AdminService> = createClient(AdminService, transport(ADMIN_TOKEN))

/** modelType (catalog) -> agent capability. `language` is the wire's `text`. */
const CAPABILITY_OF: Record<string, string> = {
  language: 'text',
  embedding: 'embedding',
  reranking: 'rerank',
  image: 'image',
  video: 'video',
  speech: 'speech',
  transcription: 'transcription',
  realtime: 'realtime',
}
/** One provider id per modality (semantic grouping: one modality per provider). */
const PROVIDER_OF: Record<string, string> = {
  text: 'gw-text',
  embedding: 'gw-embedding',
  rerank: 'gw-rerank',
  image: 'gw-image',
  video: 'gw-video',
  speech: 'gw-speech',
  transcription: 'gw-transcription',
  realtime: 'gw-realtime',
}
/** Tool knob -> the capability its model reference must match. */
const KNOBS: [string, string][] = [
  ['model.text', 'text'],
  ['model.image', 'image'],
  ['model.image_edit', 'image'],
  ['model.video', 'video'],
  ['model.speech', 'speech'],
  ['model.transcription', 'transcription'],
]
const DEFAULT_TEXT_CONTEXT = 262144

interface CatalogModel {
  id: string
  name: string
  modelType: string
}

/** Fetch the live gateway catalog — `modelType` only (never `capabilities`). */
async function fetchCatalog(): Promise<CatalogModel[]> {
  const url = `${GW_URL.replace(/\/$/, '')}/config`
  const prev = process.env['NODE_TLS_REJECT_UNAUTHORIZED']
  if (INSECURE) process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0'
  try {
    const res = await fetch(url, { headers: { authorization: `Bearer ${GW_KEY}` } })
    if (!res.ok) throw new Error(`gateway catalog ${res.status}: ${await res.text()}`)
    const body = (await res.json()) as { models?: unknown }
    const models = Array.isArray(body.models) ? body.models : []
    return models.map(m => {
      const o = m as Record<string, unknown>
      return {
        id: String(o['id'] ?? ''),
        name: String(o['name'] ?? o['id'] ?? ''),
        modelType: String(o['modelType'] ?? ''),
      }
    })
  } finally {
    if (INSECURE) {
      if (prev === undefined) delete process.env['NODE_TLS_REJECT_UNAUTHORIZED']
      else process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = prev
    }
  }
}

async function ensureTenant(): Promise<string> {
  const list = await admin.listTenants(create(ListTenantsRequestSchema, {}))
  if (!list.tenants.some(t => t.id === TENANT)) {
    const created = await admin.createTenant(
      create(CreateTenantRequestSchema, { id: TENANT, name: 'E2E providers' }),
    )
    ok('test tenant created', TENANT)
    return created.token ?? ''
  }
  // Re-enable a previously disabled tenant (deleteTenant is a soft disable).
  await admin.updateTenant(create(UpdateTenantRequestSchema, { id: TENANT, disabled: false }))
  // Mint a fresh token for this run; label it so prior runs are identifiable.
  const issued = await admin.issueTenantToken(
    create(IssueTenantTokenRequestSchema, { tenantId: TENANT, label: 'e2e-providers-run' }),
  )
  ok('test tenant reused', TENANT)
  return issued.plaintext ?? ''
}

async function main(): Promise<void> {
  console.log(`[providers-e2e] base=${BASE} tenant=${TENANT}`)

  section('gateway catalog (modelType only)')
  const catalog = await fetchCatalog()
  const byCap = new Map<string, CatalogModel[]>()
  for (const m of catalog) {
    const cap = CAPABILITY_OF[m.modelType]
    if (cap === undefined) continue
    const arr = byCap.get(cap) ?? []
    arr.push(m)
    byCap.set(cap, arr)
  }
  for (const [cap, models] of [...byCap.entries()].sort()) {
    console.log(`  ${cap.padEnd(14)} ${models.map(m => m.id).join(', ')}`)
  }
  check('catalog yielded at least text + image', byCap.has('text') && byCap.has('image'))

  section('test tenant')
  const token = await ensureTenant()
  check('test tenant token minted', token !== '')
  const client: Client<typeof AgentService> = createClient(AgentService, transport(token))

  section('rewrite test tenant providers (only this tenant)')
  const existing = await client.listProviders(create(ListProvidersRequestSchema, {}))
  for (const p of existing.providers) {
    await client.deleteProvider(create(DeleteProviderRequestSchema, { providerId: p.providerId }))
  }
  ok('cleared prior providers', `${existing.providers.length} removed`)

  for (const [cap, models] of [...byCap.entries()].sort()) {
    const providerId = PROVIDER_OF[cap]
    await client.registerProvider(
      create(RegisterProviderRequestSchema, {
        provider: create(ProviderSchema, {
          providerId,
          capability: cap,
          apiType: 'vercel-compatible-gateway',
          baseUrl: GW_URL,
          apiKey: GW_KEY,
          models: models.map(m =>
            create(ProviderModelSchema, {
              id: m.id,
              name: m.name,
              contextLimit: cap === 'text' ? BigInt(DEFAULT_TEXT_CONTEXT) : 0n,
            }),
          ),
        }),
      }),
    )
  }
  const after = await client.listProviders(create(ListProvidersRequestSchema, {}))
  check(
    'every modality registered (one provider each)',
    after.providers.length === byCap.size &&
      after.providers.every(p => (p.capability ?? 'text') === (p.models[0]?.modelType ?? 'text')),
    after.providers.map(p => `${p.providerId}:${p.capability}`).join(','),
  )

  section('rewrite test tenant knobs')
  for (const [knob, cap] of KNOBS) {
    const models = byCap.get(cap)
    if (models === undefined || models.length === 0) continue
    const ref = `${PROVIDER_OF[cap]}/${models[0].id}`
    const r = await client.setExtensionConfig(
      create(SetExtensionConfigRequestSchema, {
        extId: 'bundled',
        name: knob,
        value: create(ValueSchema, { kind: { case: 'stringValue', value: ref } }),
      }),
    )
    check(`${knob} -> ${ref}`, r.ok)
  }
  const textModels = byCap.get('text') ?? []
  if (textModels.length > 0) {
    const defaultRef = `${PROVIDER_OF.text}/${textModels[0].id}`
    const r = await client.setConfig(
      create(SetConfigRequestSchema, { key: 'default_model', value: defaultRef }),
    )
    check(`default_model -> ${defaultRef}`, r.ok)
  }

  if (PROBE) {
    section('probe every registered model (real smallest-possible call)')
    for (const p of after.providers) {
      const model = p.models[0]
      if (model === undefined) continue
      const res = await client.testProvider(
        create(TestProviderRequestSchema, {
          providerId: p.providerId,
          apiType: 'vercel-compatible-gateway',
          baseUrl: GW_URL,
          apiKey: GW_KEY,
          model: `${p.providerId}/${model.id}`,
          capability: p.capability ?? 'text',
        }),
      )
      // realtime has no consumer yet and this gateway does not expose the
      // client-secret mint endpoint; a clear "not implemented" is acceptable
      // (reported, not failed) so real drift stands out from that gap.
      if (res.ok) {
        ok(`probe ${p.providerId}/${model.id}`, res.result)
      } else if (
        (p.capability ?? '') === 'realtime' &&
        /not implemented/i.test(res.result)
      ) {
        ok(`probe ${p.providerId}/${model.id} (realtime mint unsupported by gateway)`)
      } else {
        bad(`probe ${p.providerId}/${model.id}`, res.result)
      }
    }
  }

  if (!KEEP) {
    section('cleanup')
    const now = await client.listProviders(create(ListProvidersRequestSchema, {}))
    for (const p of now.providers) {
      await client.deleteProvider(create(DeleteProviderRequestSchema, { providerId: p.providerId }))
    }
    ok('test tenant providers cleared', `${now.providers.length} removed`)
  }

  console.log(`\n[providers-e2e] passed=${passed} failed=${failed}`)
  if (failed > 0) {
    console.log(`[providers-e2e] failures:\n  - ${failures.join('\n  - ')}`)
  }
  process.exit(failed === 0 ? 0 : 1)
}

main().catch(err => {
  console.error(`[providers-e2e] fatal: ${String(err)}`)
  process.exit(1)
})
