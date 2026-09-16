# easylab-agent (standalone agent)

Multi-tenant, single-binary agent service implementing `agent.v1.AgentService`
(Connect RPC over HTTP/1+h2c): chat sessions with streaming turns, tool
calling, presets, providers (text + multimodal via a Vercel-compatible AI
gateway), files, undo/fork, and cross-session subsessions — all state on
NATS/JetStream + SQLite or Postgres, no other runtime dependency.

```
┌────────────┐  Connect RPC   ┌────────────────────────────────────┐
│  clients   │ ─────────────► │  easylab-agent (Node SEA binary)   │
│ (Flutter / │  Bearer token  │  ┌────────┐ ┌────────┐ ┌────────┐  │     NATS
│  web / SDK)│                │  │ server │ │ agent  │ │ schema │  │ ──────────► JetStream
└────────────┘                │  └────────┘ └────────┘ └────────┘  │   streams/KV/object store
                              │   bundled-extension (in-process)   │
                              └────────────────────────────────────┘
```

## Packages

```
agent/
├── packages/
│   ├── schema/     # proto-derived messages + shared zod schemas
│   ├── agent/      # core: turn loop (streamText), mailbox + session lease,
│   │               # chain messages/parts, providers, presets, i18n,
│   │               # compaction, interrupt, models.dev catalog
│   └── server/     # Connect RPC surface (AgentService + AdminService),
│                   # auth (tenant bearer tokens), in-process bundled
│                   # extension, provider test probes
├── scripts/        # build (esbuild → SEA) + e2e drivers
└── build-image.sh  # buildkit → forgejo OCI image
```

## Develop

```bash
npm install
npm run dev          # tsx watch packages/server (needs NATS_URL)
npm run build        # schema → agent → server → .sea/easylab-agent
npm run check        # biome lint/format
npm test             # vitest (agent + server + bundled suites)
npm run e2e          # boots the REAL binary: embedded NATS + sqlite + mock LLM
```

E2E drivers (all against the real binary, no shared state):

- `scripts/e2e.mts` — every RPC: CRUD, streaming turns, tools, undo/fork,
  watch streams, providers/presets/config/files, interrupt, compact, admin.
- `scripts/subsession-test.mts` — subsession create → mail-send → parent resume.
- `scripts/subsession-i18n-test.mts` — handoff localization (zh/en).
- `scripts/item-test.mts` — mailbox item semantics (idempotency, ordering).

Environment: `E2E_AGENT_BIN` (default `.sea/easylab-agent`),
`ABC_NATS_SERVER_BIN` (nats-server binary; defaults to PATH lookup),
`ABC_NATS_URL` (reuse an external NATS), `E2E_KEEP=1`, `E2E_DEBUG=1`.

## Configuration

| Env | Meaning |
| --- | --- |
| `PORT` / `HTTP_PROTOCOL` | listen port; `auto` (h1+h2c) / `h1` / `h2c` |
| `DB_BACKEND` / `DATABASE_URL` | `sqlite` (default) or `pg` |
| `NATS_URL` | JetStream bus (required) |
| `AGENT_AUTH_MODE` | `required` (default) or `none` (dev) |
| `AGENT_ADMIN_TOKEN` | static admin bearer (AdminService) |
| `AGENT_BOOTSTRAP_TENANT` / `AGENT_BOOTSTRAP_TOKEN` | first-boot tenant + token |
| `AGENT_DEFAULT_TENANT` | tenant when auth=none / v2 migration target |
| `DISABLED_TOOLS` | host hard-denylist, `<extId>.<name>` comma-separated |
| `AGENT_CORS_ORIGIN` | browser CORS origin |
| `LOG_LEVEL` | pino level |

Tenants are managed via `agent.v1.AdminService` (tokens stored as sha256 only);
provider API keys are stored server-side and returned **masked**.

## Behavior notes

- Sessions own a `prev_id` message chain; undo/fork move the tip. Subsessions
  are O(1) forks sharing the parent chain (`sessions.group` = parent name).
- Cross-replica mutual exclusion via a NATS KV session lease (claim/renew);
  mailbox delivery is a durable consumer; user prompts are idempotent by
  message id.
- The models.dev catalog refreshes every 30 minutes (offline snapshot
  fallback bundled) and feeds reasoning-variant metadata.
- Multi-tenant: every subject/KV key/object is tenant-scoped (`t.<tenant>…`);
  per-session abort controllers are tenant-scoped in-memory.

## Deploy

`build-image.sh` builds the SEA binary in buildkit and pushes to the internal
registry; the standalone deployment manifest lives at
`easylab/k8s/standalone-agent.yaml` (agent + embedded NATS sidecar, hostPath
sqlite).
