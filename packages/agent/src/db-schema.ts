import {
  type AnySQLiteColumn,
  index,
  integer,
  primaryKey,
  sqliteTable,
  text,
} from 'drizzle-orm/sqlite-core'

/**
 * Multi-tenant schema (v2). Every row is scoped by `tenant` (an opaque,
 * plaintext isolation key). `sessions` uses a COMPOSITE primary key
 * `(tenant, name)` because the same session name may exist under different
 * tenants; `providers` uses `(tenant, provider_id)` for the same reason.
 * `messages`/`parts`/`mailbox` keep their globally-unique UUID primary keys
 * but carry `tenant` so every read can be scoped.
 */
export const sessions = sqliteTable(
  'sessions',
  {
    tenant: text('tenant').notNull().default('default'),
    name: text('name').notNull(),
    // Canonical model reference "provider_id/model_id".
    model: text('model').notNull().default(''),
    // Selected reasoning variant id (empty = provider defaults).
    variant: text('variant').notNull().default(''),
    preset: text('preset').notNull().default(''),
    tipId: text('tip_id'),
    maxTurns: integer('max_turns').notNull().default(0),
    systemPrompt: text('system_prompt').notNull().default(''),
    inputTokens: integer('input_tokens').notNull().default(0),
    outputTokens: integer('output_tokens').notNull().default(0),
    totalTokens: integer('total_tokens').notNull().default(0),
    // Last single request (one LLM step), overwritten not accumulated. The
    // cumulative *_tokens above are reserved for billing/history; the chat
    // footer shows the most recent request's context size.
    lastInputTokens: integer('last_input_tokens').notNull().default(0),
    lastOutputTokens: integer('last_output_tokens').notNull().default(0),
    createdAt: text('created_at').notNull(),
    updatedAt: text('updated_at').notNull(),
    lastUsedAt: text('last_used_at'),
    locale: text('locale').notNull().default(''),
    // Generic grouping key (tenant-scoped, free-form). Empty = ungrouped. A
    // subsession records its parent session's name here, but the field is
    // generic: any client may group sessions arbitrarily.
    group: text('group').notNull().default(''),
  },
  t => ({ pk: primaryKey({ columns: [t.tenant, t.name] }) }),
)

export const messages = sqliteTable('messages', {
  id: text('id').primaryKey(),
  tenant: text('tenant').notNull().default('default'),
  role: text('role').notNull(),
  prevId: text('prev_id').references((): AnySQLiteColumn => messages.id, {
    onDelete: 'set null',
  }),
  createdAt: text('created_at').notNull(),
})

export const parts = sqliteTable('parts', {
  id: text('id').primaryKey(),
  tenant: text('tenant').notNull().default('default'),
  messageId: text('message_id')
    .notNull()
    .references(() => messages.id, { onDelete: 'cascade' }),
  type: text('type').notNull(),
  seq: integer('seq').notNull().default(0),
  data: text('data').notNull().default('{}'),
})

export const mailbox = sqliteTable('mailbox', {
  id: text('id').primaryKey(),
  tenant: text('tenant').notNull().default('default'),
  sessionName: text('session_name').notNull(),
  msgType: text('msg_type').notNull(),
  payload: text('payload').notNull().default('{}'),
  effectiveAt: text('effective_at'),
  status: text('status').notNull().default('pending'),
  createdAt: text('created_at').notNull(),
  consumedAt: text('consumed_at'),
  seq: integer('seq'),
})

export const providers = sqliteTable(
  'providers',
  {
    tenant: text('tenant').notNull().default('default'),
    providerId: text('provider_id').notNull(),
    // Semantic grouping: a provider serves EXACTLY ONE modality. Its models
    // all share this capability (text -> context_limit>0, everything else 0).
    // A host serving several modalities registers one provider per modality.
    capability: text('capability').notNull().default('text'),
    apiType: text('api_type').notNull().default('openai-compatible'),
    baseUrl: text('base_url').notNull(),
    apiKey: text('api_key').notNull().default(''),
    headers: text('headers').notNull().default('null'),
    models: text('models').notNull().default('[]'),
    createdAt: text('created_at').notNull().default(''),
    updatedAt: text('updated_at').notNull().default(''),
  },
  t => ({ pk: primaryKey({ columns: [t.tenant, t.providerId] }) }),
)

/**
 * Tenants (isolation domains). `id` is the plaintext isolation key used on the
 * wire (`abc.<id>.<...>`) and throughout the schema.
 */
export const tenants = sqliteTable('tenants', {
  id: text('id').primaryKey(),
  name: text('name').notNull().default(''),
  disabled: integer('disabled').notNull().default(0),
  createdAt: text('created_at').notNull(),
  updatedAt: text('updated_at').notNull(),
})

/**
 * Bearer tokens minted for tenants. Only the sha256 of the plaintext token is
 * stored; the plaintext is returned once at issue/rotate time. Revocation is a
 * soft flag so tokens stay listable/auditable.
 */
export const tenantTokens = sqliteTable(
  'tenant_tokens',
  {
    tokenId: text('token_id').primaryKey(),
    tenantId: text('tenant_id').notNull(),
    tokenSha256: text('token_sha256').notNull().unique(),
    label: text('label').notNull().default(''),
    createdAt: text('created_at').notNull(),
    lastUsedAt: text('last_used_at'),
    revokedAt: text('revoked_at'),
  },
  t => ({ tenantIdx: index('idx_tenant_tokens_tenant').on(t.tenantId) }),
)
