import {
  type AnySQLiteColumn,
  integer,
  sqliteTable,
  text,
} from 'drizzle-orm/sqlite-core'

export const sessions = sqliteTable('sessions', {
  name: text('name').primaryKey(),
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
})

export const messages = sqliteTable('messages', {
  id: text('id').primaryKey(),
  role: text('role').notNull(),
  prevId: text('prev_id').references((): AnySQLiteColumn => messages.id, {
    onDelete: 'set null',
  }),
  createdAt: text('created_at').notNull(),
})

export const parts = sqliteTable('parts', {
  id: text('id').primaryKey(),
  messageId: text('message_id')
    .notNull()
    .references(() => messages.id, { onDelete: 'cascade' }),
  type: text('type').notNull(),
  seq: integer('seq').notNull().default(0),
  data: text('data').notNull().default('{}'),
})

export const mailbox = sqliteTable('mailbox', {
  id: text('id').primaryKey(),
  sessionName: text('session_name')
    .notNull()
    .references(() => sessions.name, { onDelete: 'cascade' }),
  msgType: text('msg_type').notNull(),
  payload: text('payload').notNull().default('{}'),
  effectiveAt: text('effective_at'),
  status: text('status').notNull().default('pending'),
  createdAt: text('created_at').notNull(),
  consumedAt: text('consumed_at'),
  seq: integer('seq'),
})

export const providers = sqliteTable('providers', {
  providerId: text('provider_id').primaryKey(),
  apiType: text('api_type').notNull().default('openai-compatible'),
  baseUrl: text('base_url').notNull(),
  apiKey: text('api_key').notNull().default(''),
  headers: text('headers').notNull().default('null'),
  models: text('models').notNull().default('[]'),
  createdAt: text('created_at').notNull().default(''),
  updatedAt: text('updated_at').notNull().default(''),
})
