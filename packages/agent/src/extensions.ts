import {
  Agent as AbcAgent,
  ExtensionManifestSchema,
  type ExtensionVariable,
  ExtensionVariableSchema,
} from '@abc-protocol/sdk'
import { ResultAsync } from 'neverthrow'
import type { Bus } from './bus.js'
import { GLOBAL_TENANT } from './bus.js'
import { parse } from './json.js'

/**
 * Extension-server discovery + template-variable resolution over the bus.
 *
 * There is no HTTP discovery and no static server list: an extension is any
 * bus client that (a) subscribes to `abc.discover` and replies with its
 * `ExtensionManifestSchema` JSON, and (b) optionally answers
 * `abc.var.{id}.{name}` requests (lazy variable fallback).
 */

/** Subject an extension must subscribe to for discovery. */
export const EXTENSION_DISCOVER_SUBJECT = 'abc.discover'

/** A discovered extension with its declared template variables. */
export interface ResolvedExtension {
  id: string
  variables: ExtensionVariable[]
}

const VAR_TOKEN = /\{\{vars\.([^.}]+)\.([^}]+)\}\}/g

/**
 * Discover every reachable prompt-capable extension via a broadcast.
 * Each reply is validated against `ExtensionManifestSchema`; malformed replies
 * are skipped. Returns after `maxWaitMs` has elapsed.
 */
export function discoverExtensions(
  bus: Bus,
  maxWaitMs = 500,
): ResultAsync<ResolvedExtension[], string> {
  return ResultAsync.fromPromise(
    bus.requestMany(
      EXTENSION_DISCOVER_SUBJECT,
      {},
      { maxWaitMs, tenant: GLOBAL_TENANT },
    ),
    e => `discover: ${String(e)}`,
  ).map(replies => {
    const out: ResolvedExtension[] = []
    const seen = new Map<string, boolean>()
    for (const env of replies) {
      const parsed = parse(ExtensionManifestSchema, JSON.stringify(env.payload))
      if (parsed.isErr()) continue
      const m = parsed.value
      if (seen.has(m.id)) continue
      seen.set(m.id, true)
      out.push({
        id: m.id,
        variables: m.prompt?.variables ?? [],
      })
    }
    return out
  })
}

/** Built-in variables the agent resolves itself (no extension required). */
function builtinVariableValue(name: string): string | null {
  switch (name) {
    case 'date':
      return new Date().toISOString().slice(0, 10)
    case 'datetime':
      return new Date().toISOString()
    default:
      return null
  }
}

/**
 * Render a system-prompt template, substituting `{{vars.<provider>.<name>}}`
 * tokens:
 *   - `vars.builtin.*`     → date / datetime (agent-local)
 *   - `vars.agent.*`       → agent-owned variables (KV)
 *   - `vars.<extId>.*`     → extension variables (KV, lazy fallback)
 *
 * `sessionName`, when given, scopes session variables; without it (preview)
 * only global variables resolve and session variables stay literal.
 */
export async function renderTemplate(
  template: string,
  bus: Bus,
  tenant: string,
  sessionName?: string,
): Promise<string> {
  const matches = [...template.matchAll(VAR_TOKEN)]
  if (matches.length === 0) return template

  const agent = new AbcAgent(bus)
  let out = template
  for (const match of matches) {
    const [full, provider, name] = match
    if (full === undefined || provider === undefined || name === undefined) {
      continue
    }

    let value: string | null = null
    if (provider === 'builtin') {
      value = builtinVariableValue(name)
    } else {
      value = await agent.resolveVariable(tenant, provider, name, sessionName)
    }

    if (value !== null) {
      out = out.split(full).join(value)
    }
  }
  return out
}

// Re-export the variable schema so extension authors can build manifests.
export { ExtensionVariableSchema }
