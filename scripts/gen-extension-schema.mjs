import { writeFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

/**
 * Emit the extension-server protocol schemas as JSON Schema so non-TS
 * extension implementations (Go/Rust/Python) can generate their types from a
 * single source of truth. The TS side imports the zod schemas directly; this
 * file is the interop bridge.
 *
 * Run as part of `npm run build -w @easylab-agent/schema` (cwd is the schema
 * package dir, so `dist/index.js` is relative to process.cwd()).
 */

const outDir = resolve(process.cwd(), 'dist')

// Imported lazily so this only runs against the built ESM output.
const m = await import(join(outDir, 'index.js'))

const schemas = {
  ExtensionToolSchema: m.ExtensionToolSchema,
  ExtensionVariableSchema: m.ExtensionVariableSchema,
  ExtensionManifestSchema: m.ExtensionManifestSchema,
  ExtensionVariableValueSchema: m.ExtensionVariableValueSchema,
}

const definitions = {
  $schema: 'https://json-schema.org/draft/2020-12/schema',
}
for (const [name, schema] of Object.entries(schemas)) {
  definitions[name] = schema.toJSONSchema()
}

const out = join(outDir, 'extension-schema.json')
writeFileSync(out, JSON.stringify(definitions, null, 2) + '\n')
console.log(`[schema] wrote ${out}`)
