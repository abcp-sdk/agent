import { build } from 'esbuild'
import { execFileSync } from 'node:child_process'
import {
  chmodSync,
  copyFileSync,
  mkdirSync,
  readdirSync,
  writeFileSync,
} from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')

/**
 * Bundle the server into a single ESM file, then wrap it in a Node SEA
 * (single executable application) so the whole backend ships as one binary
 * with the SPA assets embedded as SEA assets.
 */

const bundleOut = join(root, '.sea', 'server.cjs')
const blob = join(root, '.sea', 'sea-prep.blob')
const bin = join(root, '.sea', 'easylab-agent')
const seaConfig = join(root, '.sea', 'sea-config.json')

mkdirSync(dirname(bundleOut), { recursive: true })

await build({
  entryPoints: [join('packages', 'server', 'src', 'index.ts')],
  bundle: true,
  platform: 'node',
  format: 'cjs',
  target: 'node26',
  outfile: bundleOut,
  // Bundle all dependencies into the single CJS file so the SEA binary is
  // fully self-contained (node:sea's require only loads built-ins + bundle).
  packages: undefined,
  outExtension: { '.js': '.cjs' },
  loader: {
    '.txt': 'text',
    '.html': 'text',
    '.css': 'text',
    '.json': 'text',
    '.svg': 'text',
    '.png': 'binary',
    '.woff': 'binary',
    '.woff2': 'binary',
  },
  logLevel: 'info',
})

// Embed the SPA dist as a SEA asset. SEA assets are keyed by path relative to
// the `assets` map in sea-config.json.
const seaAssets = {}
const uiDist = join(root, 'packages', 'ui', 'dist')
try {
  const walk = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const full = join(dir, entry.name)
      if (entry.isDirectory()) {
        walk(full)
      } else {
        // SEA asset keys can't start with '/' and shouldn't contain '..'
        const key = full.replace(uiDist, '').replace(/^[/\\]+/, '').replace(/\\/g, '/')
        seaAssets[key] = full
      }
    }
  }
  walk(uiDist)
} catch {
  // No UI dist yet; the server will fall back gracefully.
}

writeFileSync(
  seaConfig,
  JSON.stringify({ main: bundleOut, output: blob, assets: seaAssets }, null, 2),
)

execFileSync(process.execPath, ['--experimental-sea-config', seaConfig], {
  stdio: 'inherit',
})

// Copy the current node binary and inject the SEA blob.
const nodeBinary = process.execPath
copyFileSync(nodeBinary, bin)
chmodSync(bin, 0o755)

const sentinel = 'NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2'
execFileSync(
  'npx',
  ['postject', bin, 'NODE_SEA_BLOB', blob, '--sentinel-fuse', sentinel],
  { stdio: 'inherit' },
)

console.log(`\nBuilt single executable: ${bin}`)
