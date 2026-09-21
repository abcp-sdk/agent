import { extname } from 'node:path'
import { fileTypeFromBuffer } from 'file-type'

/**
 * Content-type resolution for stored files.
 *
 * The agent is the trust boundary for a file's type: callers (webui,
 * extensions) no longer send a mime at all. A type is DERIVED here, once,
 * from the bytes:
 *
 *   1. magic-byte sniff (`file-type`) — authoritative when it recognizes the
 *      content (images, video/audio containers, PDF, ZIP/OOXML, archives…);
 *   2. an ffprobe refinement for media that the sniff could not name (some
 *      codecs/containers), performed by the media probe — see media.ts;
 *   3. a small extension table as the last resort (text formats — md / json /
 *      csv / code — have no magic bytes), and finally a binary default.
 *
 * The name's extension is only ever a HINT: it can promote a generic text
 * detection to a specific one (`.json` -> application/json) but never
 * overrides a positive magic-byte result.
 */

/** Extension -> mime for text formats that carry no magic bytes. */
const TEXT_EXT_MIME: Record<string, string> = {
  md: 'text/markdown',
  markdown: 'text/markdown',
  json: 'application/json',
  jsonc: 'application/json',
  csv: 'text/csv',
  tsv: 'text/tab-separated-values',
  txt: 'text/plain',
  log: 'text/plain',
  yaml: 'application/x-yaml',
  yml: 'application/x-yaml',
  xml: 'application/xml',
  toml: 'application/toml',
  ini: 'text/plain',
  conf: 'text/plain',
  env: 'text/plain',
  html: 'text/html',
  htm: 'text/html',
  css: 'text/css',
  js: 'text/javascript',
  mjs: 'text/javascript',
  cjs: 'text/javascript',
  ts: 'text/typescript',
  tsx: 'text/typescript',
  jsx: 'text/javascript',
  py: 'text/x-python',
  go: 'text/x-go',
  rs: 'text/x-rust',
  java: 'text/x-java',
  kt: 'text/x-kotlin',
  swift: 'text/x-swift',
  c: 'text/x-c',
  h: 'text/x-c',
  cpp: 'text/x-c++',
  cc: 'text/x-c++',
  hpp: 'text/x-c++',
  cs: 'text/x-csharp',
  rb: 'text/x-ruby',
  php: 'text/x-php',
  sh: 'text/x-shellscript',
  bash: 'text/x-shellscript',
  zsh: 'text/x-shellscript',
  sql: 'text/x-sql',
  lua: 'text/x-lua',
  pl: 'text/x-perl',
  r: 'text/x-r',
  dart: 'text/x-dart',
  scala: 'text/x-scala',
  clj: 'text/x-clojure',
  ex: 'text/x-elixir',
  exs: 'text/x-elixir',
  erl: 'text/x-erlang',
  hs: 'text/x-haskell',
  ml: 'text/x-ocaml',
  vue: 'text/plain',
  svelte: 'text/plain',
  proto: 'text/x-protobuf',
  gradle: 'text/plain',
  tf: 'text/plain',
  dockerfile: 'text/plain',
  makefile: 'text/plain',
}

const BINARY_DEFAULT = 'application/octet-stream'

/** Sniff the content type from the leading bytes, refined by the name.
 *
 *  Returns the derived mime (never empty): a magic-byte hit wins; otherwise a
 *  text-extension hint; otherwise `application/octet-stream`. */
export async function sniffMime(
  data: Uint8Array,
  name = '',
): Promise<{ mime: string; ext: string }> {
  try {
    const hit = await fileTypeFromBuffer(data)
    if (hit !== undefined) return { mime: hit.mime, ext: hit.ext }
  } catch {
    // A sniffer failure must never block an ingest; fall through.
  }
  const ext = extname(name).replace(/^\./, '').toLowerCase()
  const hint = TEXT_EXT_MIME[ext]
  if (hint !== undefined) return { mime: hint, ext }
  return { mime: BINARY_DEFAULT, ext }
}

/** True when a mime is one the media probe knows how to decode. */
export function isMediaMime(mime: string): boolean {
  return (
    mime.startsWith('image/') ||
    mime.startsWith('video/') ||
    mime.startsWith('audio/')
  )
}

/** File extension for a mime (used to complete a name that lacks one). */
export function extForMime(mime: string): string {
  const m = mime.toLowerCase()
  const table: Record<string, string> = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/webp': 'webp',
    'image/gif': 'gif',
    'image/avif': 'avif',
    'image/bmp': 'bmp',
    'image/tiff': 'tiff',
    'image/svg+xml': 'svg',
    'video/mp4': 'mp4',
    'video/x-matroska': 'mkv',
    'video/webm': 'webm',
    'video/quicktime': 'mov',
    'audio/mpeg': 'mp3',
    'audio/wav': 'wav',
    'audio/x-wav': 'wav',
    'audio/ogg': 'ogg',
    'audio/webm': 'webm',
    'audio/mp4': 'm4a',
    'audio/flac': 'flac',
    'audio/aac': 'aac',
    'application/pdf': 'pdf',
    'application/json': 'json',
    'application/zip': 'zip',
    'application/x-tar': 'tar',
    'application/gzip': 'gz',
    'text/plain': 'txt',
    'text/markdown': 'md',
    'text/csv': 'csv',
    'text/html': 'html',
  }
  return table[m] ?? ''
}
