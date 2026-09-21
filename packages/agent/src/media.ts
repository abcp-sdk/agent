import { spawn } from 'node:child_process'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { rgbaToThumbHash } from 'thumbhash'
import type { Bus } from './bus.js'
import {
  type BlobStore,
  type FileRecord,
  randomCode,
  sha256Hex,
  updateFileMedia,
  upsertFile,
} from './files.js'
import { logger } from './logger.js'
import { isMediaMime } from './mime.js'

/**
 * Server-side media probe: derive width / height / duration / thumbnail /
 * thumbhash for an uploaded image / video / audio file.
 *
 * WHY ON THE AGENT: clients render media cards before (and instead of)
 * downloading the bytes, so the SERVER must know a media file's dimensions
 * and carry a tiny preview. Deriving that in every client would duplicate a
 * decoder + a thumbnailer per platform; doing it once at ingest keeps the
 * metadata uniform and lets a client render a stable, jump-free placeholder.
 *
 * HOW: standalone `ffprobe` / `ffmpeg` binaries invoked via child_process. The
 * agent ships as a Node SEA single executable, which CANNOT load native
 * `.node` addons (sharp & friends are unusable), but spawning a separate
 * binary works. One toolchain covers image + video + audio.
 *
 * NOT INLINE-BLOCKING: probing runs asynchronously AFTER the bytes are
 * durably stored, under a bounded-concurrency queue, so an upload's response
 * is never held hostage to a decode. A missing ffmpeg, a malformed file, or a
 * timeout simply leaves the optional fields unset (the client falls back).
 */

/** Max pixels we will ask ffmpeg to decode on the probe path. Guards against
 *  decompression bombs / a claimed-100MP image ballooning memory. */
const MAX_DECODE_PIXELS = 40_000_000
/** Thumbnail longest-edge (px); the stored thumb is a small WebP. */
const THUMB_EDGE = 512
/** Thumbhash input must fit 100x100 (library constraint). */
const HASH_EDGE = 100
/** Kill a probe that runs longer than this (seconds → ms). */
const PROBE_TIMEOUT_MS = 30_000
/** Bounded concurrency: at most this many ffmpeg/ffprobe jobs at once. */
const MAX_CONCURRENT = 2

const IMAGE_MIME = /^image\//
const VIDEO_MIME = /^video\//
const AUDIO_MIME = /^audio\//

let ffmpegMissingLogged = false

/** Run a binary to completion, capturing stdout as a Buffer (stderr dropped).
 *  Rejects on spawn error, non-zero exit, or timeout. */
function run(
  bin: string,
  args: string[],
): Promise<{ stdout: Buffer; stderr: string }> {
  return new Promise((resolve, reject) => {
    let child: ReturnType<typeof spawn>
    try {
      child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] })
    } catch (e) {
      reject(e)
      return
    }
    const out: Buffer[] = []
    const err: Buffer[] = []
    let done = false
    const timer = setTimeout(() => {
      if (done) return
      done = true
      child.kill('SIGKILL')
      reject(new Error(`${bin} timed out after ${PROBE_TIMEOUT_MS}ms`))
    }, PROBE_TIMEOUT_MS)
    child.stdout?.on('data', (c: Buffer) => out.push(c))
    child.stderr?.on('data', (c: Buffer) => err.push(c))
    child.on('error', e => {
      if (done) return
      done = true
      clearTimeout(timer)
      reject(e)
    })
    child.on('close', code => {
      if (done) return
      done = true
      clearTimeout(timer)
      if (code === 0) {
        resolve({
          stdout: Buffer.concat(out),
          stderr: Buffer.concat(err).toString('utf8'),
        })
      } else {
        reject(
          new Error(
            `${bin} exited ${code}: ${Buffer.concat(err).toString('utf8').slice(0, 400)}`,
          ),
        )
      }
    })
  })
}

/** True when a probe binary exists and answers. Cached briefly. */
let ffmpegAvailable: boolean | null = null
async function haveFfmpeg(): Promise<boolean> {
  if (ffmpegAvailable !== null) return ffmpegAvailable
  try {
    await run('ffmpeg', ['-version'])
    await run('ffprobe', ['-version'])
    ffmpegAvailable = true
  } catch {
    ffmpegAvailable = false
    if (!ffmpegMissingLogged) {
      ffmpegMissingLogged = true
      logger.warn(
        'ffmpeg/ffprobe not found: image/video/audio metadata + thumbnails disabled',
      )
    }
  }
  return ffmpegAvailable
}

interface ProbeInfo {
  width: number | null
  height: number | null
  durationMs: number | null
  /** ffprobe container name (e.g. "mov,mp4,m4a,3gp,3g2,mj2", "png_pipe"). */
  formatName: string
  /** Stream codec kinds present (video/audio/image/subtitle/…). */
  streamTypes: string[]
}

/** Map an ffprobe container/format name to a canonical mime. Used only to
 *  REFINE an inconclusive magic-byte result (application/octet-stream). */
function mimeFromProbe(info: ProbeInfo): string | null {
  const f = info.formatName.toLowerCase()
  const has = (t: string) => info.streamTypes.includes(t)
  const table: Array<[RegExp, string]> = [
    [/png/, 'image/png'],
    [/jpe?g/, 'image/jpeg'],
    [/webp/, 'image/webp'],
    [/gif/, 'image/gif'],
    [/bmp/, 'image/bmp'],
    [/tiff/, 'image/tiff'],
    [/avif|heic|heif/, 'image/avif'],
    [
      /mp4|mov|m4a|3gp|mj2|isom/,
      has('audio') && !has('video') ? 'audio/mp4' : 'video/mp4',
    ],
    [
      /matroska|webm/,
      has('audio') && !has('video') ? 'audio/webm' : 'video/webm',
    ],
    [/mpeg|mp3/, 'audio/mpeg'],
    [/flac/, 'audio/flac'],
    [/ogg/, 'audio/ogg'],
    [/wav|wave/, 'audio/wav'],
    [/aac/, 'audio/aac'],
  ]
  for (const [re, mime] of table) if (re.test(f)) return mime
  // No container hint: fall back to the stream kind.
  if (has('video')) return 'video/mp4'
  if (has('image')) return 'image/png'
  if (has('audio')) return 'audio/mpeg'
  return null
}

/** ffprobe the first video/image stream + the container duration. */
async function probeInfo(path: string): Promise<ProbeInfo> {
  const { stdout } = await run('ffprobe', [
    '-v',
    'error',
    '-print_format',
    'json',
    '-show_format',
    '-show_streams',
    path,
  ])
  const json = JSON.parse(stdout.toString('utf8')) as {
    streams?: Array<{
      codec_type?: string
      width?: number
      height?: number
      duration?: string
    }>
    format?: { duration?: string; format_name?: string }
  }
  const visual = json.streams?.find(
    s => s.codec_type === 'video' || s.codec_type === 'image',
  )
  const width = visual?.width && visual.width > 0 ? visual.width : null
  const height = visual?.height && visual.height > 0 ? visual.height : null
  const durStr =
    json.format?.duration ??
    json.streams?.find(s => s.duration !== undefined)?.duration
  const dur = durStr !== undefined ? Number(durStr) : NaN
  const durationMs =
    Number.isFinite(dur) && dur > 0 ? Math.round(dur * 1000) : null
  return {
    width,
    height,
    durationMs,
    formatName: json.format?.format_name ?? '',
    streamTypes: (json.streams ?? [])
      .map(s => s.codec_type ?? '')
      .filter(t => t !== ''),
  }
}

/** Generate a small WebP thumbnail (longest edge THUMB_EDGE). */
async function makeThumb(path: string, outPath: string): Promise<void> {
  await run('ffmpeg', [
    '-y',
    '-v',
    'error',
    '-i',
    path,
    '-frames:v',
    '1',
    '-vf',
    `scale=${THUMB_EDGE}:${THUMB_EDGE}:force_original_aspect_ratio=decrease`,
    '-f',
    'webp',
    outPath,
  ])
}

/** Render a tiny (≤100px) raw-RGBA frame for thumbhash encoding. */
async function makeHashRgba(
  path: string,
  srcW: number,
  srcH: number,
): Promise<{ w: number; h: number; rgba: Buffer } | null> {
  const scale = Math.min(HASH_EDGE / srcW, HASH_EDGE / srcH, 1)
  const w = Math.max(1, Math.round(srcW * scale))
  const h = Math.max(1, Math.round(srcH * scale))
  const { stdout } = await run('ffmpeg', [
    '-v',
    'error',
    '-i',
    path,
    '-frames:v',
    '1',
    '-vf',
    `scale=${w}:${h}`,
    '-f',
    'rawvideo',
    '-pix_fmt',
    'rgba',
    '-',
  ])
  if (stdout.length < w * h * 4) return null
  return { w, h, rgba: stdout.subarray(0, w * h * 4) }
}

function thumbName(name: string): string {
  const base = name.replace(/\.[^./\\]+$/, '')
  return `${base || 'file'}.thumb.webp`
}

interface MediaDeps {
  bus: Bus
  files: BlobStore
}

/**
 * Probe one file and persist the derived media metadata. Never throws: any
 * failure is logged and leaves the optional fields unset. The thumbnail is
 * itself stored as a canonical content-addressed file (its own code), so the
 * client fetches it through the normal GetFile path.
 *
 * Besides facts, the probe may REFINE the stored mime: a magic-byte sniff that
 * could not name the content (`application/octet-stream`) is re-typed from
 * ffprobe's container/codec identification, then the media facts follow.
 */
async function probeAndStore(
  deps: MediaDeps,
  tenant: string,
  record: FileRecord,
): Promise<void> {
  if (!(await haveFfmpeg())) return

  const got = await deps.files.get(tenant, record.code)
  const bytes = got.data
  if (bytes.length === 0) return

  const dir = await mkdtemp(join(tmpdir(), 'abcp-media-'))
  const inPath = join(dir, 'input')
  try {
    await writeFile(inPath, bytes)

    let info: ProbeInfo
    try {
      info = await probeInfo(inPath)
    } catch (e) {
      logger.warn(
        { code: record.code, err: String(e) },
        'media probe (ffprobe) failed',
      )
      return
    }

    const patch: Partial<
      Pick<
        FileRecord,
        'width' | 'height' | 'duration_ms' | 'thumb_code' | 'thumbhash' | 'mime'
      >
    > = {}

    // Refine an inconclusive type from the probe's identification. Never
    // override a positive magic-byte/sniff result.
    let mime = record.mime
    if (mime === 'application/octet-stream' || mime === '') {
      const refined = mimeFromProbe(info)
      if (refined !== null) {
        mime = refined
        patch.mime = refined
      }
    }

    const isImage = IMAGE_MIME.test(mime)
    const isVideo = VIDEO_MIME.test(mime)
    const isAudio = AUDIO_MIME.test(mime)
    if (!isImage && !isVideo && !isAudio) {
      // Not media after refinement: persist only the refined type, if any.
      if (patch.mime !== undefined) {
        await updateFileMedia(deps.bus, tenant, record.code, patch)
      }
      return
    }

    if (info.width !== null && info.height !== null) {
      // Skip thumbnailing an implausibly large frame (decode guard). We still
      // record the dimensions ffprobe reported without decoding pixels.
      const pixels = info.width * info.height
      if (pixels > MAX_DECODE_PIXELS) {
        logger.warn(
          { code: record.code, width: info.width, height: info.height },
          'media probe: frame exceeds decode guard; metadata only',
        )
        patch.width = info.width
        patch.height = info.height
        if (info.durationMs !== null) patch.duration_ms = info.durationMs
        await updateFileMedia(deps.bus, tenant, record.code, patch)
        return
      }
      patch.width = info.width
      patch.height = info.height
    }
    if (info.durationMs !== null) patch.duration_ms = info.durationMs

    // Visual thumbnail + thumbhash (image / video only; no audio poster).
    if ((isImage || isVideo) && info.width !== null && info.height !== null) {
      const thumbPath = join(dir, 'thumb.webp')
      try {
        await makeThumb(inPath, thumbPath)
        const thumb = await readFile(thumbPath)
        if (thumb.length > 0) {
          const thumbRecord: FileRecord = {
            code: randomCode(),
            sha256: sha256Hex(thumb),
            name: thumbName(record.name),
            mime: 'image/webp',
            size: thumb.length,
            uploader_session: record.uploader_session,
            created_at: new Date().toISOString(),
          }
          await deps.files.put(tenant, thumbRecord.code, thumbRecord, thumb)
          const stored = await upsertFile(deps.bus, tenant, thumbRecord)
          patch.thumb_code = stored.isOk()
            ? stored.value.code
            : thumbRecord.code
        }
      } catch (e) {
        logger.warn(
          { code: record.code, err: String(e) },
          'media probe: thumbnail generation failed',
        )
      }
      try {
        const rgba = await makeHashRgba(inPath, info.width, info.height)
        if (rgba !== null) {
          const hash = rgbaToThumbHash(rgba.w, rgba.h, rgba.rgba)
          patch.thumbhash = Buffer.from(hash).toString('base64')
        }
      } catch (e) {
        logger.warn(
          { code: record.code, err: String(e) },
          'media probe: thumbhash generation failed',
        )
      }
    }

    if (Object.keys(patch).length > 0) {
      await updateFileMedia(deps.bus, tenant, record.code, patch)
    }
  } finally {
    await rm(dir, { recursive: true, force: true }).catch(() => {})
  }
}

// ---- bounded-concurrency queue -------------------------------------------

let active = 0
const queue: Array<() => Promise<void>> = []

function pump(): void {
  while (active < MAX_CONCURRENT && queue.length > 0) {
    const job = queue.shift()
    if (job === undefined) break
    active++
    void job().finally(() => {
      active--
      pump()
    })
  }
}

/**
 * Enqueue an asynchronous media probe for a just-stored file. Non-blocking:
 * returns immediately. `record` MUST be the canonical stored record (the one
 * whose code holds the bytes).
 *
 * A file is probed when its mime is media OR is still inconclusive
 * (`application/octet-stream`/empty), since the probe can also REFINE the
 * mime from ffprobe's container identification. A file whose sniffed type is
 * a definite non-media (pdf, text, zip…) is skipped.
 */
export function scheduleMediaProbe(
  deps: MediaDeps,
  tenant: string,
  record: FileRecord,
): void {
  const indefinite =
    record.mime === '' || record.mime === 'application/octet-stream'
  if (!indefinite && !isMediaMime(record.mime)) return
  queue.push(() => probeAndStore(deps, tenant, record))
  pump()
}
