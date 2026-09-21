import {
  buildGenerativeModel,
  buildModelForApiType,
  type ModelCapability,
  type ProviderCredentials,
} from '@abcp-agent/agent'
import {
  embedMany,
  experimental_generateVideo,
  generateImage,
  generateSpeech,
  generateText,
  rerank,
  transcribe,
} from 'ai'

/**
 * Provider-model test probes. The test runs a REAL smallest-possible
 * generation per capability to prove the endpoint + credentials work — it
 * never returns or stores the artifact (result is a short text summary).
 */

/** Fixed test parameters (not user-configurable). */
export const TEST_IMAGE_SIZE = '1024x1024'
export const TEST_SPEECH_TEXT = 'hi'
export const TEST_VIDEO_SECONDS = 4
/** Video generation is slow; allow up to 30 minutes (unary call). */
export const TEST_VIDEO_TIMEOUT_MS = 1_800_000
/** ASR test sample: a 1s 16kHz mono PCM WAV (quiet tone). */
export const TEST_ASR_SAMPLE_SECONDS = 1

/**
 * Minimal shape of a realtime model/factory. `buildGenerativeModel` returns the
 * realtime MODEL (gateway + OpenAI protocols); some SDK versions also expose a
 * factory-level `getToken`. Either way the server-side proof is minting a
 * short-lived client secret via `doCreateClientSecret` / `getToken`. Typed
 * structurally so the probe does not couple to the SDK's experimental aliases.
 */
interface RealtimeSecretMinter {
  getToken?(options: {
    model: string
  }): Promise<{ token: string; url: string; expiresAt?: number }>
  doCreateClientSecret?(options?: {
    expiresAfterSeconds?: number
  }): Promise<{ token: string; url: string; expiresAt?: number }>
}

/**
 * Synthesize a minimal 16kHz mono 16-bit PCM WAV containing a quiet tone. An
 * ASR endpoint needs audio input; a real upload proves multipart handling and
 * a transcript response.
 */
export function asrSampleWav(sampleRate = 16000, seconds = 1): Uint8Array {
  const n = Math.floor(sampleRate * seconds)
  const data = new Uint8Array(n * 2)
  const view = new DataView(data.buffer)
  for (let i = 0; i < n; i++) {
    const s = Math.round(3000 * Math.sin((2 * Math.PI * 220 * i) / sampleRate))
    view.setInt16(i * 2, s, true)
  }
  const header = new Uint8Array(44)
  const hv = new DataView(header.buffer)
  const writeAscii = (off: number, s: string) => {
    for (let i = 0; i < s.length; i++) header[off + i] = s.charCodeAt(i)
  }
  writeAscii(0, 'RIFF')
  hv.setUint32(4, 36 + data.length, true)
  writeAscii(8, 'WAVE')
  writeAscii(12, 'fmt ')
  hv.setUint32(16, 16, true)
  hv.setUint16(20, 1, true) // PCM
  hv.setUint16(22, 1, true) // mono
  hv.setUint32(24, sampleRate, true)
  hv.setUint32(28, sampleRate * 2, true) // byte rate
  hv.setUint16(32, 2, true) // block align
  hv.setUint16(34, 16, true) // bits per sample
  writeAscii(36, 'data')
  hv.setUint32(40, data.length, true)
  const out = new Uint8Array(header.length + data.length)
  out.set(header, 0)
  out.set(data, header.length)
  return out
}

export interface ProviderTestInput {
  apiType: string
  baseUrl: string
  apiKey: string
  /** Trailing model id (provider is passed separately for catalog lookup). */
  modelId: string
  capability: ModelCapability
  providerId: string
  variant: string
}

export interface ProviderTestResult {
  ok: boolean
  result: string
}

function creds(input: ProviderTestInput): ProviderCredentials {
  return {
    providerId: input.providerId,
    apiType: input.apiType,
    baseUrl: input.baseUrl,
    apiKey: input.apiKey,
    headers: {},
  }
}

/**
 * Run one smallest-possible generation for the model's capability and return a
 * short human-readable summary. Generation models are resolved with
 * [buildGenerativeModel]; text uses the language-model factory.
 */
export async function runProviderTest(
  input: ProviderTestInput,
  /** Variant providerOptions for the text path (models.dev-derived). */
  textProviderOptions?: Record<string, unknown>,
): Promise<ProviderTestResult> {
  const c = creds(input)
  switch (input.capability) {
    case 'text': {
      const built = buildModelForApiType(c, input.modelId)
      if (built.isErr()) return { ok: false, result: built.error }
      const gen = await generateText({
        model: built.value,
        prompt: 'hi',
        maxOutputTokens: 8,
        ...(textProviderOptions !== undefined
          ? { providerOptions: textProviderOptions as never }
          : {}),
      }).catch(e => {
        throw new Error(`text generation failed: ${String(e)}`)
      })
      return { ok: true, result: gen.text }
    }
    case 'image': {
      const built = buildGenerativeModel(c, input.modelId, 'image')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await generateImage({
        // biome-ignore lint/suspicious/noExplicitAny: the AI SDK's generate* option bags are not fully typed across providers
        model: built.value as any,
        prompt: 'a small red square on a white background',
        n: 1,
        size: TEST_IMAGE_SIZE as `${number}x${number}`,
      })
      const bytes = res.images[0]?.uint8Array?.length ?? 0
      return {
        ok: true,
        result: `image ok (${TEST_IMAGE_SIZE}, ${bytes} bytes)`,
      }
    }
    case 'speech': {
      const built = buildGenerativeModel(c, input.modelId, 'speech')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await generateSpeech({
        // biome-ignore lint/suspicious/noExplicitAny: the AI SDK's generate* option bags are not fully typed across providers
        model: built.value as any,
        text: TEST_SPEECH_TEXT,
      })
      const bytes = res.audio.uint8Array.length
      return { ok: true, result: `speech ok (${bytes} bytes)` }
    }
    case 'transcription': {
      const built = buildGenerativeModel(c, input.modelId, 'transcription')
      if (built.isErr()) return { ok: false, result: built.error }
      // A synthetic tone often yields an empty transcript (nothing spoken);
      // the AI SDK throws AI_NoTranscriptGeneratedError in that case, so treat
      // it as a PASS with an explicit note rather than a provider failure.
      const res = await transcribe({
        // biome-ignore lint/suspicious/noExplicitAny: the AI SDK's generate* option bags are not fully typed across providers
        model: built.value as any,
        audio: asrSampleWav(16000, TEST_ASR_SAMPLE_SECONDS),
      }).catch((e: unknown) => {
        if (String(e).includes('NoTranscriptGenerated')) {
          return {
            text: '',
            segments: [],
            language: undefined,
            durationInSeconds: undefined,
            warnings: [],
            providerMetadata: undefined,
            response: {},
          }
        }
        throw e
      })
      return {
        ok: true,
        result:
          res.text.trim() === ''
            ? 'transcription ok (empty transcript — synthetic tone carries no speech)'
            : `transcription ok: ${res.text.trim().slice(0, 80)}`,
      }
    }
    case 'video': {
      const built = buildGenerativeModel(c, input.modelId, 'video')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await experimental_generateVideo({
        // biome-ignore lint/suspicious/noExplicitAny: the AI SDK's generate* option bags are not fully typed across providers
        model: built.value as any,
        prompt: 'a cat sitting still',
        duration: TEST_VIDEO_SECONDS,
        // Async start/status flow (POST /video-model/start + /status): the
        // gateway's native contract and the AI SDK's default when `poll` is
        // given. The abort timeout bounds worst-case latency.
        poll: { intervalMs: 2000, timeoutMs: TEST_VIDEO_TIMEOUT_MS },
        abortSignal: AbortSignal.timeout(TEST_VIDEO_TIMEOUT_MS),
      })
      const bytes = res.videos[0]?.uint8Array?.length ?? 0
      return {
        ok: true,
        result: `video ok (${TEST_VIDEO_SECONDS}s, ${bytes} bytes)`,
      }
    }
    case 'embedding': {
      const built = buildGenerativeModel(c, input.modelId, 'embedding')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await embedMany({
        // biome-ignore lint/suspicious/noExplicitAny: the AI SDK's generate* option bags are not fully typed across providers
        model: built.value as any,
        values: ['hello', 'world'],
      })
      const dim = res.embeddings[0]?.length ?? 0
      return {
        ok: dim > 0,
        result:
          dim > 0
            ? `embedding ok (${res.embeddings.length} vectors, ${dim} dims)`
            : 'embedding returned no vectors',
      }
    }
    case 'rerank': {
      const built = buildGenerativeModel(c, input.modelId, 'rerank')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await rerank({
        // biome-ignore lint/suspicious/noExplicitAny: the AI SDK's generate* option bags are not fully typed across providers
        model: built.value as any,
        query: 'cat',
        documents: ['a dog barks', 'a cat sleeps'],
        topN: 2,
      })
      return {
        ok: res.ranking.length > 0,
        result:
          res.ranking.length > 0
            ? `rerank ok (top: doc #${res.ranking[0]?.originalIndex})`
            : 'rerank returned no ranking',
      }
    }
    case 'realtime': {
      const built = buildGenerativeModel(c, input.modelId, 'realtime')
      if (built.isErr()) return { ok: false, result: built.error }
      // Realtime is WebSocket-based: there is no one-shot generation to run.
      // The server-side proof is minting a short-lived client secret (the
      // browser then opens the socket with it). Some gateways do not expose
      // the mint endpoint yet, so an UNSUPPORTED error is reported as a
      // distinct "not implemented" note rather than a credential failure.
      const minter = built.value as RealtimeSecretMinter
      try {
        const secret =
          typeof minter.getToken === 'function'
            ? await minter.getToken({ model: input.modelId })
            : await (minter.doCreateClientSecret?.({}) ??
                Promise.reject(new Error('realtime mint unsupported')))
        if (secret.token === '') {
          return { ok: false, result: 'realtime mint returned no token' }
        }
        return {
          ok: true,
          result: `realtime ok (token ${secret.token.slice(0, 6)}…, ws url ${secret.url})`,
        }
      } catch (e) {
        const msg = String(e)
        // A gateway that does not implement the mint endpoint answers 404 and
        // the SDK surfaces it as `GatewayResponseError: Invalid error response
        // format` (the 404 body is not a gateway error envelope). Treat both
        // the explicit "unsupported" wording and that wrapper as NOT
        // IMPLEMENTED, so it is not mistaken for a bad credential.
        if (
          /404|not implemented|unsupported|GatewayResponseError|Invalid error response format/i.test(
            msg,
          )
        ) {
          return {
            ok: false,
            result: `realtime endpoint not implemented by this provider: ${msg.slice(0, 200)}`,
          }
        }
        throw e
      }
    }
    default:
      return { ok: false, result: `unknown capability '${input.capability}'` }
  }
}
