import {
  buildGenerativeModel,
  buildModelForApiType,
  type ModelCapability,
  type ProviderCredentials,
} from '@easylab-agent/agent'
import { generateImage, generateSpeech, generateText, experimental_generateVideo, transcribe } from 'ai'

/**
 * Provider-model test probes. The test runs a REAL smallest-possible
 * generation per capability to prove the endpoint + credentials work — it
 * never returns or stores the artifact (result is a short text summary).
 */

/** Fixed test parameters (not user-configurable). */
export const TEST_IMAGE_SIZE = '256x256'
export const TEST_SPEECH_TEXT = 'hi'
export const TEST_VIDEO_SECONDS = 4
/** Video generation is slow; allow up to 10 minutes (unary call). */
export const TEST_VIDEO_TIMEOUT_MS = 600_000
/** ASR test sample: a 1s 16kHz mono PCM WAV (quiet tone). */
export const TEST_ASR_SAMPLE_SECONDS = 1

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
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        model: built.value as any,
        prompt: 'a small red square on a white background',
        n: 1,
        size: TEST_IMAGE_SIZE as `${number}x${number}`,
      })
      const bytes = res.images[0]?.uint8Array?.length ?? 0
      return { ok: true, result: `image ok (${TEST_IMAGE_SIZE}, ${bytes} bytes)` }
    }
    case 'speech': {
      const built = buildGenerativeModel(c, input.modelId, 'speech')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await generateSpeech({
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        model: built.value as any,
        text: TEST_SPEECH_TEXT,
      })
      const bytes = res.audio.uint8Array.length
      return { ok: true, result: `speech ok (${bytes} bytes)` }
    }
    case 'transcription': {
      const built = buildGenerativeModel(c, input.modelId, 'transcription')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await transcribe({
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        model: built.value as any,
        audio: asrSampleWav(16000, TEST_ASR_SAMPLE_SECONDS),
      })
      return {
        ok: true,
        result: res.text.trim() === ''
          ? 'transcription ok (empty transcript)'
          : `transcription ok: ${res.text.trim().slice(0, 80)}`,
      }
    }
    case 'video': {
      // Only google/Veo has an AI-SDK video model. Others (openai /
      // openai-compatible) are not supported by the SDK.
      const built = buildGenerativeModel(c, input.modelId, 'video')
      if (built.isErr()) return { ok: false, result: built.error }
      const res = await experimental_generateVideo({
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        model: built.value as any,
        prompt: 'a cat sitting still',
        duration: TEST_VIDEO_SECONDS,
        abortSignal: AbortSignal.timeout(TEST_VIDEO_TIMEOUT_MS),
      })
      const bytes = res.videos[0]?.uint8Array?.length ?? 0
      return { ok: true, result: `video ok (${TEST_VIDEO_SECONDS}s, ${bytes} bytes)` }
    }
    default:
      return { ok: false, result: `unknown capability '${input.capability}'` }
  }
}
