import { describe, expect, it } from 'vitest'
import { asrSampleWav, TEST_IMAGE_SIZE } from '../src/provider-test.js'

describe('provider-test params', () => {
  it('fixed image size', () => {
    expect(TEST_IMAGE_SIZE).toBe('256x256')
  })
})

describe('asrSampleWav', () => {
  it('builds a valid RIFF/WAVE PCM header', () => {
    const wav = asrSampleWav(16000, 1)
    const dv = new DataView(wav.buffer, wav.byteOffset, wav.byteLength)
    const ascii = (o: number, n: number) =>
      String.fromCharCode(...wav.slice(o, o + n))
    expect(ascii(0, 4)).toBe('RIFF')
    expect(ascii(8, 4)).toBe('WAVE')
    expect(ascii(12, 4)).toBe('fmt ')
    expect(ascii(36, 4)).toBe('data')
    // mono, 16kHz, 16-bit
    expect(dv.getUint16(22, true)).toBe(1)
    expect(dv.getUint32(24, true)).toBe(16000)
    expect(dv.getUint16(34, true)).toBe(16)
    // 1 second of samples → 32000 data bytes
    expect(dv.getUint32(40, true)).toBe(32000)
    expect(wav.length).toBe(44 + 32000)
  })
})
