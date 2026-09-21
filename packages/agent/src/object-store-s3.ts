/**
 * S3-compatible `ObjectStore` implementation (AWS S3, MinIO, Ceph, OSS, …).
 *
 * Selected when `AGENT_BLOB_BACKEND=s3`: durable file bytes live in S3 and
 * never touch NATS. The SDK is imported lazily so NATS-only deployments never
 * load it.
 *
 * The bus only delegates the DURABLE methods (`objectPutPersistent` /
 * `objectGetPersistent`) to this store; transient objects (tool payloads,
 * catalog caches, coordination KV) keep flowing through NATS. The transient
 * methods below just satisfy the interface and are never used by the bus.
 */

import type { ObjectStore } from '@abc-protocol/sdk'
import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3'

export interface S3Config {
  bucket: string
  region: string
  /** Custom endpoint (MinIO/OSS/...); omit for AWS S3. */
  endpoint?: string | undefined
  accessKeyId?: string | undefined
  secretAccessKey?: string | undefined
  forcePathStyle: boolean
  /** Key prefix. Empty -> objects live at the bucket root. */
  prefix: string
}

/**
 * S3 stores BOTH classes; the deployment picks this backend precisely to keep
 * objects out of NATS, so keeping transient objects on NATS would reintroduce
 * the dependency. Transient vs durable is a prefix, not a different store.
 */
export class S3ObjectStore implements ObjectStore {
  private readonly client: S3Client
  private readonly bucket: string
  private readonly prefix: string

  constructor(cfg: S3Config) {
    this.bucket = cfg.bucket
    this.prefix = cfg.prefix.replace(/\/+$/, '')
    this.client = new S3Client({
      region: cfg.region,
      ...(cfg.endpoint !== undefined
        ? { endpoint: cfg.endpoint, forcePathStyle: cfg.forcePathStyle }
        : {}),
      ...(cfg.accessKeyId !== undefined && cfg.secretAccessKey !== undefined
        ? {
            credentials: {
              accessKeyId: cfg.accessKeyId,
              secretAccessKey: cfg.secretAccessKey,
            },
          }
        : {}),
    })
  }

  private key(name: string): string {
    return this.prefix === '' ? name : `${this.prefix}/${name}`
  }

  async objectPut(name: string, data: Uint8Array): Promise<void> {
    await this.objectPutPersistent(name, data)
  }

  async objectPutPersistent(name: string, data: Uint8Array): Promise<void> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: this.key(name),
        Body: data,
      }),
    )
  }

  async objectGet(name: string): Promise<Uint8Array | null> {
    return this.objectGetPersistent(name)
  }

  /**
   * Stream a durable object's bytes in order, without buffering the whole
   * body. S3's `GetObject` returns an async iterable body, so a large media
   * file is delivered chunk by chunk to the agent's `GetFileStream` RPC.
   * Returns `null` when the key is absent.
   */
  async objectGetStream(
    name: string,
  ): Promise<AsyncIterable<Uint8Array> | null> {
    try {
      const out = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: this.key(name) }),
      )
      const body = out.Body as
        | { transformToWebStream?: () => ReadableStream<Uint8Array> }
        | undefined
      if (body === undefined) return null
      if (typeof body.transformToWebStream === 'function') {
        const reader = body.transformToWebStream().getReader()
        return {
          [Symbol.asyncIterator]() {
            return {
              async next() {
                const { done, value } = await reader.read()
                return done
                  ? { done: true, value: undefined }
                  : { done: false, value }
              },
              async return() {
                await reader.cancel().catch(() => {})
                return { done: true, value: undefined }
              },
            }
          },
        }
      }
      // Fallback: no web-stream bridge on this SDK build — buffer.
      const bytes = new Uint8Array(await out.Body!.transformToByteArray())
      return (async function* () {
        yield bytes
      })()
    } catch (e) {
      const name_ = (e as { name?: string }).name
      const status = (e as { $metadata?: { httpStatusCode?: number } })
        .$metadata?.httpStatusCode
      if (name_ === 'NoSuchKey' || name_ === 'NotFound' || status === 404) {
        return null
      }
      throw e
    }
  }

  async objectGetPersistent(name: string): Promise<Uint8Array | null> {
    try {
      const out = await this.client.send(
        new GetObjectCommand({ Bucket: this.bucket, Key: this.key(name) }),
      )
      if (out.Body === undefined) return null
      return new Uint8Array(await out.Body.transformToByteArray())
    } catch (e) {
      // A missing key is a normal "null"; only rethrow real failures.
      const name_ = (e as { name?: string }).name
      const status = (e as { $metadata?: { httpStatusCode?: number } })
        .$metadata?.httpStatusCode
      if (name_ === 'NoSuchKey' || name_ === 'NotFound' || status === 404) {
        return null
      }
      throw e
    }
  }
}
