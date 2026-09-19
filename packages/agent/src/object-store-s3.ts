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
import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import type { ObjectStore } from '@abc-protocol/sdk'

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
