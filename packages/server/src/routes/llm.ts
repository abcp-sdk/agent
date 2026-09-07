import { createRoute, OpenAPIHono } from '@hono/zod-openapi'
import { parseProviderModelRef } from '@easylab-agent/agent'
import { z } from 'zod'
import type { AppEnv } from '../context.js'

const ErrorSchema = z.object({ ok: z.boolean(), error: z.string() })

const contentPartSchema = z.union([
  z.object({ type: z.literal('text'), text: z.string() }),
  z.object({ type: z.literal('image'), image: z.unknown() }),
  z.object({
    type: z.literal('image_url'),
    image_url: z.object({ url: z.string() }),
  }),
])

const messageSchema = z.object({
  role: z.enum(['system', 'user', 'assistant']),
  content: z.string().or(z.array(contentPartSchema)),
})

const chatCompletionsRoute = createRoute({
  method: 'post',
  path: '/llm/chat/completions',
  summary: 'Single-turn chat completion (OpenAI-compatible)',
  description:
    'One-shot completion reusing the agent-provided providers. The `model` field carries the "provider_id/model_id" reference. `messages[].content` accepts an OpenAI-style array with `{type:"image_url",image_url:{url:"<data-url>"}}` (or legacy `{type:"image",image:"<data-url>"}`) parts for VLM calls. Returns OpenAI-compatible chat completion JSON. Exposed so extensions (e.g. memory-extension image-read) share the registered providers without duplicating base_url/api_key.',
  request: {
    body: {
      content: {
        'application/json': {
          schema: z.object({
            model: z.string(),
            messages: z.array(messageSchema).min(1),
            temperature: z.number().optional(),
            max_tokens: z.number().optional(),
          }),
        },
      },
    },
  },
  responses: {
    200: {
      description: 'Completion',
      content: {
        'application/json': {
          schema: z.object({
            id: z.string(),
            object: z.string(),
            model: z.string(),
            choices: z.array(
              z.object({
                index: z.number(),
                message: z.object({
                  role: z.string(),
                  content: z.string().nullable(),
                }),
                finish_reason: z.string().nullable(),
              }),
            ),
            usage: z
              .object({
                prompt_tokens: z.number(),
                completion_tokens: z.number(),
              })
              .optional(),
          }),
        },
      },
    },
    400: {
      description: 'Bad model reference',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    404: {
      description: 'Provider/model not found',
      content: { 'application/json': { schema: ErrorSchema } },
    },
    500: {
      description: 'Error',
      content: { 'application/json': { schema: ErrorSchema } },
    },
  },
})

/**
 * Exposed single-turn LLM endpoint. The agent is the sole owner of provider
 * credentials; extensions call this instead of hardcoding base_url/api_key.
 * The OpenAI-compatible shape lets a VLM extension pass an image part directly.
 */
export const llmRoutes = new OpenAPIHono<AppEnv>().openapi(
  chatCompletionsRoute,
  async c => {
    const { db, llm } = c.get('deps')
    const body = c.req.valid('json')

    const ref = parseProviderModelRef(body.model)
    if (ref === null) {
      return c.json(
        {
          ok: false,
          error: `model must be "provider_id/model_id": ${body.model}`,
        },
        400,
      )
    }

    const resolved = await llm.resolveByProvider(
      db,
      ref.providerId,
      ref.modelId,
    )
    if (resolved.isErr()) {
      return c.json({ ok: false, error: resolved.error }, 404)
    }

    // Map messages to AI-SDK ModelMessages. The tool-enforced contract keeps
    // content to text or image parts; we translate an OpenAI-style image part
    // (data URL) into the SDK's `{type:'image', image}` part.
    const messages: import('ai').ModelMessage[] = []
    for (const m of body.messages) {
      if (typeof m.content === 'string') {
        messages.push({ role: m.role, content: m.content })
        continue
      }
      const content: import('ai').UserContent = []
      for (const part of m.content) {
        if (part.type === 'text')
          content.push({ type: 'text', text: part.text })
        else if (part.type === 'image_url')
          content.push({
            type: 'image',
            image: part.image_url.url,
          })
        else
          content.push({
            type: 'image',
            image: part.image as import('ai').ImagePart['image'],
          })
      }
      messages.push({ role: 'user', content })
    }

    let result: Awaited<ReturnType<typeof import('ai').generateText>>
    try {
      result = await import('ai').then(m =>
        m.generateText({
          model: resolved.value.model,
          messages,
          ...(body.temperature !== undefined
            ? { temperature: body.temperature }
            : {}),
          ...(body.max_tokens !== undefined
            ? { maxOutputTokens: body.max_tokens }
            : {}),
        }),
      )
    } catch (e) {
      return c.json({ ok: false, error: `LLM call failed: ${String(e)}` }, 500)
    }

    return c.json(
      {
        id: `chatcmpl-${crypto.randomUUID()}`,
        object: 'chat.completion',
        model: body.model,
        choices: [
          {
            index: 0,
            message: { role: 'assistant', content: result.text ?? null },
            finish_reason: 'stop',
          },
        ],
        usage: {
          prompt_tokens: result.usage?.inputTokens ?? 0,
          completion_tokens: result.usage?.outputTokens ?? 0,
        },
      },
      200,
    )
  },
)
