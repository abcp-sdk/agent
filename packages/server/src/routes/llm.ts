import { parseProviderModelRef } from '@easylab-agent/agent'
import { z } from 'zod'
import { type Router } from '../http.js'

const contentPartSchema = z.union([
  z.object({ type: z.literal('text'), text: z.string() }),
  z.object({
    type: z.literal('image'),
    image: z.custom<import('ai').ImagePart['image']>(),
  }),
  z.object({
    type: z.literal('image_url'),
    image_url: z.object({ url: z.string() }),
  }),
])

const messageSchema = z.object({
  role: z.enum(['system', 'user', 'assistant']),
  content: z.string().or(z.array(contentPartSchema)),
})

const ChatCompletionsBodySchema = z.object({
  model: z.string(),
  messages: z.array(messageSchema).min(1),
  temperature: z.number().optional(),
  max_tokens: z.number().optional(),
})

/**
 * Exposed single-turn LLM endpoint. The agent is the sole owner of provider
 * credentials; extensions call this instead of hardcoding base_url/api_key.
 * The OpenAI-compatible shape lets a VLM extension pass an image part directly.
 */
export function llmRoutes(r: Router): void {
  r.post(
    '/llm/chat/completions',
    async c => {
      const { db, llm } = c.deps
      const body = c.body

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
          else content.push({ type: 'image', image: part.image })
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
        return c.json(
          { ok: false, error: `LLM call failed: ${String(e)}` },
          500,
        )
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
    ChatCompletionsBodySchema,
  )
}
