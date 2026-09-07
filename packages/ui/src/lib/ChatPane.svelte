<script lang="ts">
  import { onMount } from 'svelte'
  import { api, type Session } from '$lib/api'
  import { SseParamsSchema } from '@easylab-agent/schema'
  import { markdown } from '$lib/markdown'
  import { Send, Square, GitFork, Mailbox, FilePenLine, ChevronDown, Undo2, Layers, PenLine } from '@lucide/svelte'
  import MailboxPanel from '$lib/MailboxPanel.svelte'
  import ToolCallLog from '$lib/ToolCallLog.svelte'
  import { Button } from '$lib/components/ui/button'
  import { Input } from '$lib/components/ui/input'
  import { Textarea } from '$lib/components/ui/textarea'
  import * as Dialog from '$lib/components/ui/dialog'

  let { active, onrefresh }: { active: Session; onrefresh: () => void } = $props()

  type Msg = { id: string; role: string; content: string }
  type ToolLog = { toolCallId: string; toolName: string; state: 'running' | 'done'; log: string }
  let messages: Msg[] = $state([])
  let toolLogs: ToolLog[] = $state([])
  let input = $state('')
  let streaming = $state(false)
  let assistantBuf = $state('')
  let error = $state('')
  let showMailbox = $state(false)
  let showRename = $state(false)
  let renameInput = $state('')
  let models: string[] = $state([])
  let presets: { id: string }[] = $state([])
  let showModelPicker = $state(false)
  let showPresetPicker = $state(false)
  let attachInput: HTMLInputElement | null = $state(null)
  let pends: { code: string; name: string; mime: string; size: number }[] = $state([])
  let uploading = $state(false)

  let streamAbort: AbortController | null = null
  let streamIter: AsyncIterator<unknown> | null = null

  const FILE_REF = /\[附件\s+([^|\]]+)\s*\|\s*file:([0-9a-zA-Z]+)\s*\|\s*([^|\]]*)\s*\|\s*([^\]]*)\]/g

  function compact() {
    void api.compact(active.name).match(
      () => loadHistory(),
      e => {
        error = e
      },
    )
  }

  function loadHistory() {
    void api.listMessages(active.name).match(
      rows => {
        messages = rows
          .filter(m => m.role !== 'system')
          .map(m => ({ id: m.id, role: m.role, content: m.content }))
      },
      e => {
        error = e
      },
    )
  }
  function loadModels() {
    void api.listModels().match(
      m => {
        models = m
      },
      () => {},
    )
  }

  function loadPresets() {
    void api.listPresets().match(
      p => {
        presets = p
      },
      () => {},
    )
  }

  function startStream() {
    stopStream()
    void (async () => {
      streamAbort = new AbortController()
      try {
        const stream = await api.watchSession(active.name, { signal: streamAbort.signal })
        streamIter = stream[Symbol.asyncIterator]()
        for (;;) {
          const next = await streamIter.next()
          if (next.done) break
          handleEvent(next.value as { event: string; params?: Record<string, unknown> })
        }
      } catch {
        stopStream()
      }
    })()
  }

  function handleEvent(data: { event: string; params?: Record<string, unknown> }) {
    {
      const p = data.params ?? {}
      if (data.event === 'text-delta') {
        const text = SseParamsSchema.safeParse(p)
        if (text.success && 'text' in text.data) assistantBuf += text.data.text
      } else if (data.event === 'tool-call') {
        const c = SseParamsSchema.safeParse(p)
        if (c.success && 'toolName' in c.data) {
          toolLogs = [
            ...toolLogs,
            { toolCallId: c.data.toolCallId, toolName: c.data.toolName, state: 'running', log: '' },
          ]
        }
      } else if (data.event === 'tool-result') {
        const r = SseParamsSchema.safeParse(p)
        if (r.success && 'toolName' in r.data) {
          toolLogs = toolLogs.map(t =>
            t.toolCallId === r.data.toolCallId ? { ...t, state: 'done' } : t,
          )
        }
      } else if (data.event === 'turn-complete') {
        flushAssistant()
        streaming = false
        onrefresh()
        stopStream()
      } else if (data.event === 'compacted') {
        flushAssistant()
        loadHistory()
      } else if (data.event === 'error') {
        const m = SseParamsSchema.safeParse(p)
        error = m.success && 'message' in m.data ? m.data.message : 'turn error'
        streaming = false
        stopStream()
      }
    }
  }

  function stopStream() {
    streamAbort?.abort()
    streamAbort = null
    streamIter = null
  }

  function flushAssistant() {
    if (assistantBuf.trim() !== '') {
      messages = [
        ...messages,
        { id: `local-${Date.now()}`, role: 'assistant', content: assistantBuf },
      ]
      assistantBuf = ''
    }
  }

  function send() {
    const text = input.trim()
    const att = pends
    const full = att.length > 0
      ? `${att.map(a => `[附件 ${a.name || a.code} | file:${a.code} | ${a.mime} | ${a.size ?? '?'}]`).join('\n')}\n`
      : ''
    const message = full + text
    if ((text === '' && att.length === 0) || streaming) return
    messages = [...messages, { id: `local-${Date.now()}`, role: 'user', content: message }]
    toolLogs = []
    input = ''
    pends = []
    assistantBuf = ''
    streaming = true
    error = ''
    void api.prompt(active.name, message).match(
      () => startStream(),
      e => {
        error = e
        streaming = false
      },
    )
  }

  async function pickFiles() {
    if (attachInput === null) return
    attachInput.click()
  }

  async function onFiles(e: Event) {
    const target = e.target as HTMLInputElement
    const files = Array.from(target.files ?? [])
    target.value = ''
    if (files.length === 0) return
    uploading = true
    for (const f of files) {
      try {
        const up = await api.uploadFile(f, active.name)
        pends = [...pends, { code: up.code, name: up.name, mime: up.mime, size: up.size }]
      } catch (err) {
        error = String(err)
      }
    }
    uploading = false
  }

  function interrupt() {
    stopStream()
    streaming = false
    flushAssistant()
    void api.interrupt(active.name).match(
      () => loadHistory(),
      e => {
        error = e
      },
    )
  }

  function undo(messageId?: string) {
    void api.undo(active.name, messageId).match(
      r => {
        if (r.undone) loadHistory()
      },
      e => {
        error = e
      },
    )
  }

  function switchModel(modelId: string) {
    void api.setSessionModel(active.name, modelId).match(
      () => {
        showModelPicker = false
        onrefresh()
      },
      e => {
        error = e
      },
    )
  }

  function switchPreset(presetId: string) {
    void api.updateSettings(active.name, { preset: presetId }).match(
      () => {
        showPresetPicker = false
        onrefresh()
      },
      e => {
        error = e
      },
    )
  }

  function fork() {
    void api.fork(active.name, `${active.name}-fork`).match(
      () => {
        onrefresh()
      },
      e => {
        error = e
      },
    )
  }

  function rename() {
    const newName = renameInput.trim()
    if (newName === '') return
    void api.rename(active.name, newName).match(
      () => {
        renameInput = ''
        showRename = false
        onrefresh()
      },
      e => {
        error = e
      },
    )
  }

  function onKey(e: KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      send()
    }
  }

  onMount(() => {
    loadHistory()
    loadModels()
    loadPresets()
    return () => stopStream()
  })

  function scrollToBottom(el: HTMLElement) {
    el.scrollTop = el.scrollHeight
  }

  function renderFileLinks(content: string) {
    return content.replace(
      FILE_REF,
      (_m, name, code, mime, size) =>
        `[📎 ${name || code} (${mime ?? ''})](${api.fileUrl(code)})`,
    )
  }
</script>

<div class="h-full flex flex-col">
  <header class="shrink-0 px-4 py-2 border-b border-border flex items-center gap-2 text-sm">
    <span class="font-medium">{active.name}</span>
    <div class="flex-1"></div>
    <Button
      variant="ghost"
      size="icon"
      aria-label="Mailbox"
      title="Mailbox"
      onclick={() => (showMailbox = true)}
    >
      <Mailbox class="size-4" />
    </Button>
    <Button
      variant="ghost"
      size="icon"
      aria-label="Fork"
      title="Fork"
      onclick={fork}
    >
      <GitFork class="size-4" />
    </Button>
    <Button
      variant="ghost"
      size="icon"
      aria-label="Rename"
      title="Rename"
      onclick={() => {
        renameInput = active.name
        showRename = true
      }}
    >
      <FilePenLine class="size-4" />
    </Button>
    <Button
      variant="ghost"
      size="icon"
      aria-label="Compact history"
      title="Compact history"
      onclick={compact}
    >
      <Layers class="size-4" />
    </Button>
  </header>

  <div class="flex-1 overflow-y-auto px-4 py-4 flex flex-col gap-4" use:scrollToBottom={messages.length + (assistantBuf.length > 0 ? 1 : 0)}>
    {#each messages as m (m.id)}
      {#if m.role === 'compaction'}
        <div class="flex items-center gap-3 text-xs text-muted-foreground">
          <div class="h-px flex-1 bg-border"></div>
          <Layers class="size-3.5" />
          <span>历史已压缩</span>
          <div class="h-px flex-1 bg-border"></div>
        </div>
        <details class="rounded-lg border bg-muted/40 px-3 py-2 text-xs">
          <summary class="cursor-pointer select-none text-muted-foreground">
            查看压缩摘要
          </summary>
          <pre class="mt-2 whitespace-pre-wrap font-sans text-foreground">{m.content}</pre>
        </details>
      {:else}
        <div class="flex flex-col {m.role === 'user' ? 'items-end' : 'items-start'}">
          <div
            class="max-w-[80%] rounded-lg px-3 py-2 text-sm whitespace-pre-wrap border {m.role === 'user' ? 'bg-primary/10 border-border' : 'bg-muted border'}"
          >
            <!-- eslint-disable-next-line svelte/no-at-html-tags -->
            {#if m.role === 'assistant'}
              {@html markdown(renderFileLinks(m.content))}
            {:else}
              <span class="whitespace-pre-wrap">{renderFileLinks(m.content)}</span>
            {/if}
          </div>
          <Button
            variant="ghost"
            class="h-auto w-auto p-1 mt-0.5 text-muted-foreground hover:text-foreground"
            aria-label="Undo"
            title="Undo"
            onclick={() => undo(m.id)}
          >
            <Undo2 class="size-3.5" />
          </Button>
        </div>
      {/if}
    {/each}

    {#if assistantBuf !== ''}
      <div class="flex justify-start">
        <div class="max-w-[80%] rounded-lg px-3 py-2 text-sm whitespace-pre-wrap bg-muted">
          {@html markdown(assistantBuf)}
        </div>
      </div>
    {/if}

    {#each toolLogs as t (t.toolCallId)}
      <div class="flex justify-start">
        <div class="max-w-[80%] w-full">
          <ToolCallLog toolName={t.toolName} state={t.state} log={t.log} />
        </div>
      </div>
    {/each}

    {#if streaming && assistantBuf === '' && toolLogs.length === 0}
      <div class="text-muted-foreground text-sm">Thinking…</div>
    {/if}
  </div>

  <footer class="shrink-0 p-3 border-t border-border">
    {#if error}
      <div class="text-destructive text-xs mb-2">{error}</div>
    {/if}

    {#if pends.length > 0}
      <div class="flex flex-wrap gap-1 mb-2">
        {#each pends as a (a.code)}
          <div class="flex items-center gap-1 text-xs rounded border border-border bg-muted/40 px-2 py-0.5">
            <span class="truncate max-w-[120px]">{a.name || a.code}</span>
            <button
              class="text-muted-foreground hover:text-foreground"
              aria-label="Remove attachment"
              onclick={() => (pends = pends.filter(x => x.code !== a.code))}
            >
              ×
            </button>
          </div>
        {/each}
      </div>
    {/if}

    {#if uploading}
      <div class="text-xs text-muted-foreground mb-2">Uploading…</div>
    {/if}

    <div class="flex gap-2 items-end">
      <Button
        variant="ghost"
        size="icon"
        class="h-9"
        disabled={streaming || uploading}
        title="Attach file"
        onclick={pickFiles}
      >
        <PenLine class="size-4" />
      </Button>
      <Textarea
        rows={2}
        class="flex-1 resize-none"
        disabled={streaming}
        placeholder="Ask the agent…"
        bind:value={input}
        onkeydown={onKey}
      />
      {#if streaming}
        <Button
          class="bg-destructive text-destructive-foreground h-9"
          size="icon"
          onclick={interrupt}
          title="Stop"
        >
          <Square class="size-4" />
        </Button>
      {:else}
        <Button
          size="icon"
          disabled={input.trim() === '' && pends.length === 0}
          onclick={() => send()}
          title="Send"
        >
          <Send class="size-4" />
        </Button>
      {/if}
    </div>

    <input
      bind:this={attachInput}
      type="file"
      multiple
      class="hidden"
      onchange={onFiles}
    />

    <div class="flex items-center gap-2 mt-2 text-xs">
      <div class="relative">
        <button
          class="flex items-center gap-1 px-2 py-0.5 rounded border border-input hover:bg-accent/40 transition-colors"
          onclick={() => (showModelPicker = !showModelPicker)}
        >
          <span class="font-medium truncate max-w-[180px]">{active.model || 'model'}</span>
          <ChevronDown class="size-3" />
        </button>
        {#if showModelPicker}
          <div
            class="absolute bottom-full left-0 mb-1 w-64 rounded-md border bg-popover shadow-md z-50 max-h-48 overflow-auto"
            role="listbox"
            tabindex="-1"
            aria-label="Model picker"
          >
            <button
              class="w-full text-left px-3 py-1.5 text-xs hover:bg-accent {active.model ? '' : 'bg-accent/60 font-medium'}"
              onclick={() => switchModel('')}
            >
              default
            </button>
            {#each models as m (m)}
              <button
                class="w-full text-left px-3 py-1.5 text-xs hover:bg-accent {m === active.model ? 'bg-accent/60 font-medium' : ''}"
                onclick={() => switchModel(m)}
              >
                <span class="truncate">{m}</span>
              </button>
            {/each}
          </div>
        {/if}
      </div>

      <div class="relative">
        <button
          class="flex items-center gap-1 px-2 py-0.5 rounded border border-input hover:bg-accent/40 transition-colors"
          onclick={() => (showPresetPicker = !showPresetPicker)}
        >
          <span class="font-medium">{active.preset || 'preset'}</span>
          <ChevronDown class="size-3" />
        </button>
        {#if showPresetPicker}
          <div
            class="absolute bottom-full left-0 mb-1 w-48 rounded-md border bg-popover shadow-md z-50 max-h-48 overflow-auto"
            role="listbox"
            tabindex="-1"
            aria-label="Preset picker"
          >
            <button
              class="w-full text-left px-3 py-1.5 text-xs hover:bg-accent {active.preset ? '' : 'bg-accent/60 font-medium'}"
              onclick={() => switchPreset('')}
            >
              default
            </button>
            {#each presets as p (p.id)}
              <button
                class="w-full text-left px-3 py-1.5 text-xs hover:bg-accent {p.id === active.preset ? 'bg-accent/60 font-medium' : ''}"
                onclick={() => switchPreset(p.id)}
              >
                {p.id}
              </button>
            {/each}
          </div>
        {/if}
      </div>
    </div>
  </footer>
</div>

{#if showMailbox}
  <MailboxPanel name={active.name} onclose={() => (showMailbox = false)} />
{/if}

{#if showRename}
  <Dialog.Root open onOpenChange={o => { if (!o) showRename = false }}>
    <Dialog.Content class="max-w-xs w-full">
      <Dialog.Header>
        <Dialog.Title>Rename session</Dialog.Title>
      </Dialog.Header>
      <Input bind:value={renameInput} placeholder="new name" />
      <Dialog.Footer>
        <Button
          variant="outline"
          onclick={() => (showRename = false)}
        >
          Cancel
        </Button>
        <Button
          disabled={renameInput.trim() === ''}
          onclick={rename}
        >
          Rename
        </Button>
      </Dialog.Footer>
    </Dialog.Content>
  </Dialog.Root>
{/if}
