<script lang="ts">
  import { onMount } from 'svelte'
  import { api, type Provider } from '$lib/api'
  import type { CatalogProvider } from '@easylab-agent/schema'
  import { Plus, Trash2, Check, RefreshCw } from '@lucide/svelte'
  import { Button } from '$lib/components/ui/button'
  import { Input } from '$lib/components/ui/input'
  import { NativeSelect } from '$lib/components/ui/native-select'
  import * as Dialog from '$lib/components/ui/dialog'

  let { onclose }: { onclose: () => void } = $props()

  let providers: Record<string, Provider> = $state({})
  let catalog: Record<string, CatalogProvider> = $state({})
  let error = $state('')

  let pid = $state('')
  let apiType = $state('openai-compatible')
  let baseUrl = $state('')
  let apiKey = $state('')
  let selectedModels: string[] = $state([])
  let testResult = $state('')

  const list = $derived.by(() => Object.values(providers ?? {}))
  const catalogList = $derived.by(() => Object.values(catalog ?? {}))
  const modelChoices = $derived.by(() =>
    Object.keys(catalogList.find(c => c.id === pid)?.models ?? {}),
  )

  function refresh() {
    void api.listProviders().match(
      r => {
        providers = r.providers
      },
      e => {
        error = e
      },
    )
    void api.catalogProviders().match(
      r => {
        catalog = r
      },
      () => {
        catalog = {}
      },
    )
  }

  function prefill(item: CatalogProvider) {
    pid = item.id ?? ''
    baseUrl = item.api ?? defaultBaseUrl(item.npm)
    apiType = apiTypeFromNpm(item.npm)
    selectedModels = Object.keys(item.models)
    apiKey = ''
  }

  function toggleModel(model: string) {
    selectedModels = selectedModels.includes(model)
      ? selectedModels.filter(m => m !== model)
      : [...selectedModels, model]
  }

  function apiTypeFromNpm(npm: string): string {
    if (npm === '@ai-sdk/anthropic') return 'anthropic'
    if (npm === '@ai-sdk/openai') return 'openai'
    if (npm === '@ai-sdk/deepseek') return 'deepseek'
    if (npm === '@ai-sdk/google') return 'google'
    return 'openai-compatible'
  }

  function defaultBaseUrl(npm: string): string {
    if (npm === '@ai-sdk/anthropic') return 'https://api.anthropic.com/v1'
    if (npm === '@ai-sdk/openai') return 'https://api.openai.com/v1'
    if (npm === '@ai-sdk/google') return 'https://generativelanguage.googleapis.com/v1beta'
    return ''
  }

  function register() {
    if (!pid.trim() || !baseUrl.trim()) return
    error = ''
    void api
      .registerProvider({
        provider_id: pid.trim(),
        api_type: apiType,
        base_url: baseUrl.trim(),
        api_key: apiKey,
        models: selectedModels,
      })
      .match(
        () => {
          pid = ''
          baseUrl = ''
          apiKey = ''
          selectedModels = []
          refresh()
        },
        e => {
          error = e
        },
      )
  }

  function test() {
    testResult = 'testing…'
    void api
      .testProvider({ api_type: apiType, base_url: baseUrl.trim(), api_key: apiKey })
      .match(
        r => {
          testResult = r.ok ? 'OK' : (r.error ?? 'failed')
        },
        e => {
          testResult = e
        },
      )
  }

  function remove(id: string) {
    void api.deleteProvider(id).match(
      () => refresh(),
      e => {
        error = e
      },
    )
  }

  onMount(refresh)
</script>

<!-- backdrop -->
<Dialog.Root open onOpenChange={o => { if (!o) onclose() }}>
  <Dialog.Content class="max-w-lg w-full">
    <Dialog.Header>
      <Dialog.Title>Providers</Dialog.Title>
    </Dialog.Header>

    <div class="flex flex-col gap-3 max-h-[70vh] overflow-y-auto">
      {#if list.length === 0}
        <div class="text-muted-foreground text-sm">No providers registered.</div>
      {/if}

      {#each list as p (p.provider_id)}
        <div class="flex items-center gap-2 bg-muted rounded px-3 py-2">
          <Check class="size-4 text-emerald-400 shrink-0" />
          <div class="flex-1 min-w-0">
            <div class="text-sm font-medium">{p.provider_id}</div>
            <div class="text-xs text-muted-foreground truncate">
              {p.api_type} · {p.base_url}
            </div>
          </div>
          <Button
            variant="ghost"
            size="icon"
            aria-label="Delete"
            title="Delete"
            onclick={() => remove(p.provider_id)}
          >
            <Trash2 class="text-destructive size-4" />
          </Button>
        </div>
      {/each}

      <div class="border-t border-border pt-3 flex flex-col gap-2">
        <div class="text-xs text-muted-foreground font-medium uppercase">Register new</div>
        <NativeSelect bind:value={pid}>
          <option value="" disabled>pick from catalog…</option>
          {#each catalogList as item (item.id)}
            <option value={item.id}>{item.name}</option>
          {/each}
        </NativeSelect>
        <Button
          variant="ghost"
          class="text-xs text-left h-auto py-1 px-0 justify-start"
          onclick={() => {
            const item = catalogList.find(c => c.id === pid)
            if (item) prefill(item)
          }}
        >
          <RefreshCw class="size-3" /> prefill from catalog
        </Button>
        <Input bind:value={pid} placeholder="provider_id" />
        <NativeSelect bind:value={apiType}>
          <option value="openai-compatible">openai-compatible</option>
          <option value="openai">openai</option>
          <option value="anthropic">anthropic</option>
          <option value="deepseek">deepseek</option>
          <option value="google">google</option>
        </NativeSelect>
        <Input bind:value={baseUrl} placeholder="base_url (https://…)" />
        <Input
          bind:value={apiKey}
          type="password"
          placeholder="api_key (optional)"
        />
        {#if catalogList.length > 0}
          <div class="text-xs text-muted-foreground font-medium">Models</div>
          <div class="flex flex-wrap gap-1.5 max-h-32 overflow-y-auto">
            {#each modelChoices as m (m)}
              <Button
                variant={selectedModels.includes(m) ? 'secondary' : 'outline'}
                size="xs"
                class="rounded-full"
                onclick={() => toggleModel(m)}
              >
                {m}
              </Button>
            {/each}
          </div>
        {/if}
        <div class="flex gap-2">
          <Button
            class="flex-1"
            disabled={!pid.trim() || !baseUrl.trim()}
            onclick={register}
          >
            <Plus class="size-4" /> Add provider
          </Button>
          <Button
            variant="outline"
            disabled={!baseUrl.trim()}
            onclick={test}
          >
            Test
          </Button>
        </div>
        {#if testResult !== ''}
          <div class="text-xs text-muted-foreground">{testResult}</div>
        {/if}
      </div>

      {#if error}
        <div class="text-destructive text-xs">{error}</div>
      {/if}
    </div>
  </Dialog.Content>
</Dialog.Root>
