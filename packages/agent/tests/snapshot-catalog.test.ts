import { providers as snapshotProviders } from '@opencode-ai/models/snapshot'
import { describe, expect, it } from 'vitest'
import { catalogModel, toModelVariant, variantsForApiType } from '../src/variants.js'

describe('real snapshot catalog', () => {
  it('resolves anthropic/claude-opus-4-6 with effort variants', () => {
    const m = catalogModel(snapshotProviders, 'anthropic', 'claude-opus-4-6')
    expect(m).not.toBeNull()
    const vs = variantsForApiType(m!, 'anthropic').map(toModelVariant)
    const ids = vs.map(v => v.id)
    expect(ids).toContain('low')
    expect(ids).toContain('high')
  })

  it('does NOT resolve an unknown provider id', () => {
    expect(catalogModel(snapshotProviders, 'no-such-provider', 'glm-5.3')).toBeNull()
    expect(catalogModel(snapshotProviders, 'anthropic', 'no-such-model')).toBeNull()
  })
})
