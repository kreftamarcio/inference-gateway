import { describe, expect, it } from 'vitest';
import { MockProvider } from '../../src/providers/mock.js';

describe('MockProvider', () => {
  it('echoes the last user message with the selected model', async () => {
    const provider = new MockProvider({ prefix: 'test' });
    const result = await provider.complete({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: 'ping' }],
    });

    expect(result.content).toBe('[gpt-4o-mini / test] ping');
    expect(result.usage.totalTokens).toBeGreaterThan(0);
  });

  it('streams the same content and reports usage on the last chunk', async () => {
    const provider = new MockProvider({ prefix: 'test' });
    const stream = await provider.stream({
      model: 'gpt-4o-mini',
      messages: [{ role: 'user', content: 'ping' }],
    });

    let content = '';
    let usage;
    for await (const chunk of stream) {
      content += chunk.delta;
      if (chunk.usage) usage = chunk.usage;
    }

    expect(content).toBe('[gpt-4o-mini / test] ping');
    expect(usage?.totalTokens).toBeGreaterThan(0);
  });
});
