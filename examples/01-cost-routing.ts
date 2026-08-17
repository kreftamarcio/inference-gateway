/**
 * Exercises routing, budget, and streaming without calling a paid provider.
 *
 *   npx tsx examples/01-cost-routing.ts
 */

import { InferenceGateway } from '../src/core/gateway.js';
import { MockProvider } from '../src/providers/mock.js';

const gateway = new InferenceGateway({
  providers: [
    {
      name: 'openai',
      apiKey: 'mock',
      models: ['gpt-4o-mini'],
      weight: 0.6,
      adapter: new MockProvider({ prefix: 'openai-mock' }),
    },
    {
      name: 'groq',
      apiKey: 'mock',
      models: ['llama-3.1-8b-instant'],
      weight: 0.4,
      adapter: new MockProvider({ prefix: 'groq-mock' }),
    },
  ],
  routing: {
    strategy: 'cost-optimized',
    fallbackChain: ['groq', 'openai'],
  },
  budget: {
    daily: 1,
    monthly: 10,
    alertThreshold: 0.8,
  },
  telemetry: {
    serviceName: 'inference-gateway-example',
    enabled: false,
  },
});

gateway.on('request:complete', (response) => {
  console.log('complete', {
    provider: response.provider,
    model: response.model,
    cost: response.cost.total,
    latencyMs: Math.round(response.latency),
  });
});

const response = await gateway.complete({
  messages: [{ role: 'user', content: 'Route this to the cheapest healthy model.' }],
  model: 'auto',
});

console.log(response.content);
console.log('health', gateway.getProviderHealth());
console.log('usage', gateway.getUsage());

process.stdout.write('stream: ');
for await (const chunk of gateway.stream({
  messages: [{ role: 'user', content: 'Stream a short reply.' }],
  model: 'auto',
})) {
  process.stdout.write(chunk.delta);
}
process.stdout.write('\n');
