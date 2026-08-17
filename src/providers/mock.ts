/**
 * Deterministic provider for examples and tests.
 *
 * Does not call the network. Completions echo the last user message with a
 * model tag so routing decisions are visible without spending tokens.
 */

import type {
  ProviderAdapter,
  ProviderCompleteResult,
  ProviderRequest,
} from '../core/config.js';
import type { StreamChunk } from '../streaming/multiplexer.js';

export interface MockProviderOptions {
  /** Artificial delay before the first token. Defaults to 0. */
  latencyMs?: number;
  /** Prefix prepended to the echoed content. */
  prefix?: string;
}

function estimateTokens(text: string): number {
  return Math.max(1, Math.ceil(text.length / 4));
}

export class MockProvider implements ProviderAdapter {
  constructor(private readonly options: MockProviderOptions = {}) {}

  async complete(request: ProviderRequest): Promise<ProviderCompleteResult> {
    if (this.options.latencyMs) {
      await new Promise((resolve) => setTimeout(resolve, this.options.latencyMs));
    }

    const lastUser = [...request.messages].reverse().find((m) => m.role === 'user');
    const prompt = lastUser?.content ?? '';
    const prefix = this.options.prefix ?? 'mock';
    const content = `[${request.model} / ${prefix}] ${prompt}`;
    const inputTokens = estimateTokens(prompt);
    const outputTokens = estimateTokens(content);

    return {
      content,
      usage: {
        inputTokens,
        outputTokens,
        totalTokens: inputTokens + outputTokens,
      },
    };
  }

  async stream(request: ProviderRequest): Promise<AsyncIterable<StreamChunk>> {
    const result = await this.complete(request);
    return this.emitChunks(result);
  }

  private async *emitChunks(result: ProviderCompleteResult): AsyncGenerator<StreamChunk> {
    const parts = result.content.split(' ').filter((part) => part.length > 0);

    for (const [index, part] of parts.entries()) {
      const isLast = index === parts.length - 1;
      yield {
        delta: isLast ? part : `${part} `,
        ...(isLast
          ? { finishReason: 'stop' as const, usage: result.usage }
          : {}),
      };
    }
  }
}
