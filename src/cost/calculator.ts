/**
 * Cost Calculator
 *
 * Computes per-request cost based on provider, model, and token usage.
 * Pricing data is maintained in a registry that can be updated at runtime.
 */

export interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
}

export interface CostBreakdown {
  input: number;
  output: number;
  total: number;
  currency: 'USD';
}

interface ModelPricing {
  inputPerMillion: number;
  outputPerMillion: number;
}

// Pricing as of 2026-08 (USD per million tokens)
const DEFAULT_PRICING: Record<string, Record<string, ModelPricing>> = {
  openai: {
    'gpt-4o': { inputPerMillion: 2.50, outputPerMillion: 10.00 },
    'gpt-4o-mini': { inputPerMillion: 0.15, outputPerMillion: 0.60 },
    'gpt-4-turbo': { inputPerMillion: 10.00, outputPerMillion: 30.00 },
    'o1': { inputPerMillion: 15.00, outputPerMillion: 60.00 },
    'o1-mini': { inputPerMillion: 3.00, outputPerMillion: 12.00 },
  },
  anthropic: {
    'claude-sonnet-4-20250514': { inputPerMillion: 3.00, outputPerMillion: 15.00 },
    'claude-haiku-4-20250514': { inputPerMillion: 0.80, outputPerMillion: 4.00 },
    'claude-opus-4-20250514': { inputPerMillion: 15.00, outputPerMillion: 75.00 },
  },
  groq: {
    'llama-3.3-70b-versatile': { inputPerMillion: 0.59, outputPerMillion: 0.79 },
    'llama-3.1-8b-instant': { inputPerMillion: 0.05, outputPerMillion: 0.08 },
    'mixtral-8x7b-32768': { inputPerMillion: 0.24, outputPerMillion: 0.24 },
  },
  google: {
    'gemini-2.5-pro': { inputPerMillion: 1.25, outputPerMillion: 10.00 },
    'gemini-2.5-flash': { inputPerMillion: 0.15, outputPerMillion: 0.60 },
  },
};

export class CostCalculator {
  private pricing: Record<string, Record<string, ModelPricing>>;

  constructor(customPricing?: Record<string, Record<string, ModelPricing>>) {
    this.pricing = customPricing ?? { ...DEFAULT_PRICING };
  }

  calculate(provider: string, model: string, usage: TokenUsage): CostBreakdown {
    const providerPricing = this.pricing[provider];
    if (!providerPricing) {
      return this.zeroCost();
    }

    const modelPricing = providerPricing[model];
    if (!modelPricing) {
      return this.zeroCost();
    }

    const inputCost = (usage.inputTokens / 1_000_000) * modelPricing.inputPerMillion;
    const outputCost = (usage.outputTokens / 1_000_000) * modelPricing.outputPerMillion;

    return {
      input: this.round(inputCost),
      output: this.round(outputCost),
      total: this.round(inputCost + outputCost),
      currency: 'USD',
    };
  }

  estimateCost(
    provider: string,
    model: string,
    estimatedInputTokens: number,
    estimatedOutputTokens: number,
  ): CostBreakdown {
    return this.calculate(provider, model, {
      inputTokens: estimatedInputTokens,
      outputTokens: estimatedOutputTokens,
      totalTokens: estimatedInputTokens + estimatedOutputTokens,
    });
  }

  updatePricing(provider: string, model: string, pricing: ModelPricing): void {
    if (!this.pricing[provider]) {
      this.pricing[provider] = {};
    }
    this.pricing[provider][model] = pricing;
  }

  getAvailableModels(provider: string): string[] {
    return Object.keys(this.pricing[provider] ?? {});
  }

  getCheapestModel(providers: string[]): { provider: string; model: string; pricing: ModelPricing } | null {
    let cheapest: { provider: string; model: string; pricing: ModelPricing } | null = null;
    let lowestCost = Infinity;

    for (const provider of providers) {
      const models = this.pricing[provider];
      if (!models) continue;

      for (const [model, pricing] of Object.entries(models)) {
        // Weighted average: assume 1:2 input:output ratio
        const avgCost = pricing.inputPerMillion * 0.33 + pricing.outputPerMillion * 0.67;
        if (avgCost < lowestCost) {
          lowestCost = avgCost;
          cheapest = { provider, model, pricing };
        }
      }
    }

    return cheapest;
  }

  private zeroCost(): CostBreakdown {
    return { input: 0, output: 0, total: 0, currency: 'USD' };
  }

  private round(value: number): number {
    return Math.round(value * 1_000_000) / 1_000_000; // 6 decimal places
  }
}
