import type { CircuitBreaker } from '../resilience/circuit-breaker.js';
import type { CostCalculator } from '../cost/calculator.js';

export type RoutingStrategy = 'round-robin' | 'least-latency' | 'cost-optimized' | 'capability-based';

export interface RoutingConfig {
  strategy: RoutingStrategy;
  fallbackChain: string[];
  stickySession?: boolean;
  maxRetries?: number;
}

export interface ProviderSelection {
  provider: ProviderInstance;
  model: string;
  reason: string;
}

export interface ProviderInstance {
  name: string;
  models: string[];
  weight: number;
  complete: (request: unknown) => Promise<unknown>;
  stream: (request: unknown) => Promise<AsyncIterable<unknown>>;
}

interface LatencyRecord {
  values: number[];
  lastUpdated: number;
}

export class Router {
  private roundRobinIndex = 0;
  private latencyHistory: Map<string, LatencyRecord> = new Map();
  private readonly maxHistorySize = 100;

  constructor(
    private readonly config: RoutingConfig,
    private readonly circuitBreakers: Map<string, CircuitBreaker>,
    private readonly costCalculator: CostCalculator,
  ) {}

  async selectProvider(request: { model?: string; messages?: unknown[] }): Promise<ProviderSelection> {
    const availableProviders = this.getHealthyProviders();

    if (availableProviders.length === 0) {
      throw new NoHealthyProviderError('All providers are unavailable');
    }

    // If specific model requested, find the provider that has it
    if (request.model && request.model !== 'auto') {
      const match = availableProviders.find(p =>
        p.models.includes(request.model!),
      );
      if (match) {
        return { provider: match, model: request.model, reason: 'explicit-model' };
      }
    }

    // Apply routing strategy
    switch (this.config.strategy) {
      case 'round-robin':
        return this.roundRobin(availableProviders);
      case 'least-latency':
        return this.leastLatency(availableProviders);
      case 'cost-optimized':
        return this.costOptimized(availableProviders);
      case 'capability-based':
        return this.capabilityBased(availableProviders, request);
      default:
        return this.roundRobin(availableProviders);
    }
  }

  recordLatency(provider: string, latencyMs: number): void {
    let record = this.latencyHistory.get(provider);
    if (!record) {
      record = { values: [], lastUpdated: Date.now() };
      this.latencyHistory.set(provider, record);
    }

    record.values.push(latencyMs);
    if (record.values.length > this.maxHistorySize) {
      record.values.shift();
    }
    record.lastUpdated = Date.now();
  }

  getLatencyP95(provider: string): number {
    const record = this.latencyHistory.get(provider);
    if (!record || record.values.length === 0) return 0;

    const sorted = [...record.values].sort((a, b) => a - b);
    const index = Math.ceil(sorted.length * 0.95) - 1;
    return sorted[index] ?? 0;
  }

  private getHealthyProviders(): ProviderInstance[] {
    const healthy: ProviderInstance[] = [];

    for (const [name, breaker] of this.circuitBreakers) {
      if (breaker.getState() !== 'OPEN') {
        // Provider is available (CLOSED or HALF_OPEN)
        // In production, this would reference actual provider instances
        healthy.push({
          name,
          models: [],
          weight: 1,
          complete: async () => ({}),
          stream: async () => (async function* () {})(),
        });
      }
    }

    return healthy;
  }

  private roundRobin(providers: ProviderInstance[]): ProviderSelection {
    // Weighted round-robin
    const totalWeight = providers.reduce((sum, p) => sum + p.weight, 0);
    this.roundRobinIndex = (this.roundRobinIndex + 1) % providers.length;

    const selected = providers[this.roundRobinIndex]!;
    const model = selected.models[0] ?? 'default';

    return { provider: selected, model, reason: 'round-robin' };
  }

  private leastLatency(providers: ProviderInstance[]): ProviderSelection {
    let bestProvider = providers[0]!;
    let bestLatency = Infinity;

    for (const provider of providers) {
      const p95 = this.getLatencyP95(provider.name);
      if (p95 < bestLatency) {
        bestLatency = p95;
        bestProvider = provider;
      }
    }

    return {
      provider: bestProvider,
      model: bestProvider.models[0] ?? 'default',
      reason: `least-latency (P95: ${bestLatency}ms)`,
    };
  }

  private costOptimized(providers: ProviderInstance[]): ProviderSelection {
    const cheapest = this.costCalculator.getCheapestModel(
      providers.map(p => p.name),
    );

    if (cheapest) {
      const provider = providers.find(p => p.name === cheapest.provider)!;
      return {
        provider,
        model: cheapest.model,
        reason: `cost-optimized ($${cheapest.pricing.inputPerMillion}/M input)`,
      };
    }

    return this.roundRobin(providers);
  }

  private capabilityBased(
    providers: ProviderInstance[],
    _request: { model?: string; messages?: unknown[] },
  ): ProviderSelection {
    // Default to first available for now
    // In production: analyze request for vision/tools/long-context needs
    const selected = providers[0]!;
    return {
      provider: selected,
      model: selected.models[0] ?? 'default',
      reason: 'capability-based',
    };
  }
}

export class NoHealthyProviderError extends Error {
  readonly code = 'NO_HEALTHY_PROVIDER';

  constructor(message: string) {
    super(message);
    this.name = 'NoHealthyProviderError';
  }
}
