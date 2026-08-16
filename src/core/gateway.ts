import { EventEmitter } from 'eventemitter3';
import { z } from 'zod';
import type { ProviderConfig, RoutingConfig, GatewayConfig } from './config.js';
import { Router } from './router.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';
import { CostCalculator } from '../cost/calculator.js';
import { BudgetGuard } from '../cost/budget.js';
import { StreamMultiplexer } from '../streaming/multiplexer.js';
import { Tracer } from '../telemetry/tracer.js';
import { Logger } from '../telemetry/logger.js';

export interface CompletionRequest {
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>;
  model?: string | 'auto';
  maxTokens?: number;
  temperature?: number;
  tools?: Array<{ name: string; description: string; parameters: Record<string, unknown> }>;
  stream?: boolean;
}

export interface CompletionResponse {
  id: string;
  content: string;
  model: string;
  provider: string;
  usage: { inputTokens: number; outputTokens: number; totalTokens: number };
  cost: { input: number; output: number; total: number };
  latency: number;
  cached: boolean;
}

export interface StreamChunk {
  delta: string;
  finishReason?: 'stop' | 'length' | 'tool_calls';
  usage?: CompletionResponse['usage'];
}

type GatewayEvents = {
  'request:start': [{ requestId: string; provider: string; model: string }];
  'request:complete': [CompletionResponse];
  'request:error': [{ requestId: string; error: Error; provider: string }];
  'circuit:open': [{ provider: string; failures: number }];
  'circuit:close': [{ provider: string }];
  'budget:warning': [{ usage: number; limit: number; period: string }];
  'budget:exceeded': [{ usage: number; limit: number; period: string }];
};

export class InferenceGateway extends EventEmitter<GatewayEvents> {
  private readonly router: Router;
  private readonly circuitBreakers: Map<string, CircuitBreaker>;
  private readonly costCalculator: CostCalculator;
  private readonly budgetGuard: BudgetGuard;
  private readonly multiplexer: StreamMultiplexer;
  private readonly tracer: Tracer;
  private readonly logger: Logger;

  constructor(private readonly config: GatewayConfig) {
    super();
    this.validateConfig(config);

    this.logger = new Logger(config.telemetry?.serviceName ?? 'inference-gateway');
    this.tracer = new Tracer(config.telemetry);
    this.costCalculator = new CostCalculator();
    this.budgetGuard = new BudgetGuard(config.budget, this);
    this.multiplexer = new StreamMultiplexer();

    this.circuitBreakers = new Map();
    for (const provider of config.providers) {
      this.circuitBreakers.set(
        provider.name,
        new CircuitBreaker(provider.circuitBreaker ?? {
          failureThreshold: 5,
          recoveryTimeout: 30_000,
          successThreshold: 3,
          monitorWindow: 60_000,
        }),
      );
    }

    this.router = new Router(config.routing, this.circuitBreakers, this.costCalculator);
    this.logger.info('Gateway initialized', { providers: config.providers.map(p => p.name) });
  }

  async complete(request: CompletionRequest): Promise<CompletionResponse> {
    const requestId = crypto.randomUUID();
    const span = this.tracer.startSpan('gateway.complete', { requestId });

    try {
      // Budget check
      await this.budgetGuard.assertWithinBudget();

      // Route to best provider
      const selected = await this.router.selectProvider(request);
      span.setAttribute('provider', selected.provider.name);
      span.setAttribute('model', selected.model);

      this.emit('request:start', {
        requestId,
        provider: selected.provider.name,
        model: selected.model,
      });

      // Execute with circuit breaker
      const breaker = this.circuitBreakers.get(selected.provider.name)!;
      const startTime = performance.now();

      const result = await breaker.execute(async () => {
        return selected.provider.complete({
          ...request,
          model: selected.model,
        });
      });

      const latency = performance.now() - startTime;

      // Calculate cost
      const cost = this.costCalculator.calculate(
        selected.provider.name,
        selected.model,
        result.usage,
      );

      const response: CompletionResponse = {
        id: requestId,
        content: result.content,
        model: selected.model,
        provider: selected.provider.name,
        usage: result.usage,
        cost,
        latency,
        cached: false,
      };

      // Track cost
      await this.budgetGuard.recordUsage(cost.total);
      this.emit('request:complete', response);
      span.setStatus({ code: 0 });

      return response;
    } catch (error) {
      span.setStatus({ code: 2, message: (error as Error).message });
      this.emit('request:error', {
        requestId,
        error: error as Error,
        provider: 'unknown',
      });
      throw error;
    } finally {
      span.end();
    }
  }

  async *stream(request: CompletionRequest): AsyncGenerator<StreamChunk> {
    const requestId = crypto.randomUUID();
    const selected = await this.router.selectProvider(request);
    const breaker = this.circuitBreakers.get(selected.provider.name)!;

    const providerStream = await breaker.execute(async () => {
      return selected.provider.stream({
        ...request,
        model: selected.model,
      });
    });

    yield* this.multiplexer.pipe(providerStream, { requestId });
  }

  getProviderHealth(): Record<string, { state: string; failures: number; latencyP95: number }> {
    const health: Record<string, { state: string; failures: number; latencyP95: number }> = {};
    for (const [name, breaker] of this.circuitBreakers) {
      health[name] = {
        state: breaker.getState(),
        failures: breaker.getFailureCount(),
        latencyP95: this.router.getLatencyP95(name),
      };
    }
    return health;
  }

  private validateConfig(config: GatewayConfig): void {
    if (!config.providers || config.providers.length === 0) {
      throw new Error('At least one provider must be configured');
    }
    for (const provider of config.providers) {
      if (!provider.apiKey) {
        throw new Error(`API key required for provider: ${provider.name}`);
      }
    }
  }
}
