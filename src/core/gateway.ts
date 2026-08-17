import { EventEmitter } from 'eventemitter3';
import type { GatewayConfig, ProviderRequest } from './config.js';
import { Router, type ProviderInstance } from './router.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';
import { CostCalculator } from '../cost/calculator.js';
import { BudgetGuard } from '../cost/budget.js';
import { StreamMultiplexer, type StreamChunk } from '../streaming/multiplexer.js';
import { Tracer } from '../telemetry/tracer.js';
import { Logger } from '../telemetry/logger.js';

export type { StreamChunk };

export interface CompletionRequest {
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>;
  model?: string | 'auto';
  maxTokens?: number;
  temperature?: number;
  tools?: Array<{ name: string; description: string; parameters: Record<string, unknown> }>;
  stream?: boolean;
  /** Attributes spend to a tenant when per-tenant budgets are configured. */
  tenantId?: string;
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

/** Emitted when a stream finishes, so a streamed call is as observable as a
 *  buffered one. Without it, streaming spend and latency are invisible. */
export interface StreamSummary {
  requestId: string;
  provider: string;
  model: string;
  usage: CompletionResponse['usage'];
  cost: CompletionResponse['cost'];
  timeToFirstTokenMs: number | null;
  totalDurationMs: number;
  chunkCount: number;
}

type GatewayEvents = {
  'request:start': [{ requestId: string; provider: string; model: string }];
  'request:complete': [CompletionResponse];
  'request:error': [{ requestId: string; error: Error; provider: string }];
  'stream:complete': [StreamSummary];
  'circuit:open': [{ provider: string; failures: number }];
  'circuit:close': [{ provider: string }];
  'budget:warning': [{ usage: number; limit: number; period: string; tenantId?: string }];
  'budget:exceeded': [{ usage: number; limit: number; period: string; tenantId?: string }];
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
    const instances = new Map<string, ProviderInstance>();

    for (const provider of config.providers) {
      this.circuitBreakers.set(
        provider.name,
        new CircuitBreaker(
          provider.circuitBreaker ?? {
            failureThreshold: 5,
            recoveryTimeout: 30_000,
            successThreshold: 3,
            monitorWindow: 60_000,
          },
        ),
      );

      instances.set(provider.name, {
        name: provider.name,
        models: provider.models,
        weight: provider.weight ?? 1,
        complete: (request) => provider.adapter.complete(request),
        stream: (request) => provider.adapter.stream(request),
      });
    }

    this.router = new Router(config.routing, this.circuitBreakers, this.costCalculator, instances);
    this.logger.info('Gateway initialized', {
      providers: config.providers.map((p) => p.name),
    });
  }

  async complete(request: CompletionRequest): Promise<CompletionResponse> {
    const requestId = crypto.randomUUID();
    const span = this.tracer.startSpan('gateway.complete', { requestId });
    let providerName = 'unrouted';

    try {
      await this.budgetGuard.assertWithinBudget(request.tenantId);

      const selected = await this.router.selectProvider(request);
      providerName = selected.provider.name;

      span.setAttribute('provider', providerName);
      span.setAttribute('model', selected.model);

      this.emit('request:start', { requestId, provider: providerName, model: selected.model });

      const breaker = this.breakerFor(providerName);
      const startTime = performance.now();
      const providerRequest = this.toProviderRequest(request, selected.model);

      const result = await breaker.execute(async () => selected.provider.complete(providerRequest));

      const latency = performance.now() - startTime;
      this.router.recordLatency(providerName, latency);
      const cost = this.costCalculator.calculate(providerName, selected.model, result.usage);

      const response: CompletionResponse = {
        id: requestId,
        content: result.content,
        model: selected.model,
        provider: providerName,
        usage: result.usage,
        cost,
        latency,
        cached: false,
      };

      await this.budgetGuard.recordUsage(cost.total, request.tenantId);
      this.emit('request:complete', response);
      span.setStatus({ code: 0 });

      return response;
    } catch (error) {
      span.setStatus({ code: 2, message: (error as Error).message });
      this.emit('request:error', {
        requestId,
        error: error as Error,
        provider: providerName,
      });
      throw error;
    } finally {
      span.end();
    }
  }

  async *stream(request: CompletionRequest): AsyncGenerator<StreamChunk> {
    const requestId = crypto.randomUUID();
    const span = this.tracer.startSpan('gateway.stream', { requestId });

    let providerName = 'unrouted';
    let model = 'unknown';

    try {
      await this.budgetGuard.assertWithinBudget(request.tenantId);

      const selected = await this.router.selectProvider(request);
      providerName = selected.provider.name;
      model = selected.model;

      span.setAttribute('provider', providerName);
      span.setAttribute('model', model);

      this.emit('request:start', { requestId, provider: providerName, model });

      const breaker = this.breakerFor(providerName);
      const providerRequest = this.toProviderRequest(request, model);
      const providerStream = await breaker.execute(async () => selected.provider.stream(providerRequest));

      let usage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };
      let chunkCount = 0;
      let timeToFirstTokenMs: number | null = null;
      let totalDurationMs = 0;

      for await (const chunk of this.multiplexer.pipe(providerStream, {
        requestId,
        onComplete: (metrics) => {
          timeToFirstTokenMs = metrics.firstChunkLatencyMs;
          totalDurationMs = metrics.totalDurationMs;
          chunkCount = metrics.chunksReceived;
        },
      })) {
        if (chunk.usage) {
          usage = {
            inputTokens: chunk.usage.inputTokens || usage.inputTokens,
            outputTokens: chunk.usage.outputTokens || usage.outputTokens,
            totalTokens: chunk.usage.totalTokens || usage.totalTokens,
          };
        }

        yield chunk;
      }

      if (usage.totalTokens === 0) {
        usage.totalTokens = usage.inputTokens + usage.outputTokens;
      }

      const cost = this.costCalculator.calculate(providerName, model, usage);
      await this.budgetGuard.recordUsage(cost.total, request.tenantId);
      this.router.recordLatency(providerName, totalDurationMs);

      this.emit('stream:complete', {
        requestId,
        provider: providerName,
        model,
        usage,
        cost,
        timeToFirstTokenMs,
        totalDurationMs,
        chunkCount,
      });

      span.setStatus({ code: 0 });
    } catch (error) {
      span.setStatus({ code: 2, message: (error as Error).message });
      this.emit('request:error', {
        requestId,
        error: error as Error,
        provider: providerName,
      });
      throw error;
    } finally {
      span.end();
    }
  }

  getProviderHealth(): Record<
    string,
    { state: string; failures: number; latencyP95: number }
  > {
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

  getUsage(tenantId?: string): ReturnType<BudgetGuard['getUsageSummary']> {
    return this.budgetGuard.getUsageSummary(tenantId);
  }

  private toProviderRequest(request: CompletionRequest, model: string): ProviderRequest {
    return {
      messages: request.messages,
      model,
      ...(request.maxTokens !== undefined ? { maxTokens: request.maxTokens } : {}),
      ...(request.temperature !== undefined ? { temperature: request.temperature } : {}),
    };
  }

  private breakerFor(providerName: string): CircuitBreaker {
    const breaker = this.circuitBreakers.get(providerName);

    if (!breaker) {
      throw new Error(
        `No circuit breaker registered for provider "${providerName}". The router ` +
          'selected a provider that is not in the gateway configuration.',
      );
    }

    return breaker;
  }

  private validateConfig(config: GatewayConfig): void {
    if (!config.providers || config.providers.length === 0) {
      throw new Error('At least one provider must be configured');
    }

    const seen = new Set<string>();

    for (const provider of config.providers) {
      if (!provider.apiKey) {
        throw new Error(`API key required for provider: ${provider.name}`);
      }

      if (!provider.adapter) {
        throw new Error(
          `Provider "${provider.name}" is missing an adapter. Inject MockProvider ` +
            'or a real provider adapter; the gateway does not invent HTTP clients.',
        );
      }

      if (seen.has(provider.name)) {
        throw new Error(
          `Duplicate provider name "${provider.name}". Breakers and health are keyed by ` +
            'name, so a duplicate would silently share one breaker between two configs.',
        );
      }

      seen.add(provider.name);
    }
  }
}
