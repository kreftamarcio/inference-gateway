import { EventEmitter } from 'eventemitter3';
import type { GatewayConfig } from './config.js';
import { Router } from './router.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';
import { CostCalculator } from '../cost/calculator.js';
import { BudgetGuard } from '../cost/budget.js';
import { StreamMultiplexer, type StreamChunk } from '../streaming/multiplexer.js';
import { Tracer } from '../telemetry/tracer.js';
import { Logger } from '../telemetry/logger.js';

// Re-exported rather than redeclared. This file previously defined its own
// StreamChunk that omitted 'content_filter' and 'error' from the finish-reason union
// and used a different usage shape, so the same contract had two incompatible
// definitions and index.ts was exporting both.
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
    }

    this.router = new Router(config.routing, this.circuitBreakers, this.costCalculator);
    this.logger.info('Gateway initialized', {
      providers: config.providers.map((p) => p.name),
    });
  }

  async complete(request: CompletionRequest): Promise<CompletionResponse> {
    const requestId = crypto.randomUUID();
    const span = this.tracer.startSpan('gateway.complete', { requestId });

    // Declared outside the try so the catch can attribute a failure to the provider
    // that was actually selected. It previously always reported 'unknown', which made
    // a trace unable to answer "which provider failed?".
    let providerName = 'unrouted';

    try {
      // Checked BEFORE routing and inference. Checking afterwards means every run
      // exceeds its ceiling by one call, and inference is the most expensive operation
      // in the system.
      await this.budgetGuard.assertWithinBudget(request.tenantId);

      const selected = await this.router.selectProvider(request);
      providerName = selected.provider.name;

      span.setAttribute('provider', providerName);
      span.setAttribute('model', selected.model);

      this.emit('request:start', { requestId, provider: providerName, model: selected.model });

      const breaker = this.breakerFor(providerName);
      const startTime = performance.now();

      const result = await breaker.execute(async () =>
        selected.provider.complete({ ...request, model: selected.model }),
      );

      const latency = performance.now() - startTime;
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

  /**
   * Stream a completion.
   *
   * Enforces the same budget and records the same cost as complete(). Both were missing
   * here: a budget-exhausted caller was blocked on buffered calls and served on
   * streamed ones, and streamed tokens never reached recordUsage, so a streaming
   * workload could exceed a monthly ceiling without ever tripping it.
   */
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
      const providerStream = await breaker.execute(async () =>
        selected.provider.stream({ ...request, model }),
      );

      // Usage accumulates across chunks rather than being read from the last one:
      // providers report input and output token counts in different frames, so
      // reading a single chunk yields half the total.
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

  /**
   * Look up a breaker, throwing a diagnosable error when absent.
   *
   * This replaces two non-null assertions. They were safe only because the map is built
   * from the same provider list the router selects from, which is an invariant no type
   * expresses: any future code path that returns a provider not in config would produce
   * "cannot read property of undefined" with no indication of why.
   */
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
        // The provider name is safe to include; the key value never is.
        throw new Error(`API key required for provider: ${provider.name}`);
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
