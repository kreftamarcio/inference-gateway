/**
 * Gateway configuration contracts.
 *
 * This file is the compile-time boundary between user config and the rest of the
 * library. Gateway.ts imported it before it existed; without it the package does
 * not typecheck.
 *
 * Adapters are injected, not selected by provider name. Built-in OpenAI/Anthropic
 * adapters are still on the roadmap. Until they land, a caller must supply an
 * adapter — MockProvider is enough to exercise routing, budgets, and streaming.
 */

import type { CircuitBreakerConfig } from '../resilience/circuit-breaker.js';
import type { BudgetConfig } from '../cost/budget.js';
import type { TelemetryConfig } from '../telemetry/tracer.js';
import type { RoutingConfig } from './router.js';
import type { StreamChunk } from '../streaming/multiplexer.js';

export interface ProviderCompleteResult {
  content: string;
  usage: {
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
  };
}

export interface ProviderRequest {
  messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }>;
  model: string;
  maxTokens?: number;
  temperature?: number;
}

export interface ProviderAdapter {
  complete(request: ProviderRequest): Promise<ProviderCompleteResult>;
  stream(request: ProviderRequest): Promise<AsyncIterable<StreamChunk>>;
}

export interface ProviderConfig {
  name: string;
  apiKey: string;
  models: string[];
  weight?: number;
  priority?: 'low-latency' | 'low-cost' | 'high-quality';
  circuitBreaker?: CircuitBreakerConfig;
  rateLimit?: { rpm: number; tpm: number };
  timeout?: number;
  /** Required until built-in adapters ship. */
  adapter: ProviderAdapter;
}

export interface GatewayConfig {
  providers: ProviderConfig[];
  routing: RoutingConfig;
  budget?: BudgetConfig;
  telemetry?: TelemetryConfig;
}
