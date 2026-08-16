/**
 * @q1-digital/inference-gateway
 *
 * Multi-provider LLM inference routing with circuit breakers, cost-aware load
 * balancing, stream protocol normalization, and OpenTelemetry observability.
 */

// Core
export { InferenceGateway } from './core/gateway.js';
export type {
  CompletionRequest,
  CompletionResponse,
} from './core/gateway.js';

export { Router, NoHealthyProviderError } from './core/router.js';
export type { RoutingStrategy, RoutingConfig, ProviderSelection } from './core/router.js';

// Resilience
export { CircuitBreaker, CircuitOpenError } from './resilience/circuit-breaker.js';
export type { CircuitBreakerConfig, CircuitState } from './resilience/circuit-breaker.js';

export {
  retry,
  withRetry,
  isRetryableHttpError,
  MaxRetriesExceededError,
  RetryAbortedError,
} from './resilience/retry.js';
export type { RetryConfig, RetryAttempt, RetryHook } from './resilience/retry.js';

export {
  createDeadline,
  withTimeout,
  createCascadingTimeout,
  TimeoutError,
} from './resilience/timeout.js';
export type { TimeoutConfig, Deadline } from './resilience/timeout.js';

export { Bulkhead, BulkheadFullError, BulkheadTimeoutError } from './resilience/bulkhead.js';
export type { BulkheadConfig, BulkheadMetrics } from './resilience/bulkhead.js';

// Streaming
//
// StreamError is exported because pipe() throws it on upstream failure and on idle
// timeout. Without the class, a caller cannot narrow on `error.code` and is forced to
// match on the message string, which breaks the moment the wording changes.
export { StreamMultiplexer, StreamError } from './streaming/multiplexer.js';
export type {
  StreamChunk,
  StreamMetrics,
  MultiplexerOptions,
} from './streaming/multiplexer.js';

// Cost
export { CostCalculator } from './cost/calculator.js';
export type { TokenUsage, CostBreakdown } from './cost/calculator.js';

export { BudgetGuard, BudgetExceededError } from './cost/budget.js';
export type { BudgetConfig } from './cost/budget.js';

// Telemetry
export { Tracer } from './telemetry/tracer.js';
export type { TelemetryConfig, SpanWrapper } from './telemetry/tracer.js';
export { Logger } from './telemetry/logger.js';
