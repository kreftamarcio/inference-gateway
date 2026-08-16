# inference-gateway

> Multi-provider LLM inference routing with circuit breakers, cost-aware load balancing, streaming multiplexer, automatic fallback, and native OpenTelemetry observability.

[![TypeScript](https://img.shields.io/badge/TypeScript-5.4-blue.svg)](https://www.typescriptlang.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20+-green.svg)](https://nodejs.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Client Request                         │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Request Interceptor                         │
│  ┌─────────┐ ┌──────────┐ ┌───────────┐ ┌───────────┐  │
│  │  Auth   │ │  Rate    │ │  Request  │ │  Budget   │  │
│  │  Layer  │ │  Limiter │ │  Validator│ │  Guard    │  │
│  └─────────┘ └──────────┘ └───────────┘ └───────────┘  │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│                  Router Core                             │
│  ┌──────────────────────────────────────────────────┐   │
│  │           Cost-Aware Load Balancer                │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌──────────┐  │   │
│  │  │Latency │ │ Cost   │ │Quality │ │Throughput│  │   │
│  │  │Scoring │ │Scoring │ │Scoring │ │ Scoring  │  │   │
│  │  └────────┘ └────────┘ └────────┘ └──────────┘  │   │
│  └──────────────────────────────────────────────────┘   │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Provider Pool                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ OpenAI   │ │Anthropic │ │  Google  │ │  Groq    │   │
│  │          │ │          │ │  Gemini  │ │          │   │
│  │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │   │
│  │ │Circuit│ │ │ │Circuit│ │ │ │Circuit│ │ │ │Circuit│ │   │
│  │ │Breaker│ │ │ │Breaker│ │ │ │Breaker│ │ │ │Breaker│ │   │
│  │ └──────┘ │ │ └──────┘ │ │ └──────┘ │ │ └──────┘ │   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘   │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│            Streaming Multiplexer                         │
│  ┌─────────────┐ ┌──────────────┐ ┌─────────────────┐   │
│  │  SSE/WS     │ │  Backpressure│ │  Token Counter  │   │
│  │  Adapter    │ │  Controller  │ │  & Accumulator  │   │
│  └─────────────┘ └──────────────┘ └─────────────────┘   │
└─────────────────────┬───────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────┐
│              Observability Layer                          │
│  ┌───────────┐ ┌───────────┐ ┌────────────────────────┐ │
│  │  Traces   │ │  Metrics  │ │  Structured Logging    │ │
│  │  (OTel)   │ │  (OTel)   │ │  (Pino)               │ │
│  └───────────┘ └───────────┘ └────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Features

### Multi-Provider Routing
- **Weighted round-robin** with health-aware promotion/demotion
- **Least-latency** routing based on rolling P95 windows
- **Cost-optimized** routing with per-model token pricing
- **Capability-based** routing (vision, function calling, long context)

### Circuit Breaker (per provider)
- Three states: `CLOSED` → `OPEN` → `HALF_OPEN`
- Configurable failure threshold, recovery timeout, and success threshold
- Exponential backoff on repeated failures
- Health probe during half-open state

### Streaming Multiplexer
- Unified SSE/WebSocket interface regardless of provider protocol
- Token-level streaming with backpressure control
- Automatic reconnection with resume tokens
- Client-side buffering with configurable flush intervals

### Cost Engine
- Real-time cost calculation per request (input + output tokens)
- Budget enforcement with soft/hard limits
- Cost forecasting based on historical usage patterns
- Per-tenant, per-model, and per-endpoint cost attribution

### Observability (OpenTelemetry Native)
- Distributed traces spanning full request lifecycle
- Metrics: latency histograms, token throughput, error rates, cost
- Structured JSON logging with correlation IDs
- Custom spans for routing decisions and circuit breaker transitions

## Installation

```bash
npm install @q1-digital/inference-gateway
```

## Quick Start

```typescript
import { InferenceGateway } from '@q1-digital/inference-gateway';

const gateway = new InferenceGateway({
  providers: [
    {
      name: 'openai',
      apiKey: process.env.OPENAI_API_KEY!,
      models: ['gpt-4o', 'gpt-4o-mini'],
      weight: 0.6,
      circuitBreaker: {
        failureThreshold: 5,
        recoveryTimeout: 30_000,
        successThreshold: 3,
      },
    },
    {
      name: 'anthropic',
      apiKey: process.env.ANTHROPIC_API_KEY!,
      models: ['claude-sonnet-4-20250514', 'claude-haiku-4-20250514'],
      weight: 0.3,
    },
    {
      name: 'groq',
      apiKey: process.env.GROQ_API_KEY!,
      models: ['llama-3.3-70b-versatile'],
      weight: 0.1,
      priority: 'low-latency',
    },
  ],
  routing: {
    strategy: 'cost-optimized', // 'round-robin' | 'least-latency' | 'cost-optimized'
    fallbackChain: ['openai', 'anthropic', 'groq'],
  },
  budget: {
    daily: 50.00,
    monthly: 1200.00,
    alertThreshold: 0.8,
  },
  telemetry: {
    serviceName: 'inference-gateway',
    exporterEndpoint: process.env.OTEL_EXPORTER_ENDPOINT,
  },
});

// Non-streaming
const response = await gateway.complete({
  messages: [{ role: 'user', content: 'Explain quantum computing' }],
  model: 'auto', // Let the router decide
  maxTokens: 1024,
});

// Streaming
const stream = gateway.stream({
  messages: [{ role: 'user', content: 'Write a poem' }],
  model: 'auto',
});

for await (const chunk of stream) {
  process.stdout.write(chunk.delta);
}
```

## Configuration

```typescript
interface GatewayConfig {
  providers: ProviderConfig[];
  routing: RoutingConfig;
  budget?: BudgetConfig;
  telemetry?: TelemetryConfig;
  middleware?: MiddlewareConfig[];
  retry?: RetryConfig;
}

interface ProviderConfig {
  name: string;
  apiKey: string;
  baseUrl?: string;
  models: string[];
  weight?: number;
  priority?: 'low-latency' | 'low-cost' | 'high-quality';
  circuitBreaker?: CircuitBreakerConfig;
  rateLimit?: { rpm: number; tpm: number };
  timeout?: number;
}

interface CircuitBreakerConfig {
  failureThreshold: number;    // Failures before opening
  recoveryTimeout: number;     // Ms before half-open
  successThreshold: number;    // Successes to close
  monitorWindow: number;       // Rolling window size
}
```

## Project Structure

```
src/
├── core/
│   ├── gateway.ts              # Main gateway orchestrator
│   ├── router.ts               # Routing strategy engine
│   └── config.ts               # Configuration validation (Zod)
├── providers/
│   ├── base.provider.ts        # Abstract provider interface
│   ├── openai.provider.ts      # OpenAI implementation
│   ├── anthropic.provider.ts   # Anthropic implementation
│   ├── google.provider.ts      # Google Gemini implementation
│   └── groq.provider.ts        # Groq implementation
├── resilience/
│   ├── circuit-breaker.ts      # Circuit breaker state machine
│   ├── retry.ts                # Exponential backoff with jitter
│   ├── timeout.ts              # Request timeout management
│   └── bulkhead.ts             # Concurrency isolation
├── streaming/
│   ├── multiplexer.ts          # Stream protocol adapter
│   ├── backpressure.ts         # Flow control
│   └── accumulator.ts          # Token counting & buffering
├── cost/
│   ├── calculator.ts           # Per-request cost computation
│   ├── budget.ts               # Budget enforcement
│   └── pricing.ts              # Model pricing registry
├── telemetry/
│   ├── tracer.ts               # OpenTelemetry trace setup
│   ├── metrics.ts              # Prometheus-compatible metrics
│   └── logger.ts               # Structured logging (Pino)
├── middleware/
│   ├── auth.ts                 # API key / JWT validation
│   ├── rate-limiter.ts         # Per-client rate limiting
│   └── request-validator.ts    # Input schema validation
└── index.ts                    # Public API exports
```

## Benchmarks

| Metric | Value |
|--------|-------|
| Routing decision latency | < 2ms P99 |
| Streaming first-byte overhead | < 5ms |
| Circuit breaker state transition | < 1ms |
| Memory per active connection | ~2.4 KB |
| Max concurrent streams | 10,000+ |

## Roadmap

- [ ] Adaptive routing with reinforcement learning
- [ ] Response caching with semantic similarity
- [ ] Multi-region provider pools
- [ ] WebSocket provider support (real-time APIs)
- [ ] Plugin system for custom routing strategies
- [ ] Dashboard UI for monitoring and configuration

## License

MIT
