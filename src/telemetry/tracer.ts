/**
 * OpenTelemetry tracing integration.
 *
 * Provides distributed tracing across the gateway lifecycle:
 *   - Request span: full lifecycle from receipt to response
 *   - Routing span: provider selection decision
 *   - Provider span: individual provider call
 *   - Circuit breaker transitions as span events
 *
 * Exports to any OTLP-compatible backend (Jaeger, Tempo, Honeycomb, etc.)
 */

import { trace, SpanStatusCode, context, propagation } from '@opentelemetry/api';
import type { Span, SpanOptions, Tracer as OTelTracer, Context } from '@opentelemetry/api';

export interface TelemetryConfig {
  serviceName?: string;
  exporterEndpoint?: string;
  sampleRate?: number;
  enabled?: boolean;
}

export interface SpanWrapper {
  setAttribute(key: string, value: string | number | boolean): void;
  setStatus(status: { code: number; message?: string }): void;
  addEvent(name: string, attributes?: Record<string, string | number>): void;
  end(): void;
}

export class Tracer {
  private readonly tracer: OTelTracer;
  private readonly enabled: boolean;

  constructor(config?: TelemetryConfig) {
    this.enabled = config?.enabled !== false;
    const serviceName = config?.serviceName ?? 'inference-gateway';
    this.tracer = trace.getTracer(serviceName, '0.1.0');
  }

  /**
   * Start a new span. Returns a wrapper that gracefully no-ops if tracing is disabled.
   */
  startSpan(name: string, attributes?: Record<string, string | number | boolean>): SpanWrapper {
    if (!this.enabled) {
      return this.noopSpan();
    }

    const options: SpanOptions = {
      attributes: attributes as Record<string, string | number | boolean | undefined>,
    };

    const span = this.tracer.startSpan(name, options);
    return this.wrapSpan(span);
  }

  /**
   * Execute a function within a traced span.
   * Automatically sets error status and records exceptions.
   */
  async withSpan<T>(
    name: string,
    attributes: Record<string, string | number | boolean>,
    fn: (span: SpanWrapper) => Promise<T>,
  ): Promise<T> {
    const span = this.startSpan(name, attributes);

    try {
      const result = await fn(span);
      span.setStatus({ code: 0 });
      return result;
    } catch (error) {
      span.setStatus({ code: 2, message: (error as Error).message });
      span.addEvent('exception', {
        'exception.type': (error as Error).name,
        'exception.message': (error as Error).message,
      });
      throw error;
    } finally {
      span.end();
    }
  }

  /**
   * Extract trace context from incoming headers (for distributed tracing).
   */
  extractContext(headers: Record<string, string>): Context {
    return propagation.extract(context.active(), headers);
  }

  private wrapSpan(span: Span): SpanWrapper {
    return {
      setAttribute(key: string, value: string | number | boolean): void {
        span.setAttribute(key, value);
      },
      setStatus(status: { code: number; message?: string }): void {
        span.setStatus({
          code: status.code === 0 ? SpanStatusCode.OK : SpanStatusCode.ERROR,
          message: status.message,
        });
      },
      addEvent(name: string, attributes?: Record<string, string | number>): void {
        span.addEvent(name, attributes);
      },
      end(): void {
        span.end();
      },
    };
  }

  private noopSpan(): SpanWrapper {
    return {
      setAttribute: () => {},
      setStatus: () => {},
      addEvent: () => {},
      end: () => {},
    };
  }
}
