/**
 * Structured logging with Pino.
 *
 * Design choices:
 *   - JSON output in production, pretty-print in development
 *   - Request-scoped context (requestId, provider, model)
 *   - Child loggers for module isolation
 *   - Redaction of sensitive fields (apiKey, authorization)
 */

import pino from 'pino';
import type { Logger as PinoLogger } from 'pino';

export interface LogContext {
  requestId?: string;
  provider?: string;
  model?: string;
  [key: string]: unknown;
}

export class Logger {
  private readonly logger: PinoLogger;

  constructor(serviceName: string, options?: { level?: string; pretty?: boolean }) {
    const isDev = process.env.NODE_ENV !== 'production';

    this.logger = pino({
      name: serviceName,
      level: options?.level ?? (isDev ? 'debug' : 'info'),
      timestamp: pino.stdTimeFunctions.isoTime,
      redact: {
        paths: ['*.apiKey', '*.authorization', '*.token', 'config.providers[*].apiKey'],
        censor: '[REDACTED]',
      },
      ...(isDev || options?.pretty
        ? { transport: { target: 'pino-pretty', options: { colorize: true } } }
        : {}),
    });
  }

  info(message: string, context?: LogContext): void {
    this.logger.info(context ?? {}, message);
  }

  warn(message: string, context?: LogContext): void {
    this.logger.warn(context ?? {}, message);
  }

  error(message: string, error?: Error, context?: LogContext): void {
    this.logger.error(
      { ...context, err: error ? { message: error.message, stack: error.stack, name: error.name } : undefined },
      message,
    );
  }

  debug(message: string, context?: LogContext): void {
    this.logger.debug(context ?? {}, message);
  }

  /**
   * Creates a child logger with bound context.
   * Useful for request-scoped logging.
   */
  child(bindings: LogContext): Logger {
    const child = Object.create(this) as Logger;
    (child as { logger: PinoLogger }).logger = this.logger.child(bindings);
    return child;
  }

  /**
   * Flush remaining log entries. Call before process exit.
   */
  async flush(): Promise<void> {
    return new Promise((resolve) => {
      this.logger.flush(() => resolve());
    });
  }
}
