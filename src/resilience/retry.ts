/**
 * Retry with exponential backoff and decorrelated jitter.
 *
 * Strategy: AWS-style decorrelated jitter (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
 * This avoids thundering herd when multiple clients retry simultaneously.
 *
 * Features:
 *   - Configurable max retries, base delay, max delay
 *   - Decorrelated jitter (not full jitter) for better spread
 *   - Retryable error classification via predicate
 *   - Abort signal support for cancellation
 *   - Attempt metadata passed to caller for observability
 */

export interface RetryConfig {
  /** Maximum number of retry attempts (excludes initial call) */
  maxRetries: number;
  /** Base delay in milliseconds */
  baseDelay: number;
  /** Maximum delay cap in milliseconds */
  maxDelay: number;
  /** Multiplier for exponential growth (default: 2) */
  multiplier?: number;
  /** Predicate to determine if an error is retryable */
  retryable?: (error: Error) => boolean;
  /** Optional abort signal for cancellation */
  signal?: AbortSignal;
}

export interface RetryAttempt {
  attempt: number;
  maxRetries: number;
  delay: number;
  error: Error;
  elapsed: number;
}

export type RetryHook = (attempt: RetryAttempt) => void | Promise<void>;

const DEFAULT_CONFIG: Required<Omit<RetryConfig, 'signal'>> = {
  maxRetries: 3,
  baseDelay: 1000,
  maxDelay: 30_000,
  multiplier: 2,
  retryable: () => true,
};

/**
 * Executes a function with exponential backoff retry.
 *
 * Uses decorrelated jitter: each delay is random between baseDelay
 * and previousDelay * 3, capped at maxDelay. This produces better
 * spread than full jitter while still avoiding thundering herd.
 */
export async function retry<T>(
  fn: (attempt: number) => Promise<T>,
  config: RetryConfig,
  onRetry?: RetryHook,
): Promise<T> {
  const opts = { ...DEFAULT_CONFIG, ...config };
  const startTime = performance.now();
  let lastDelay = opts.baseDelay;

  for (let attempt = 0; attempt <= opts.maxRetries; attempt++) {
    try {
      if (config.signal?.aborted) {
        throw new RetryAbortedError('Retry aborted by signal');
      }
      return await fn(attempt);
    } catch (error) {
      const err = error as Error;

      // Don't retry if this is the last attempt
      if (attempt >= opts.maxRetries) {
        throw new MaxRetriesExceededError(
          `All ${opts.maxRetries} retries exhausted: ${err.message}`,
          err,
          attempt,
        );
      }

      // Don't retry non-retryable errors
      if (!opts.retryable(err)) {
        throw err;
      }

      // Check abort before sleeping
      if (config.signal?.aborted) {
        throw new RetryAbortedError('Retry aborted by signal');
      }

      // Decorrelated jitter: delay = random_between(baseDelay, lastDelay * 3)
      const jitteredDelay = randomBetween(opts.baseDelay, lastDelay * 3);
      const cappedDelay = Math.min(jitteredDelay, opts.maxDelay);
      lastDelay = cappedDelay;

      const attemptInfo: RetryAttempt = {
        attempt: attempt + 1,
        maxRetries: opts.maxRetries,
        delay: cappedDelay,
        error: err,
        elapsed: performance.now() - startTime,
      };

      if (onRetry) {
        await onRetry(attemptInfo);
      }

      await sleep(cappedDelay, config.signal);
    }
  }

  // TypeScript: unreachable but satisfies the compiler
  throw new Error('Unreachable');
}

/**
 * Creates a retryable wrapper around any async function.
 * Useful for wrapping provider calls with consistent retry behavior.
 */
export function withRetry<TArgs extends unknown[], TResult>(
  fn: (...args: TArgs) => Promise<TResult>,
  config: RetryConfig,
  onRetry?: RetryHook,
): (...args: TArgs) => Promise<TResult> {
  return (...args: TArgs) => retry(() => fn(...args), config, onRetry);
}

/**
 * Predicate: retries on network errors and 5xx/429 status codes.
 * Suitable for HTTP-based LLM provider calls.
 */
export function isRetryableHttpError(error: Error): boolean {
  const message = error.message.toLowerCase();

  // Network errors
  if (
    message.includes('econnreset') ||
    message.includes('econnrefused') ||
    message.includes('etimedout') ||
    message.includes('socket hang up') ||
    message.includes('fetch failed')
  ) {
    return true;
  }

  // Rate limits (always retry)
  if ('status' in error && (error as { status: number }).status === 429) {
    return true;
  }

  // Server errors (5xx)
  if ('status' in error) {
    const status = (error as { status: number }).status;
    return status >= 500 && status < 600;
  }

  return false;
}

function randomBetween(min: number, max: number): number {
  return min + Math.random() * (max - min);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new RetryAbortedError('Retry aborted by signal'));
      return;
    }

    const timer = setTimeout(resolve, ms);

    signal?.addEventListener('abort', () => {
      clearTimeout(timer);
      reject(new RetryAbortedError('Retry aborted by signal'));
    }, { once: true });
  });
}

export class MaxRetriesExceededError extends Error {
  readonly code = 'MAX_RETRIES_EXCEEDED';

  constructor(
    message: string,
    public readonly lastError: Error,
    public readonly attempts: number,
  ) {
    super(message);
    this.name = 'MaxRetriesExceededError';
  }
}

export class RetryAbortedError extends Error {
  readonly code = 'RETRY_ABORTED';

  constructor(message: string) {
    super(message);
    this.name = 'RetryAbortedError';
  }
}
