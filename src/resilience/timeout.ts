/**
 * Deadline propagation and timeout management.
 *
 * Implements cascading deadlines: an outer timeout encompasses the entire
 * request lifecycle, while inner timeouts apply to individual provider calls.
 * This prevents a slow provider from consuming the entire budget.
 *
 * Key design decisions:
 *   - Deadline (absolute timestamp) > Duration (relative ms) for composability
 *   - AbortController integration for cancellation propagation
 *   - Remaining time calculation for nested calls
 */

export interface TimeoutConfig {
  /** Total request timeout in milliseconds */
  requestTimeout: number;
  /** Per-provider call timeout in milliseconds */
  providerTimeout: number;
  /** Connection establishment timeout (TCP handshake) */
  connectTimeout?: number;
}

export interface Deadline {
  /** Absolute timestamp when this deadline expires */
  expiresAt: number;
  /** AbortSignal that fires when deadline is reached */
  signal: AbortSignal;
  /** Remaining milliseconds until expiry */
  remaining(): number;
  /** Whether the deadline has already expired */
  isExpired(): boolean;
  /** Create a child deadline bounded by this one */
  child(maxMs: number): Deadline;
  /** Clean up timers */
  dispose(): void;
}

/**
 * Creates a deadline that expires after the given duration.
 * The returned signal will abort when the deadline is reached.
 */
export function createDeadline(timeoutMs: number): Deadline {
  const controller = new AbortController();
  const expiresAt = Date.now() + timeoutMs;

  const timer = setTimeout(() => {
    controller.abort(new TimeoutError(`Deadline exceeded after ${timeoutMs}ms`));
  }, timeoutMs);

  // Prevent timer from keeping the process alive
  if (typeof timer === 'object' && 'unref' in timer) {
    timer.unref();
  }

  const deadline: Deadline = {
    expiresAt,
    signal: controller.signal,

    remaining(): number {
      return Math.max(0, expiresAt - Date.now());
    },

    isExpired(): boolean {
      return Date.now() >= expiresAt;
    },

    child(maxMs: number): Deadline {
      // Child deadline is bounded by parent's remaining time
      const childTimeout = Math.min(maxMs, this.remaining());
      if (childTimeout <= 0) {
        // Parent already expired, create immediately-aborted deadline
        const childController = new AbortController();
        childController.abort(new TimeoutError('Parent deadline already expired'));
        return {
          expiresAt: Date.now(),
          signal: childController.signal,
          remaining: () => 0,
          isExpired: () => true,
          child: () => this.child(0),
          dispose: () => {},
        };
      }
      return createDeadline(childTimeout);
    },

    dispose(): void {
      clearTimeout(timer);
    },
  };

  return deadline;
}

/**
 * Wraps an async operation with a timeout.
 * If the operation doesn't complete before the deadline, it throws TimeoutError
 * and signals abort to the operation (if it respects AbortSignal).
 */
export async function withTimeout<T>(
  fn: (signal: AbortSignal) => Promise<T>,
  timeoutMs: number,
): Promise<T> {
  const deadline = createDeadline(timeoutMs);

  try {
    if (deadline.isExpired()) {
      throw new TimeoutError(`Timeout of ${timeoutMs}ms already expired`);
    }

    const result = await Promise.race([
      fn(deadline.signal),
      rejectOnAbort(deadline.signal, timeoutMs),
    ]);

    return result as T;
  } finally {
    deadline.dispose();
  }
}

/**
 * Applies cascading timeouts for a multi-step request.
 * The request deadline bounds all individual provider attempts.
 */
export function createCascadingTimeout(config: TimeoutConfig): {
  requestDeadline: Deadline;
  getProviderTimeout: () => number;
} {
  const requestDeadline = createDeadline(config.requestTimeout);

  return {
    requestDeadline,
    getProviderTimeout(): number {
      // Provider timeout is bounded by remaining request time
      return Math.min(config.providerTimeout, requestDeadline.remaining());
    },
  };
}

function rejectOnAbort(signal: AbortSignal, timeoutMs: number): Promise<never> {
  return new Promise((_, reject) => {
    if (signal.aborted) {
      reject(new TimeoutError(`Operation timed out after ${timeoutMs}ms`));
      return;
    }
    signal.addEventListener('abort', () => {
      reject(new TimeoutError(`Operation timed out after ${timeoutMs}ms`));
    }, { once: true });
  });
}

export class TimeoutError extends Error {
  readonly code = 'TIMEOUT';

  constructor(message: string) {
    super(message);
    this.name = 'TimeoutError';
  }
}
