/**
 * Bulkhead pattern: concurrency isolation per provider.
 *
 * Prevents a single slow or misbehaving provider from consuming all available
 * connections/threads. Each provider gets an isolated execution slot pool.
 *
 * Design:
 *   - Semaphore-based concurrency limiter (no external deps)
 *   - Queue with configurable max size and timeout
 *   - Fairness: FIFO ordering for queued requests
 *   - Metrics: active count, queue depth, rejection count
 */

export interface BulkheadConfig {
  /** Maximum concurrent executions */
  maxConcurrent: number;
  /** Maximum queue size (requests waiting for a slot) */
  maxQueue: number;
  /** Maximum time a request can wait in queue (ms) */
  queueTimeout: number;
}

export interface BulkheadMetrics {
  active: number;
  queued: number;
  rejected: number;
  completed: number;
  averageWaitTime: number;
}

interface QueueEntry {
  resolve: () => void;
  reject: (error: Error) => void;
  enqueuedAt: number;
  timer: ReturnType<typeof setTimeout>;
}

export class Bulkhead {
  private active = 0;
  private readonly queue: QueueEntry[] = [];
  private rejected = 0;
  private completed = 0;
  private totalWaitTime = 0;

  constructor(private readonly config: BulkheadConfig) {
    if (config.maxConcurrent < 1) {
      throw new Error('maxConcurrent must be >= 1');
    }
    if (config.maxQueue < 0) {
      throw new Error('maxQueue must be >= 0');
    }
  }

  /**
   * Execute a function within the bulkhead.
   * If max concurrency is reached, the call queues (up to maxQueue).
   * If the queue is full, rejects immediately.
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    await this.acquireSlot();

    try {
      const result = await fn();
      this.completed++;
      return result;
    } finally {
      this.releaseSlot();
    }
  }

  getMetrics(): BulkheadMetrics {
    return {
      active: this.active,
      queued: this.queue.length,
      rejected: this.rejected,
      completed: this.completed,
      averageWaitTime: this.completed > 0
        ? this.totalWaitTime / this.completed
        : 0,
    };
  }

  /**
   * Drain the bulkhead: reject all queued requests.
   * Active executions continue to completion.
   */
  drain(): void {
    while (this.queue.length > 0) {
      const entry = this.queue.shift()!;
      clearTimeout(entry.timer);
      entry.reject(new BulkheadDrainedError('Bulkhead drained'));
    }
  }

  private async acquireSlot(): Promise<void> {
    // Fast path: slot available
    if (this.active < this.config.maxConcurrent) {
      this.active++;
      return;
    }

    // Queue is full: reject immediately
    if (this.queue.length >= this.config.maxQueue) {
      this.rejected++;
      throw new BulkheadFullError(
        `Bulkhead full: ${this.active} active, ${this.queue.length} queued`,
      );
    }

    // Queue the request
    return new Promise<void>((resolve, reject) => {
      const enqueuedAt = Date.now();

      const timer = setTimeout(() => {
        // Remove from queue on timeout
        const index = this.queue.findIndex(e => e.resolve === resolve);
        if (index !== -1) {
          this.queue.splice(index, 1);
        }
        this.rejected++;
        reject(new BulkheadTimeoutError(
          `Queued for ${this.config.queueTimeout}ms without acquiring slot`,
        ));
      }, this.config.queueTimeout);

      this.queue.push({ resolve, reject, enqueuedAt, timer });
    });
  }

  private releaseSlot(): void {
    this.active--;

    // Dequeue next waiting request (FIFO)
    if (this.queue.length > 0) {
      const next = this.queue.shift()!;
      clearTimeout(next.timer);

      const waitTime = Date.now() - next.enqueuedAt;
      this.totalWaitTime += waitTime;

      this.active++;
      next.resolve();
    }
  }
}

export class BulkheadFullError extends Error {
  readonly code = 'BULKHEAD_FULL';

  constructor(message: string) {
    super(message);
    this.name = 'BulkheadFullError';
  }
}

export class BulkheadTimeoutError extends Error {
  readonly code = 'BULKHEAD_TIMEOUT';

  constructor(message: string) {
    super(message);
    this.name = 'BulkheadTimeoutError';
  }
}

export class BulkheadDrainedError extends Error {
  readonly code = 'BULKHEAD_DRAINED';

  constructor(message: string) {
    super(message);
    this.name = 'BulkheadDrainedError';
  }
}
