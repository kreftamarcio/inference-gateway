/**
 * Circuit Breaker implementation following the state machine pattern.
 *
 * States:
 *   CLOSED  → Normal operation. Failures are counted.
 *   OPEN    → Requests fail immediately. Timer runs for recovery.
 *   HALF_OPEN → Limited requests pass to test recovery.
 *
 * Transitions:
 *   CLOSED → OPEN: When failure count exceeds threshold within monitor window.
 *   OPEN → HALF_OPEN: After recovery timeout elapses.
 *   HALF_OPEN → CLOSED: After success threshold consecutive successes.
 *   HALF_OPEN → OPEN: On any failure during probe.
 */

export interface CircuitBreakerConfig {
  failureThreshold: number;
  recoveryTimeout: number;
  successThreshold: number;
  monitorWindow: number;
}

export type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private failures: number[] = []; // timestamps of failures
  private consecutiveSuccesses = 0;
  private lastFailureTime = 0;
  private openedAt = 0;

  constructor(private readonly config: CircuitBreakerConfig) {}

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    this.evaluateState();

    if (this.state === 'OPEN') {
      throw new CircuitOpenError(
        `Circuit is OPEN. Recovery in ${this.remainingRecoveryTime()}ms`,
      );
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  getState(): CircuitState {
    this.evaluateState();
    return this.state;
  }

  getFailureCount(): number {
    this.pruneOldFailures();
    return this.failures.length;
  }

  reset(): void {
    this.state = 'CLOSED';
    this.failures = [];
    this.consecutiveSuccesses = 0;
    this.lastFailureTime = 0;
    this.openedAt = 0;
  }

  private onSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.consecutiveSuccesses++;
      if (this.consecutiveSuccesses >= this.config.successThreshold) {
        this.transitionTo('CLOSED');
      }
    }
    // In CLOSED state, successes don't change anything
  }

  private onFailure(): void {
    const now = Date.now();
    this.lastFailureTime = now;

    if (this.state === 'HALF_OPEN') {
      // Any failure in half-open immediately reopens
      this.transitionTo('OPEN');
      return;
    }

    if (this.state === 'CLOSED') {
      this.failures.push(now);
      this.pruneOldFailures();

      if (this.failures.length >= this.config.failureThreshold) {
        this.transitionTo('OPEN');
      }
    }
  }

  private evaluateState(): void {
    if (this.state === 'OPEN') {
      const elapsed = Date.now() - this.openedAt;
      if (elapsed >= this.config.recoveryTimeout) {
        this.transitionTo('HALF_OPEN');
      }
    }
  }

  private transitionTo(newState: CircuitState): void {
    const previousState = this.state;
    this.state = newState;

    switch (newState) {
      case 'OPEN':
        this.openedAt = Date.now();
        this.consecutiveSuccesses = 0;
        break;
      case 'HALF_OPEN':
        this.consecutiveSuccesses = 0;
        break;
      case 'CLOSED':
        this.failures = [];
        this.consecutiveSuccesses = 0;
        this.openedAt = 0;
        break;
    }
  }

  private pruneOldFailures(): void {
    const cutoff = Date.now() - this.config.monitorWindow;
    this.failures = this.failures.filter(t => t > cutoff);
  }

  private remainingRecoveryTime(): number {
    const elapsed = Date.now() - this.openedAt;
    return Math.max(0, this.config.recoveryTimeout - elapsed);
  }
}

export class CircuitOpenError extends Error {
  readonly code = 'CIRCUIT_OPEN';

  constructor(message: string) {
    super(message);
    this.name = 'CircuitOpenError';
  }
}
