import { describe, it, expect, beforeEach, vi } from 'vitest';
import { CircuitBreaker, CircuitOpenError } from '../../src/resilience/circuit-breaker.js';

describe('CircuitBreaker', () => {
  let breaker: CircuitBreaker;

  const defaultConfig = {
    failureThreshold: 3,
    recoveryTimeout: 1000,
    successThreshold: 2,
    monitorWindow: 5000,
  };

  beforeEach(() => {
    breaker = new CircuitBreaker(defaultConfig);
    vi.useFakeTimers();
  });

  describe('CLOSED state', () => {
    it('starts in CLOSED state', () => {
      expect(breaker.getState()).toBe('CLOSED');
    });

    it('executes functions normally', async () => {
      const result = await breaker.execute(async () => 'ok');
      expect(result).toBe('ok');
    });

    it('stays CLOSED on successes', async () => {
      for (let i = 0; i < 10; i++) {
        await breaker.execute(async () => 'ok');
      }
      expect(breaker.getState()).toBe('CLOSED');
    });

    it('counts failures within monitor window', async () => {
      const fail = async () => { throw new Error('fail'); };

      for (let i = 0; i < 2; i++) {
        await expect(breaker.execute(fail)).rejects.toThrow();
      }

      expect(breaker.getState()).toBe('CLOSED');
      expect(breaker.getFailureCount()).toBe(2);
    });

    it('transitions to OPEN after threshold failures', async () => {
      const fail = async () => { throw new Error('fail'); };

      for (let i = 0; i < 3; i++) {
        await expect(breaker.execute(fail)).rejects.toThrow('fail');
      }

      expect(breaker.getState()).toBe('OPEN');
    });

    it('prunes failures outside monitor window', async () => {
      const fail = async () => { throw new Error('fail'); };

      // 2 failures now
      await expect(breaker.execute(fail)).rejects.toThrow();
      await expect(breaker.execute(fail)).rejects.toThrow();

      // Advance past monitor window
      vi.advanceTimersByTime(6000);

      // 1 more failure should NOT open (previous 2 are pruned)
      await expect(breaker.execute(fail)).rejects.toThrow();
      expect(breaker.getState()).toBe('CLOSED');
    });
  });

  describe('OPEN state', () => {
    beforeEach(async () => {
      const fail = async () => { throw new Error('fail'); };
      for (let i = 0; i < 3; i++) {
        await breaker.execute(fail).catch(() => {});
      }
    });

    it('rejects immediately with CircuitOpenError', async () => {
      await expect(breaker.execute(async () => 'ok')).rejects.toBeInstanceOf(CircuitOpenError);
    });

    it('does not call the wrapped function', async () => {
      const fn = vi.fn().mockResolvedValue('ok');
      await breaker.execute(fn).catch(() => {});
      expect(fn).not.toHaveBeenCalled();
    });

    it('transitions to HALF_OPEN after recovery timeout', () => {
      vi.advanceTimersByTime(1000);
      expect(breaker.getState()).toBe('HALF_OPEN');
    });

    it('stays OPEN before recovery timeout', () => {
      vi.advanceTimersByTime(999);
      expect(breaker.getState()).toBe('OPEN');
    });
  });

  describe('HALF_OPEN state', () => {
    beforeEach(async () => {
      const fail = async () => { throw new Error('fail'); };
      for (let i = 0; i < 3; i++) {
        await breaker.execute(fail).catch(() => {});
      }
      vi.advanceTimersByTime(1000); // → HALF_OPEN
    });

    it('allows probe requests through', async () => {
      const result = await breaker.execute(async () => 'probe-ok');
      expect(result).toBe('probe-ok');
    });

    it('transitions to CLOSED after success threshold', async () => {
      await breaker.execute(async () => 'ok');
      await breaker.execute(async () => 'ok');
      expect(breaker.getState()).toBe('CLOSED');
    });

    it('reopens on ANY failure during probe', async () => {
      await breaker.execute(async () => 'ok'); // 1 success
      await breaker.execute(async () => { throw new Error('nope'); }).catch(() => {});
      expect(breaker.getState()).toBe('OPEN');
    });

    it('single failure resets success counter', async () => {
      await breaker.execute(async () => 'ok');
      await breaker.execute(async () => { throw new Error('fail'); }).catch(() => {});

      // Wait for recovery again
      vi.advanceTimersByTime(1000);
      expect(breaker.getState()).toBe('HALF_OPEN');

      // Need full successThreshold again
      await breaker.execute(async () => 'ok');
      expect(breaker.getState()).toBe('HALF_OPEN'); // Not yet closed
      await breaker.execute(async () => 'ok');
      expect(breaker.getState()).toBe('CLOSED');
    });
  });

  describe('reset()', () => {
    it('resets to clean CLOSED state', async () => {
      const fail = async () => { throw new Error('fail'); };
      for (let i = 0; i < 3; i++) {
        await breaker.execute(fail).catch(() => {});
      }
      expect(breaker.getState()).toBe('OPEN');

      breaker.reset();

      expect(breaker.getState()).toBe('CLOSED');
      expect(breaker.getFailureCount()).toBe(0);
    });
  });

  describe('edge cases', () => {
    it('handles concurrent executions correctly', async () => {
      const results = await Promise.all([
        breaker.execute(async () => 'a'),
        breaker.execute(async () => 'b'),
        breaker.execute(async () => 'c'),
      ]);
      expect(results).toEqual(['a', 'b', 'c']);
    });

    it('handles async errors that resolve after delay', async () => {
      const slowFail = () => new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error('slow-fail')), 100);
      });

      vi.useRealTimers(); // Need real timers for this test
      await expect(breaker.execute(slowFail)).rejects.toThrow('slow-fail');
    });
  });
});
