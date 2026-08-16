import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  retry,
  withRetry,
  isRetryableHttpError,
  MaxRetriesExceededError,
  RetryAbortedError,
} from '../../src/resilience/retry.js';
import type { RetryAttempt } from '../../src/resilience/retry.js';

describe('retry', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  describe('successful execution', () => {
    it('returns result on first attempt', async () => {
      const fn = vi.fn().mockResolvedValue('success');
      const result = await retry(fn, { maxRetries: 3, baseDelay: 100, maxDelay: 5000 });
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('retries and succeeds on second attempt', async () => {
      const fn = vi.fn()
        .mockRejectedValueOnce(new Error('transient'))
        .mockResolvedValue('ok');

      const promise = retry(fn, { maxRetries: 3, baseDelay: 100, maxDelay: 5000 });
      await vi.advanceTimersByTimeAsync(200);
      const result = await promise;

      expect(result).toBe('ok');
      expect(fn).toHaveBeenCalledTimes(2);
    });

    it('retries up to maxRetries before succeeding', async () => {
      const fn = vi.fn()
        .mockRejectedValueOnce(new Error('fail 1'))
        .mockRejectedValueOnce(new Error('fail 2'))
        .mockRejectedValueOnce(new Error('fail 3'))
        .mockResolvedValue('finally');

      const promise = retry(fn, { maxRetries: 3, baseDelay: 100, maxDelay: 5000 });
      await vi.advanceTimersByTimeAsync(10000);
      const result = await promise;

      expect(result).toBe('finally');
      expect(fn).toHaveBeenCalledTimes(4); // initial + 3 retries
    });
  });

  describe('exhausted retries', () => {
    it('throws MaxRetriesExceededError after all retries fail', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('always-fail'));

      const promise = retry(fn, { maxRetries: 2, baseDelay: 100, maxDelay: 5000 });
      await vi.advanceTimersByTimeAsync(10000);

      await expect(promise).rejects.toBeInstanceOf(MaxRetriesExceededError);
      expect(fn).toHaveBeenCalledTimes(3); // initial + 2 retries
    });

    it('includes last error in MaxRetriesExceededError', async () => {
      const fn = vi.fn().mockRejectedValue(new Error('root-cause'));

      const promise = retry(fn, { maxRetries: 1, baseDelay: 100, maxDelay: 5000 });
      await vi.advanceTimersByTimeAsync(10000);

      try {
        await promise;
      } catch (e) {
        const err = e as MaxRetriesExceededError;
        expect(err.lastError.message).toBe('root-cause');
        expect(err.attempts).toBe(1);
      }
    });
  });

  describe('non-retryable errors', () => {
    it('throws immediately for non-retryable errors', async () => {
      const nonRetryable = new Error('auth-failed');
      const fn = vi.fn().mockRejectedValue(nonRetryable);

      await expect(
        retry(fn, {
          maxRetries: 5,
          baseDelay: 100,
          maxDelay: 5000,
          retryable: () => false,
        }),
      ).rejects.toBe(nonRetryable);

      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('uses custom retryable predicate', async () => {
      const fn = vi.fn()
        .mockRejectedValueOnce(Object.assign(new Error('rate-limit'), { status: 429 }))
        .mockRejectedValueOnce(Object.assign(new Error('unauthorized'), { status: 401 }))
        .mockResolvedValue('ok');

      const promise = retry(fn, {
        maxRetries: 5,
        baseDelay: 100,
        maxDelay: 5000,
        retryable: (err) => 'status' in err && (err as { status: number }).status === 429,
      });

      await vi.advanceTimersByTimeAsync(500);

      // Should retry 429, then throw on 401 (non-retryable)
      await expect(promise).rejects.toThrow('unauthorized');
      expect(fn).toHaveBeenCalledTimes(2);
    });
  });

  describe('abort signal', () => {
    it('aborts immediately if signal is already aborted', async () => {
      const controller = new AbortController();
      controller.abort();

      await expect(
        retry(async () => 'ok', {
          maxRetries: 3,
          baseDelay: 100,
          maxDelay: 5000,
          signal: controller.signal,
        }),
      ).rejects.toBeInstanceOf(RetryAbortedError);
    });

    it('aborts during sleep between retries', async () => {
      const controller = new AbortController();
      const fn = vi.fn().mockRejectedValue(new Error('fail'));

      const promise = retry(fn, {
        maxRetries: 5,
        baseDelay: 1000,
        maxDelay: 5000,
        signal: controller.signal,
      });

      // Let first attempt fail, then abort during backoff sleep
      await vi.advanceTimersByTimeAsync(10);
      controller.abort();
      await vi.advanceTimersByTimeAsync(100);

      await expect(promise).rejects.toBeInstanceOf(RetryAbortedError);
    });
  });

  describe('backoff behavior', () => {
    it('delays increase with jitter (decorrelated)', async () => {
      const delays: number[] = [];
      const fn = vi.fn().mockRejectedValue(new Error('fail'));

      const onRetry = (attempt: RetryAttempt) => {
        delays.push(attempt.delay);
      };

      const promise = retry(
        fn,
        { maxRetries: 3, baseDelay: 1000, maxDelay: 30000 },
        onRetry,
      );

      await vi.advanceTimersByTimeAsync(100000);
      await promise.catch(() => {});

      // All delays should be >= baseDelay
      expect(delays.every(d => d >= 1000)).toBe(true);
      // All delays should be <= maxDelay
      expect(delays.every(d => d <= 30000)).toBe(true);
    });

    it('caps delay at maxDelay', async () => {
      const delays: number[] = [];
      const fn = vi.fn().mockRejectedValue(new Error('fail'));

      const promise = retry(
        fn,
        { maxRetries: 10, baseDelay: 1000, maxDelay: 5000 },
        (attempt) => { delays.push(attempt.delay); },
      );

      await vi.advanceTimersByTimeAsync(1000000);
      await promise.catch(() => {});

      expect(delays.every(d => d <= 5000)).toBe(true);
    });
  });

  describe('onRetry hook', () => {
    it('calls hook with attempt metadata', async () => {
      const hook = vi.fn();
      const fn = vi.fn()
        .mockRejectedValueOnce(new Error('err1'))
        .mockResolvedValue('ok');

      const promise = retry(
        fn,
        { maxRetries: 3, baseDelay: 100, maxDelay: 5000 },
        hook,
      );

      await vi.advanceTimersByTimeAsync(5000);
      await promise;

      expect(hook).toHaveBeenCalledTimes(1);
      expect(hook).toHaveBeenCalledWith(
        expect.objectContaining({
          attempt: 1,
          maxRetries: 3,
          error: expect.any(Error),
        }),
      );
    });
  });
});

describe('withRetry', () => {
  it('wraps a function with retry behavior', async () => {
    vi.useRealTimers();

    let calls = 0;
    const unstable = async (x: number) => {
      calls++;
      if (calls < 2) throw new Error('flaky');
      return x * 2;
    };

    const stable = withRetry(unstable, { maxRetries: 3, baseDelay: 10, maxDelay: 100 });
    const result = await stable(5);

    expect(result).toBe(10);
    expect(calls).toBe(2);
  });
});

describe('isRetryableHttpError', () => {
  it('returns true for network errors', () => {
    expect(isRetryableHttpError(new Error('ECONNRESET'))).toBe(true);
    expect(isRetryableHttpError(new Error('socket hang up'))).toBe(true);
    expect(isRetryableHttpError(new Error('fetch failed'))).toBe(true);
  });

  it('returns true for 429 rate limit', () => {
    const err = Object.assign(new Error('rate limited'), { status: 429 });
    expect(isRetryableHttpError(err)).toBe(true);
  });

  it('returns true for 5xx errors', () => {
    const err = Object.assign(new Error('internal'), { status: 500 });
    expect(isRetryableHttpError(err)).toBe(true);

    const err2 = Object.assign(new Error('gateway'), { status: 502 });
    expect(isRetryableHttpError(err2)).toBe(true);
  });

  it('returns false for 4xx client errors', () => {
    const err = Object.assign(new Error('not found'), { status: 404 });
    expect(isRetryableHttpError(err)).toBe(false);

    const err2 = Object.assign(new Error('unauthorized'), { status: 401 });
    expect(isRetryableHttpError(err2)).toBe(false);
  });

  it('returns false for generic errors without status', () => {
    expect(isRetryableHttpError(new Error('something broke'))).toBe(false);
  });
});
