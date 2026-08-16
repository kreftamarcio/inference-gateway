/**
 * Stream multiplexer: normalizes heterogeneous LLM streaming protocols into one
 * AsyncGenerator interface, with real backpressure and error propagation.
 *
 * Provider formats differ:
 *   OpenAI, Groq  SSE frames `data: {json}`, terminated by `data: [DONE]`
 *   Anthropic     SSE with typed events; usage is SPLIT across message_start
 *                 (input tokens) and message_delta (output tokens)
 *   Google        chunked JSON array rather than SSE
 *
 * Provider adapters emit `StreamChunk`; this module handles flow control,
 * cancellation, and failure propagation so orchestration code has no provider
 * branching and no way to mistake a truncated stream for a complete one.
 */

export interface StreamChunk {
  /** Incremental text content. */
  delta: string;
  /** Present on the terminal chunk. */
  finishReason?: 'stop' | 'length' | 'tool_calls' | 'content_filter' | 'error';
  /** Token usage. Providers report this at different points in the stream. */
  usage?: {
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
  };
  metadata?: Record<string, unknown>;
}

export interface StreamMetrics {
  chunksReceived: number;
  bytesReceived: number;
  /** Time to first non-empty delta. The latency users actually perceive. */
  firstChunkLatencyMs: number | null;
  totalDurationMs: number;
  /** Times the producer was forced to wait for the consumer. */
  backpressureEvents: number;
  keepAlivesSent: number;
}

export interface MultiplexerOptions {
  /**
   * Chunks buffered before the producer is made to wait.
   *
   * This is the actual backpressure mechanism: past this depth the channel stops
   * accepting and the producer awaits the consumer, so a fast provider cannot
   * outrun a slow reader and exhaust memory.
   */
  highWaterMark?: number;
  requestId?: string;
  /**
   * Abort if no chunk arrives within this window.
   *
   * Distinct from a total request deadline: a stream can stall indefinitely with the
   * connection technically open, and an overall timeout only catches that after the
   * full duration has elapsed.
   */
  idleTimeoutMs?: number;
  /**
   * Emit a keep-alive at this interval to stop an intermediary from closing an idle
   * connection.
   *
   * Delivered through this callback rather than as a chunk. Injecting a synthetic
   * chunk into the data stream corrupts anything that counts chunks or concatenates
   * deltas.
   */
  keepAliveMs?: number;
  onKeepAlive?: () => void;
  onComplete?: (metrics: StreamMetrics) => void;
  signal?: AbortSignal;
}

export class StreamError extends Error {
  constructor(
    readonly code: 'IDLE_TIMEOUT' | 'UPSTREAM_ERROR' | 'ABORTED',
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
    this.name = 'StreamError';
  }
}

const DEFAULT_HIGH_WATER_MARK = 64;
const DEFAULT_IDLE_TIMEOUT_MS = 30_000;

export class StreamMultiplexer {
  /**
   * Pipe a provider stream through the multiplexer.
   *
   * Failure semantics: an upstream error is rethrown as a StreamError rather than
   * being converted into a normal completion. This is the difference between a caller
   * knowing its response was truncated and silently persisting half an answer.
   */
  async *pipe(
    source: AsyncIterable<StreamChunk>,
    options: MultiplexerOptions = {},
  ): AsyncGenerator<StreamChunk> {
    const highWaterMark = Math.max(1, options.highWaterMark ?? DEFAULT_HIGH_WATER_MARK);
    const idleTimeoutMs = options.idleTimeoutMs ?? DEFAULT_IDLE_TIMEOUT_MS;

    const metrics: StreamMetrics = {
      chunksReceived: 0,
      bytesReceived: 0,
      firstChunkLatencyMs: null,
      totalDurationMs: 0,
      backpressureEvents: 0,
      keepAlivesSent: 0,
    };

    const startedAt = performance.now();
    const channel = new StreamChannel<StreamChunk>(highWaterMark);

    // Keep-alive is a side channel. It never becomes a chunk, so a consumer cannot
    // mistake it for output.
    let keepAliveTimer: ReturnType<typeof setInterval> | undefined;
    if (options.keepAliveMs !== undefined && options.onKeepAlive) {
      keepAliveTimer = setInterval(() => {
        metrics.keepAlivesSent++;
        options.onKeepAlive?.();
      }, Math.max(1000, options.keepAliveMs));
    }

    // The producer awaits channel.push when the buffer is full, which is what makes
    // backpressure real rather than a condition that is never true.
    const producer = (async (): Promise<void> => {
      try {
        for await (const chunk of source) {
          if (options.signal?.aborted) break;
          const waited = await channel.push(chunk);
          if (waited) metrics.backpressureEvents++;
        }
        channel.close();
      } catch (error) {
        channel.fail(error);
      }
    })();

    try {
      for await (const chunk of channel.consume(idleTimeoutMs, options.signal)) {
        metrics.chunksReceived++;
        metrics.bytesReceived += chunk.delta.length;

        if (metrics.firstChunkLatencyMs === null && chunk.delta.length > 0) {
          metrics.firstChunkLatencyMs = performance.now() - startedAt;
        }

        yield chunk;
      }
    } finally {
      if (keepAliveTimer) clearInterval(keepAliveTimer);

      // Awaited so a producer rejection cannot surface as an unhandled rejection
      // after the generator has already returned.
      channel.close();
      await producer.catch(() => undefined);

      metrics.totalDurationMs = performance.now() - startedAt;
      options.onComplete?.(metrics);
    }
  }

  /**
   * Race several streams and consume the one that produces first.
   *
   * Useful for latency-sensitive routing when paying N providers for one answer is
   * acceptable. Losers are cancelled as soon as a winner is established.
   *
   * A winner is the first stream to yield a chunk, not the first to COMPLETE. Racing
   * on completion would favour whichever provider finishes fastest, which is usually
   * the one that returned the least content, and an immediate empty completion would
   * beat every real answer.
   */
  async *race(
    streams: ReadonlyArray<AsyncIterable<StreamChunk>>,
    options: { signal?: AbortSignal; onWinner?: (index: number) => void } = {},
  ): AsyncGenerator<StreamChunk> {
    if (streams.length === 0) {
      throw new Error('race requires at least one stream');
    }

    const controller = new AbortController();
    const abort = (): void => controller.abort();
    options.signal?.addEventListener('abort', abort, { once: true });

    let winner = -1;
    const channel = new StreamChannel<StreamChunk>(DEFAULT_HIGH_WATER_MARK);
    const failures: unknown[] = [];

    const runners = streams.map(async (stream, index) => {
      try {
        for await (const chunk of stream) {
          if (controller.signal.aborted && index !== winner) return;

          if (winner === -1) {
            winner = index;
            options.onWinner?.(index);
            // Cancel the losers now that a producer has actually delivered.
            controller.abort();
          }

          if (index !== winner) return;

          await channel.push({
            ...chunk,
            metadata: { ...chunk.metadata, streamIndex: index },
          });
        }
      } catch (error) {
        // A loser's failure is irrelevant once a winner exists. Only the winner's
        // failure is the caller's problem.
        if (index === winner) throw error;
        failures.push(error);
      }
    });

    // Explicitly awaited rather than left floating, so a winner rejection propagates
    // instead of becoming an unhandled rejection.
    void Promise.allSettled(runners).then((results) => {
      const winnerResult = winner >= 0 ? results[winner] : undefined;

      if (winnerResult?.status === 'rejected') {
        channel.fail(winnerResult.reason);
        return;
      }

      // Every stream failed, so there is no answer to return. Reporting normal
      // completion here would hand the caller an empty string as if it were valid.
      if (winner === -1) {
        channel.fail(
          new StreamError(
            'UPSTREAM_ERROR',
            `All ${streams.length} raced stream(s) failed without producing a chunk`,
            failures,
          ),
        );
        return;
      }

      channel.close();
    });

    try {
      yield* channel.consume(DEFAULT_IDLE_TIMEOUT_MS, options.signal);
    } finally {
      controller.abort();
      options.signal?.removeEventListener('abort', abort);
    }
  }

  /**
   * Collect a stream into a single result.
   *
   * Retains the timing figures, which are the reason to stream even when buffering:
   * time to first token is invisible to a non-streaming call and is the number users
   * perceive.
   */
  async collect(
    source: AsyncIterable<StreamChunk>,
    options: MultiplexerOptions = {},
  ): Promise<{
    content: string;
    usage: { inputTokens: number; outputTokens: number; totalTokens: number };
    finishReason: string;
    metrics: StreamMetrics;
  }> {
    let content = '';
    let usage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };
    let finishReason = 'stop';
    let captured: StreamMetrics | undefined;

    for await (const chunk of this.pipe(source, {
      ...options,
      onComplete: (m) => {
        captured = m;
        options.onComplete?.(m);
      },
    })) {
      content += chunk.delta;

      // Merged field by field rather than replaced: Anthropic reports input tokens in
      // one frame and output tokens in another, so assignment would discard half.
      if (chunk.usage) {
        usage = {
          inputTokens: chunk.usage.inputTokens || usage.inputTokens,
          outputTokens: chunk.usage.outputTokens || usage.outputTokens,
          totalTokens: chunk.usage.totalTokens || usage.totalTokens,
        };
      }

      if (chunk.finishReason) finishReason = chunk.finishReason;
    }

    if (usage.totalTokens === 0) {
      usage.totalTokens = usage.inputTokens + usage.outputTokens;
    }

    return {
      content,
      usage,
      finishReason,
      metrics:
        captured ?? {
          chunksReceived: 0,
          bytesReceived: 0,
          firstChunkLatencyMs: null,
          totalDurationMs: 0,
          backpressureEvents: 0,
          keepAlivesSent: 0,
        },
    };
  }
}

/**
 * Bounded async channel with a waiter queue.
 *
 * Two properties the previous implementation lacked:
 *
 *   - A queue of waiters rather than a single slot. One slot meant a second concurrent
 *     consumer overwrote the first's resolver, and that consumer hung forever.
 *   - Errors are thrown, not converted to completion. A stored error that never
 *     surfaces makes a truncated stream look successful.
 */
class StreamChannel<T> {
  private readonly buffer: T[] = [];
  private readonly waiters: Array<{
    resolve: (result: IteratorResult<T>) => void;
    reject: (error: unknown) => void;
  }> = [];

  /** Producers parked because the buffer is at capacity. */
  private readonly drainWaiters: Array<() => void> = [];

  private closed = false;
  private failure: unknown;

  constructor(private readonly highWaterMark: number) {}

  /**
   * Push a value, awaiting the consumer when the buffer is full.
   *
   * Resolves true when the producer had to wait, which is what makes a
   * backpressure count meaningful instead of a number that is always zero.
   */
  async push(value: T): Promise<boolean> {
    if (this.closed) return false;

    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.resolve({ value, done: false });
      return false;
    }

    this.buffer.push(value);

    if (this.buffer.length < this.highWaterMark) return false;

    await new Promise<void>((resolve) => this.drainWaiters.push(resolve));
    return true;
  }

  fail(error: unknown): void {
    if (this.closed) return;

    this.failure = error;
    this.closed = true;

    // Rejected, not resolved. This is the fix for the swallowed-error defect.
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(this.wrap(error));
    }
    this.releaseProducers();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;

    // Buffered values still drain: closing must not discard what was already
    // produced and accepted.
    if (this.buffer.length === 0) {
      for (const waiter of this.waiters.splice(0)) {
        waiter.resolve({ value: undefined as unknown as T, done: true });
      }
    }
    this.releaseProducers();
  }

  /**
   * Consume with a per-chunk idle deadline.
   *
   * The timer is created and cleared per iteration rather than once, because the
   * deadline applies to the gap BETWEEN chunks. A single timer for the whole stream
   * would be a total duration limit, which is a different guarantee.
   */
  async *consume(idleTimeoutMs: number, signal?: AbortSignal): AsyncGenerator<T> {
    for (;;) {
      if (signal?.aborted) {
        throw new StreamError('ABORTED', 'Stream consumption was aborted by the caller');
      }

      if (this.buffer.length > 0) {
        const value = this.buffer.shift()!;
        this.releaseProducers();
        yield value;
        continue;
      }

      if (this.failure !== undefined) throw this.wrap(this.failure);
      if (this.closed) return;

      let timer: ReturnType<typeof setTimeout> | undefined;

      const next = new Promise<IteratorResult<T>>((resolve, reject) => {
        this.waiters.push({ resolve, reject });
      });

      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(
          () =>
            reject(
              new StreamError(
                'IDLE_TIMEOUT',
                `No chunk for ${idleTimeoutMs}ms. The stream stalled while the connection ` +
                  'remained open.',
              ),
            ),
          idleTimeoutMs,
        );
      });

      try {
        const result = await Promise.race([next, timeout]);
        if (result.done) return;
        yield result.value;
      } finally {
        // Always cleared. A surviving timer keeps the event loop alive and delays
        // process exit by the full idle window.
        if (timer) clearTimeout(timer);
      }
    }
  }

  private releaseProducers(): void {
    if (this.buffer.length >= this.highWaterMark && !this.closed) return;
    for (const release of this.drainWaiters.splice(0)) release();
  }

  private wrap(error: unknown): StreamError {
    if (error instanceof StreamError) return error;

    return new StreamError(
      'UPSTREAM_ERROR',
      `Provider stream failed: ${error instanceof Error ? error.message : String(error)}`,
      error,
    );
  }
}
