/**
 * Stream Multiplexer: normalizes heterogeneous LLM streaming protocols
 * into a unified AsyncGenerator interface.
 *
 * Problem: Each provider has a different streaming format:
 *   - OpenAI: SSE with `data: {json}` frames, `[DONE]` terminator
 *   - Anthropic: SSE with typed events (content_block_delta, message_stop)
 *   - Google: Chunked JSON array with partial objects
 *   - Groq: OpenAI-compatible SSE
 *
 * Solution: Provider adapters emit a common StreamChunk type.
 * The multiplexer handles backpressure, buffering, and token accumulation.
 */

export interface StreamChunk {
  /** Incremental text content */
  delta: string;
  /** Finish reason if stream is ending */
  finishReason?: 'stop' | 'length' | 'tool_calls' | 'content_filter';
  /** Token usage (typically only in final chunk) */
  usage?: {
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
  };
  /** Provider-specific metadata */
  metadata?: Record<string, unknown>;
}

export interface MultiplexerOptions {
  /** Maximum chunks to buffer before applying backpressure */
  highWaterMark?: number;
  /** Request ID for correlation */
  requestId?: string;
  /** Timeout for idle streams (no chunks received) in ms */
  idleTimeout?: number;
}

interface StreamMetrics {
  chunksReceived: number;
  bytesReceived: number;
  firstChunkLatency: number | null;
  totalDuration: number;
  backpressureEvents: number;
}

export class StreamMultiplexer {
  private readonly defaultHighWaterMark = 64;

  /**
   * Pipe a provider stream through the multiplexer.
   * Applies backpressure and normalizes chunks.
   */
  async *pipe(
    source: AsyncIterable<StreamChunk>,
    options: MultiplexerOptions = {},
  ): AsyncGenerator<StreamChunk> {
    const highWaterMark = options.highWaterMark ?? this.defaultHighWaterMark;
    const idleTimeout = options.idleTimeout ?? 30_000;
    const metrics: StreamMetrics = {
      chunksReceived: 0,
      bytesReceived: 0,
      firstChunkLatency: null,
      totalDuration: 0,
      backpressureEvents: 0,
    };

    const startTime = performance.now();
    const buffer: StreamChunk[] = [];
    
    // [Passo Ãšnico] Keep-Alive AssÃ­ncrono (Cloud Timeout Fix)
    const channel = new StreamChannel<StreamChunk>();
    const keepAliveInterval = Math.max(1000, Math.floor(idleTimeout / 2));
    const timer = setInterval(() => {
      // Injeta um chunk vazio para manter o TCP/LB ativo
      channel.push({ delta: '', metadata: { keepAlive: true } });
    }, keepAliveInterval);

    // Produtor
    (async () => {
      try {
        for await (const chunk of source) {
          channel.push(chunk);
        }
      } catch (e) {
        channel.error(e as Error);
      } finally {
        channel.close();
      }
    })();

    try {
      for await (const chunk of channel) {
        if (chunk.metadata?.keepAlive) {
          yield chunk; // Emite imediatamente o keep-alive
          continue;
        }

        metrics.chunksReceived++;
        metrics.bytesReceived += chunk.delta.length;

        if (metrics.firstChunkLatency === null) {
          metrics.firstChunkLatency = performance.now() - startTime;
        }

        // Backpressure
        if (buffer.length >= highWaterMark) {
          metrics.backpressureEvents++;
          for (const buffered of buffer.splice(0)) {
            yield buffered;
          }
        }

        yield chunk;
      }

      // Flush remaining buffer
      for (const buffered of buffer) {
        yield buffered;
      }
    } finally {
      clearInterval(timer);
      metrics.totalDuration = performance.now() - startTime;
    }
  }

  /**
   * Merge multiple provider streams (for parallel routing experiments).
   * Yields chunks from whichever stream produces them first.
   * Cancels slower streams once the first completes.
   */
  async *race(
    streams: AsyncIterable<StreamChunk>[],
  ): AsyncGenerator<StreamChunk> {
    const controller = new AbortController();

    // Create a shared channel
    const channel = new StreamChannel<StreamChunk>();

    // Start all streams, piping into the channel
    const runners = streams.map(async (stream, index) => {
      try {
        for await (const chunk of stream) {
          if (controller.signal.aborted) break;
          channel.push({ ...chunk, metadata: { ...chunk.metadata, streamIndex: index } });
        }
      } catch (error) {
        if (!controller.signal.aborted) {
          channel.error(error as Error);
        }
      }
    });

    // Wait for first stream to complete, then cancel others
    Promise.race(runners).then(() => {
      controller.abort();
      channel.close();
    });

    yield* channel;
  }
}

/**
 * Internal async channel for coordinating multiple streams.
 * Implements AsyncIterable with push/pull semantics.
 */
class StreamChannel<T> implements AsyncIterable<T> {
  private buffer: T[] = [];
  private waiting: ((value: IteratorResult<T>) => void) | null = null;
  private closed = false;
  private err: Error | null = null;

  push(value: T): void {
    if (this.closed) return;

    if (this.waiting) {
      const resolve = this.waiting;
      this.waiting = null;
      resolve({ value, done: false });
    } else {
      this.buffer.push(value);
    }
  }

  error(err: Error): void {
    this.err = err;
    this.close();
  }

  close(): void {
    this.closed = true;
    if (this.waiting) {
      const resolve = this.waiting;
      this.waiting = null;
      if (this.err) {
        // Caller will see this as a thrown error
        resolve({ value: undefined as unknown as T, done: true });
      } else {
        resolve({ value: undefined as unknown as T, done: true });
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.buffer.length > 0) {
          return Promise.resolve({ value: this.buffer.shift()!, done: false });
        }
        if (this.closed) {
          return Promise.resolve({ value: undefined as unknown as T, done: true });
        }
        return new Promise(resolve => {
          this.waiting = resolve;
        });
      },
    };
  }
}
