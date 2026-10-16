import { createPromiseBuffer, VERSION } from '@grafana/faro-core';
import type { PromiseBuffer, PromiseProducer, TransportItem } from '@grafana/faro-core';

import { FetchTransportBase } from './base';
import { ReliableDeliveryQueue } from './deliveryQueue';
import type { DeliveryReservation } from './deliveryQueue';
import type { FetchTransportOptions } from './types';

const DEFAULT_BUFFER_SIZE = 30;
const DEFAULT_CONCURRENCY = 5;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_INITIAL_BACKOFF_MS = 1000;
const DEFAULT_MAX_BACKOFF_MS = 30000;
const DEFAULT_BACKOFF_MULTIPLIER = 2;
export class FetchTransport extends FetchTransportBase {
  readonly name = '@grafana/faro-web-sdk:transport-fetch';
  readonly version: string = VERSION;

  /** Compatibility access to the transport's bounded task queue. Tasks added directly are not retried. */
  promiseBuffer: PromiseBuffer<Response | void>;

  private readonly defaultPromiseBuffer: PromiseBuffer<Response | void>;
  private readonly defaultBufferAdd: PromiseBuffer<Response | void>['add'];
  private readonly customPromiseBuffer: PromiseBuffer<Response | void>;
  private pendingCustomSends = 0;
  private readonly deliveryQueue: ReliableDeliveryQueue;

  constructor(options: FetchTransportOptions) {
    super(options);

    this.deliveryQueue = new ReliableDeliveryQueue({
      bufferSize: options.bufferSize ?? DEFAULT_BUFFER_SIZE,
      concurrency: options.concurrency ?? DEFAULT_CONCURRENCY,
      retry: {
        maxAttempts: options.retry?.maxAttempts ?? DEFAULT_MAX_ATTEMPTS,
        initialBackoffMs:
          options.retry?.initialBackoffMs ?? options.defaultRateLimitBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
        maxBackoffMs: options.retry?.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS,
        backoffMultiplier: options.retry?.backoffMultiplier ?? DEFAULT_BACKOFF_MULTIPLIER,
      },
      getNow: this.getNow,
      getRandom: options.getRandom ?? Math.random,
      onRetry: (delayMs, nextAttempt) => {
        this.logDebug(`Retrying failed request after ${delayMs}ms. Attempt ${nextAttempt}.`);
      },
    });

    this.customPromiseBuffer = createPromiseBuffer({
      size: options.bufferSize ?? DEFAULT_BUFFER_SIZE,
      concurrency: options.concurrency ?? DEFAULT_CONCURRENCY,
    });
    this.defaultPromiseBuffer = {
      add: (producer) => {
        // A replacement may wrap or asynchronously forward a producer through
        // the original buffer. Keep that outer scheduling separate from the
        // delivery workers it awaits, so a one-worker buffer cannot deadlock.
        if (
          this.promiseBuffer !== this.defaultPromiseBuffer ||
          this.promiseBuffer.add !== this.defaultBufferAdd ||
          this.pendingCustomSends > 0
        ) {
          return this.customPromiseBuffer.add(producer);
        }
        const reservation = this.deliveryQueue.reserve();
        if (!reservation) {
          throw new Error('Task buffer full');
        }
        return this.runBufferedTask(reservation, producer);
      },
    };
    this.defaultBufferAdd = this.defaultPromiseBuffer.add;
    this.promiseBuffer = this.defaultPromiseBuffer;

    if (typeof window !== 'undefined') {
      window.addEventListener('pagehide', () => this.deliveryQueue.flush());
      window.addEventListener('pageshow', (event) => {
        if (event.persisted) {
          this.deliveryQueue.resume();
        }
      });
    }
  }

  async send(items: TransportItem[]): Promise<void> {
    const buffer = this.promiseBuffer;
    if (buffer === this.defaultPromiseBuffer && buffer.add === this.defaultBufferAdd) {
      await this.deliver(items);
      return;
    }

    this.pendingCustomSends++;
    try {
      await buffer.add(() => this.deliver(items));
    } catch (error) {
      this.logError('Permanent delivery failure', { error, attempts: 0, elapsedTimeMs: 0 });
    } finally {
      this.pendingCustomSends--;
    }
  }

  private async runBufferedTask(
    reservation: DeliveryReservation,
    producer: PromiseProducer<Response | void>
  ): Promise<Response | void> {
    let result: Response | void = undefined;
    try {
      await reservation.deliver(async () => {
        result = await producer();
        return { kind: 'success', attemptsMade: 1 };
      });
      return result;
    } finally {
      reservation.release();
    }
  }

  private async deliver(items: TransportItem[]): Promise<void> {
    const reservation = this.deliveryQueue.reserve();
    if (!reservation) {
      this.logError('Permanent delivery failure', {
        error: 'Reliable delivery queue is full',
        attempts: 0,
        elapsedTimeMs: 0,
      });
      return;
    }

    try {
      const prepared = await this.prepareRequest(items);
      const outcome = await reservation.deliver((attemptsRemaining, unloading) =>
        this.performAttempt(prepared, attemptsRemaining, unloading)
      );
      if (outcome.kind === 'terminal') {
        this.logError(
          outcome.reason === 'retries-exhausted' ? 'Delivery retries exhausted' : 'Permanent delivery failure',
          {
            ...outcome.failure,
            reason: outcome.reason,
            attempts: outcome.attempts,
            elapsedTimeMs: outcome.elapsedTimeMs,
          }
        );
      }
    } catch (error) {
      this.logError('Permanent delivery failure', { error, attempts: 0, elapsedTimeMs: 0 });
    } finally {
      reservation.release();
    }
  }

  override isBatched(): boolean {
    return true;
  }
}
