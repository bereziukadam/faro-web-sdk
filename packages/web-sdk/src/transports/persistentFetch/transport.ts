import { VERSION } from '@grafana/faro-core';
import type { Meta, Metas, TransportItem } from '@grafana/faro-core';

import { FetchTransportBase } from '../fetch/base';

import { PersistentEventStore } from './store';
import type { PersistentFetchTransportOptions, PersistentTransportDropEvent } from './types';

async function digest(value: string): Promise<string> {
  const bytes = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer`);
  }
  return value;
}

/** Persists individual signals before batching and sending them to the Faro collector. */
export class PersistentFetchTransport extends FetchTransportBase {
  readonly name = '@grafana/faro-web-sdk:transport-persistent-fetch';
  readonly version: string = VERSION;

  private readonly maxEvents: number;
  private readonly maxAgeMs: number;
  private readonly batchSize: number;
  private readonly initialBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly timeoutMs: number;
  private readonly controller: AbortController;
  private readonly timer: ReturnType<typeof setInterval>;
  private store?: PersistentEventStore;
  private partition?: string;
  private writes?: Promise<void>;
  private pending: TransportItem[] = [];
  private draining?: Promise<void>;
  private disposed = false;
  private reportedStorageError = false;
  private observedMetas?: Metas;
  private sessionId?: string;
  private readonly onMetaChange = (meta: Meta) => {
    if (meta.session?.id !== this.sessionId) {
      this.sessionId = meta.session?.id;
      this.wake();
    }
  };
  private readonly abortFromCaller: () => void;
  private readonly wake = () => {
    void this.flush();
  };

  constructor(private readonly persistentOptions: PersistentFetchTransportOptions) {
    const controller = new AbortController();
    super({
      ...persistentOptions,
      requestOptions: { ...persistentOptions.requestOptions, signal: controller.signal },
    });
    this.controller = controller;
    if (!persistentOptions.persistence.namespace.trim()) {
      throw new Error('persistence.namespace must not be empty');
    }
    this.maxEvents = positive(persistentOptions.persistence.maxEvents ?? 10000, 'persistence.maxEvents');
    this.maxAgeMs = positive(persistentOptions.persistence.maxEventAgeMs ?? 86400000, 'persistence.maxEventAgeMs');
    this.batchSize = positive(persistentOptions.batching?.maxEvents ?? 50, 'batching.maxEvents');
    this.initialBackoffMs = positive(persistentOptions.retry?.initialBackoffMs ?? 1000, 'retry.initialBackoffMs');
    this.maxBackoffMs = positive(persistentOptions.retry?.maxBackoffMs ?? 60000, 'retry.maxBackoffMs');
    this.timeoutMs = positive(persistentOptions.requestTimeoutMs ?? 10000, 'requestTimeoutMs');
    const interval = positive(persistentOptions.batching?.sendIntervalMs ?? 1000, 'batching.sendIntervalMs');
    this.abortFromCaller = () => controller.abort(persistentOptions.requestOptions?.signal?.reason);
    persistentOptions.requestOptions?.signal?.addEventListener('abort', this.abortFromCaller, { once: true });
    if (persistentOptions.requestOptions?.signal?.aborted) {
      this.abortFromCaller();
    }
    this.timer = setInterval(this.wake, interval);
    if (typeof globalThis.addEventListener === 'function') {
      globalThis.addEventListener('online', this.wake);
      globalThis.addEventListener('pageshow', this.wake);
    }
    // Initialization injects the Faro instance synchronously. Replay does not require new events.
    void Promise.resolve().then(this.wake);
  }

  override isBatched(): boolean {
    // Bypass the SDK's volatile batch buffer; network batching happens after persistence.
    return false;
  }

  override send(items: TransportItem | TransportItem[]): Promise<void> {
    if (this.disposed) {
      return Promise.resolve();
    }
    const snapshot: TransportItem[] = [];
    for (const item of Array.isArray(items) ? items : [items]) {
      try {
        snapshot.push(JSON.parse(JSON.stringify(item)) as TransportItem);
      } catch {
        this.reportDrop({ reason: 'serialization', count: 1 });
      }
    }
    this.pending.push(...snapshot);
    // Coalesce arrivals while a transaction is pending; every signal still has its own database record.
    this.writes ??= Promise.resolve().then(async () => {
      try {
        const store = await this.getStore();
        while (this.pending.length > 0) {
          const batch = this.pending;
          this.pending = [];
          const dropped = await store.enqueue(batch);
          dropped.forEach((event) => this.reportDrop(event));
          this.reportedStorageError = false;
        }
      } catch (error) {
        this.pending = [];
        this.storageError(error);
      } finally {
        this.writes = undefined;
      }
    });
    return this.writes;
  }

  /** Attempts all currently eligible events. Failed or throttled events remain persisted. */
  async flush(): Promise<void> {
    if (!this.disposed && !this.observedMetas && this.metas.addListener) {
      this.observedMetas = this.metas;
      this.sessionId = this.metas.value.session?.id;
      this.metas.addListener(this.onMetaChange);
    }
    if (this.disposed || this.controller.signal.aborted || this.config.paused || !this.metas.value) {
      return;
    }
    if (this.draining) {
      return this.draining;
    }
    this.draining = this.drain()
      .catch((error) => this.storageError(error))
      .finally(() => {
        this.draining = undefined;
      });
    return this.draining;
  }

  private async drain(): Promise<void> {
    await this.writes;
    const store = await this.getStore();
    if (typeof navigator === 'undefined' || !navigator.locks) {
      throw new Error('PersistentFetchTransport requires Web Locks for safe cross-context delivery');
    }
    await navigator.locks.request(`faro-persistent-fetch:${this.partition}`, { ifAvailable: true }, async (lock) => {
      if (!lock) {
        return;
      }
      while (!this.disposed && !this.controller.signal.aborted && !this.config.paused) {
        const batch = await store.claim(this.batchSize);
        this.reportDrop({ reason: 'expired', count: batch.expired });
        this.reportDrop({ reason: 'corrupt', count: batch.corrupt });
        if (batch.events.length === 0) {
          return;
        }
        const ids = batch.events.map((event) => event.id);
        try {
          const prepared = await this.prepareWithDeadline(batch.events.map((event) => event.item));
          // Stable for an identical event set across retries and browser restarts.
          const identity = await digest(JSON.stringify([this.partition, batch.events.map((event) => event.identity)]));
          (prepared.requestInit.headers as Record<string, string>)['Idempotency-Key'] = identity;
          if (this.disposed || this.controller.signal.aborted || this.config.paused) {
            return;
          }
          const result = await this.performAttempt(prepared, 2, false);
          if (result.kind === 'success') {
            await store.acknowledge(ids);
            continue;
          }
          // Keep all rejected requests until age/capacity eviction, including authentication failures.
          await store.retry(
            this.initialBackoffMs,
            this.maxBackoffMs,
            this.persistentOptions.getRandom?.() ?? Math.random(),
            result.kind === 'retry' ? result.retryAfterMs : undefined
          );
          return;
        } catch {
          await store.retry(
            this.initialBackoffMs,
            this.maxBackoffMs,
            this.persistentOptions.getRandom?.() ?? Math.random()
          );
          return;
        }
      }
    });
  }

  private async prepareWithDeadline(items: TransportItem[]) {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        this.prepareRequest(items),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error('Persistent Faro request preparation timed out')), this.timeoutMs);
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async getStore(): Promise<PersistentEventStore> {
    if (!this.store) {
      const partition = await digest(JSON.stringify([this.persistentOptions.persistence.namespace, this.options.url]));
      this.partition = partition;
      this.store ??= new PersistentEventStore(partition, this.maxEvents, this.maxAgeMs, this.getNow);
    }
    return this.store;
  }

  private reportDrop(event: PersistentTransportDropEvent): void {
    if (event.count === 0) {
      return;
    }
    this.logWarn('Persistent transport dropped events', event);
    try {
      this.persistentOptions.onDrop?.(event);
    } catch {
      this.logError('Persistent transport onDrop callback failed');
    }
  }

  private storageError(error: unknown): void {
    if (this.reportedStorageError) {
      return;
    }
    this.reportedStorageError = true;
    this.logError('Persistent transport storage unavailable; unpersisted events will not be sent');
    try {
      this.persistentOptions.onStorageError?.(error);
    } catch {
      this.logError('Persistent transport onStorageError callback failed');
    }
  }

  /** Stops delivery, waits for accepted writes, and retains the queue for the next instance/session. */
  async dispose(): Promise<void> {
    this.disposed = true;
    clearInterval(this.timer);
    this.observedMetas?.removeListener(this.onMetaChange);
    this.controller.abort();
    this.persistentOptions.requestOptions?.signal?.removeEventListener('abort', this.abortFromCaller);
    if (typeof globalThis.removeEventListener === 'function') {
      globalThis.removeEventListener('online', this.wake);
      globalThis.removeEventListener('pageshow', this.wake);
    }
    await this.writes;
    await this.draining;
    await this.store?.close();
  }
}
