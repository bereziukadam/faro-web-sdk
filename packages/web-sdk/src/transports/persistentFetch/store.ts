import { TransportItemType } from '@grafana/faro-core';
import type { TransportItem } from '@grafana/faro-core';

import type { PersistentTransportDropEvent } from './types';

interface StoredEvent {
  id: number;
  identity: string;
  partition: string;
  createdAt: number;
  item: TransportItem;
}

interface QueueState {
  partition: string;
  count: number;
  attempts: number;
  nextAttemptAt: number;
  inFlight: number[];
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

/** Native IndexedDB only. All mutations resolve on transaction completion, not request success. */
export class PersistentEventStore {
  private database?: Promise<IDBDatabase>;

  constructor(
    private readonly partition: string,
    private readonly maxEvents: number,
    private readonly maxAgeMs: number,
    private readonly now: () => number
  ) {}

  private open(): Promise<IDBDatabase> {
    if (!this.database) {
      this.database = new Promise<IDBDatabase>((resolve, reject) => {
        let blocked = false;
        const request = globalThis.indexedDB.open('faro-persistent-fetch', 1);
        request.onupgradeneeded = () => {
          const db = request.result;
          const events = db.createObjectStore('events', { keyPath: 'id', autoIncrement: true });
          events.createIndex('partition', 'partition');
          events.createIndex('age', ['partition', 'createdAt']);
          db.createObjectStore('state', { keyPath: 'partition' });
        };
        request.onsuccess = () => {
          const db = request.result;
          if (blocked) {
            db.close();
            return;
          }
          db.onversionchange = () => {
            db.close();
            this.database = undefined;
          };
          resolve(db);
        };
        request.onerror = () => reject(request.error);
        request.onblocked = () => {
          blocked = true;
          reject(new Error('Persistent Faro database upgrade is blocked'));
        };
      }).catch((error) => {
        this.database = undefined;
        throw error;
      });
    }
    return this.database;
  }

  private async transaction<T>(action: (events: IDBObjectStore, state: IDBObjectStore) => Promise<T>): Promise<T> {
    const db = await this.open();
    const transaction = db.transaction(['events', 'state'], 'readwrite');
    const completion = new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error('Persistent Faro transaction aborted'));
      transaction.onerror = () => reject(transaction.error);
    });
    // Attach the rejection handler before awaiting requests to avoid an unhandled transaction rejection.
    const result = action(transaction.objectStore('events'), transaction.objectStore('state')).catch((error) => {
      try {
        transaction.abort();
      } catch {
        // The transaction may already have aborted.
      }
      throw error;
    });
    const [value] = await Promise.all([result, completion]);
    return value;
  }

  private async readState(states: IDBObjectStore): Promise<QueueState> {
    return (
      (await requestResult<QueueState | undefined>(states.get(this.partition))) ?? {
        partition: this.partition,
        count: 0,
        attempts: 0,
        nextAttemptAt: 0,
        inFlight: [],
      }
    );
  }

  private walk(index: IDBIndex, range: IDBKeyRange, visit: (cursor: IDBCursorWithValue) => boolean): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = index.openCursor(range);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) {
          resolve();
          return;
        }
        try {
          if (visit(cursor)) {
            cursor.continue();
          } else {
            resolve();
          }
        } catch (error) {
          reject(error);
        }
      };
    });
  }

  private async expire(events: IDBObjectStore, state: QueueState): Promise<number> {
    let expired = 0;
    const protectedIds = new Set(state.inFlight);
    await this.walk(
      events.index('age'),
      IDBKeyRange.bound([this.partition, -Infinity], [this.partition, this.now() - this.maxAgeMs]),
      (cursor) => {
        if (!protectedIds.has((cursor.value as StoredEvent).id)) {
          cursor.delete();
          expired++;
        }
        return true;
      }
    );
    state.count -= expired;
    return expired;
  }

  async enqueue(items: TransportItem[]): Promise<PersistentTransportDropEvent[]> {
    return this.transaction(async (events, states) => {
      const state = await this.readState(states);
      const expired = await this.expire(events, state);
      const protectedIds = new Set(state.inFlight);
      let overflow = Math.max(0, state.count + items.length - this.maxEvents);
      let evicted = 0;
      if (overflow > 0) {
        await this.walk(events.index('partition'), IDBKeyRange.only(this.partition), (cursor) => {
          if (!protectedIds.has((cursor.value as StoredEvent).id)) {
            cursor.delete();
            overflow--;
            evicted++;
          }
          return overflow > 0;
        });
      }
      const accepted = items.slice(overflow);
      const createdAt = this.now();
      for (const item of accepted) {
        events.add({ partition: this.partition, identity: globalThis.crypto.randomUUID(), createdAt, item });
      }
      state.count += accepted.length - evicted;
      states.put(state);
      return [
        { reason: 'expired', count: expired },
        { reason: 'capacity', count: evicted + Math.min(overflow, items.length) },
      ];
    });
  }

  /** Caller must own the partition's Web Lock. A new owner recovers stale in-flight markers. */
  async claim(limit: number): Promise<{ events: StoredEvent[]; expired: number; corrupt: number }> {
    return this.transaction(async (events, states) => {
      const state = await this.readState(states);
      state.inFlight = [];
      const expired = await this.expire(events, state);
      const selected: StoredEvent[] = [];
      let corrupt = 0;
      if (state.nextAttemptAt <= this.now() && state.count > 0) {
        let meta: string | undefined;
        await this.walk(events.index('partition'), IDBKeyRange.only(this.partition), (cursor) => {
          const event = cursor.value as StoredEvent;
          if (
            typeof event.identity !== 'string' ||
            !Number.isFinite(event.createdAt) ||
            !event.item ||
            !Object.values(TransportItemType).includes(event.item.type) ||
            !event.item.meta ||
            typeof event.item.meta !== 'object' ||
            !event.item.payload ||
            typeof event.item.payload !== 'object'
          ) {
            cursor.delete();
            state.count--;
            corrupt++;
            return true;
          }
          const eventMeta = JSON.stringify(event.item.meta);
          if (selected.length > 0 && eventMeta !== meta) {
            return false;
          }
          meta = eventMeta;
          selected.push(event);
          return selected.length < limit;
        });
      }
      state.inFlight = selected.map((event) => event.id);
      states.put(state);
      return { events: selected, expired, corrupt };
    });
  }

  async acknowledge(ids: number[]): Promise<void> {
    await this.transaction(async (events, states) => {
      const state = await this.readState(states);
      for (const id of ids) {
        events.delete(id);
      }
      states.put({
        partition: this.partition,
        count: state.count - ids.length,
        attempts: 0,
        nextAttemptAt: 0,
        inFlight: [],
      });
    });
  }

  async retry(initialMs: number, maxMs: number, random: number, retryAfterMs = 0): Promise<void> {
    await this.transaction(async (_events, states) => {
      const state = (await requestResult<QueueState>(states.get(this.partition)))!;
      const delay = Math.min(maxMs, initialMs * 2 ** Math.min(state.attempts, 30));
      state.attempts++;
      state.nextAttemptAt = this.now() + Math.max(delay * (0.5 + random * 0.5), retryAfterMs);
      state.inFlight = [];
      states.put(state);
    });
  }

  async close(): Promise<void> {
    (await this.database)?.close();
    this.database = undefined;
  }
}
