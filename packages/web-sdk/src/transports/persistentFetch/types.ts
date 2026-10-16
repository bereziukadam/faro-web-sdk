import type { FetchTransportOptions } from '../fetch';

export interface PersistentFetchTransportOptions extends Pick<
  FetchTransportOptions,
  'url' | 'apiKey' | 'requestOptions' | 'requestCompression' | 'getNow' | 'getRandom'
> {
  persistence: {
    /** Stable across sessions; use a different namespace for each application/environment. */
    namespace: string;
    /** Maximum queued transport items (a trace item may contain several spans). Default: 10000. */
    maxEvents?: number;
    /** Maximum time since enqueue, in milliseconds. Default: 24 hours. */
    maxEventAgeMs?: number;
  };
  batching?: {
    /** Maximum transport items per HTTP request. Default: 50. */
    maxEvents?: number;
    /** Delay between queue checks, in milliseconds. Default: 1000. */
    sendIntervalMs?: number;
  };
  retry?: {
    /** Default: 1000 milliseconds. */
    initialBackoffMs?: number;
    /** Caps exponential backoff, but never a server's Retry-After. Default: 60000 milliseconds. */
    maxBackoffMs?: number;
  };
  /** Maximum duration of one HTTP attempt. Default: 10000 milliseconds. */
  requestTimeoutMs?: number;
  /** Receives counts only; telemetry contents are never passed to diagnostics. */
  onDrop?: (event: PersistentTransportDropEvent) => void;
  onStorageError?: (error: unknown) => void;
}

export interface PersistentTransportDropEvent {
  reason: 'expired' | 'capacity' | 'serialization' | 'corrupt';
  count: number;
}
