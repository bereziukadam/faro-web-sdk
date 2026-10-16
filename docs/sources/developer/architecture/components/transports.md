# Transports

Transports are the final data processors in the Faro architecture. They are responsible for doing something with the
data once it has been collected by the instrumentations and processed by the internal API.

The core library does not provide any transports out of the box. They are either provided by wrapper packages like
`web-sdk` or by the user.

## Fetch delivery and session identity

The default Fetch transport sends each request with an `X-Faro-Session-Id` header identifying
the session in its payload. Async header resolution, compression, queueing, and retries do not
replace that identity with the current session.

A collector `202` response with `X-Faro-Session-Status: invalid` renews the session only if the
request's session is still current in memory and storage. Delayed responses cannot renew a newer
session, including one another tab has established.

The transport retries transient network failures and HTTP 408, 425, 429, 500, 502, 503, and 504
responses. Defaults are three total attempts, a ten-second request timeout, and exponential
backoff starting at one second and capped at thirty seconds. Each batch retains its body and
`Idempotency-Key` across attempts. Collector CORS policies must allow `Idempotency-Key` before
this SDK version is deployed. The header alone does not guarantee server-side deduplication.

Package-root `FetchTransport` imports and constructor options remain supported. Advanced callers can
set `retry` and `requestTimeoutMs` when constructing a transport. The deprecated
`defaultRateLimitBackoffMs` option aliases `retry.initialBackoffMs`; an explicit
`retry.initialBackoffMs` takes precedence. It now controls retry backoff rather than a global
cooldown that drops intervening events.

Remove `experimental.fetchTransportV2` from configuration: reliable Fetch is now unconditional.
The former `fetch-v2` module paths have been removed. Import `FetchTransport` and its public option
types from `@grafana/faro-web-sdk` instead. This is a breaking change for callers using the flag or
removed deep imports; there is no legacy transport fallback.

The default `promiseBuffer.add()` shares admission and concurrency with delivery. Tasks submitted
directly through that API run once; transport requests use the retry policy. Custom buffer replacements
and decorators retain their own outer scheduling so waiting for delivery cannot deadlock their worker.

## Transports SDK

The transports SDK is the internal handler for the transports component. It is responsible for keeping track of the
initialized transports as well as adding others, removing existing ones, pausing them etc.

Methods and properties:

- `add()` - adds a new transport
- `addBeforeSendHook()` - adds a hook that is called before a signal is sent to each transport
- `getBeforeSendHooks()` - returns the list of hooks that are called before a signal is sent to each transport
- `execute()` - sends a signal to each registered transport
- `isPaused()` - returns whether the transports are paused or not
- `pause()` - pauses the transports
- `remove()` - removes a specific transport
- `removeBeforeSendHooks()` - removes a specific hook
- `transports` - accesses the current list of registered transports
- `unpause()` - unpauses the transports

## Persistent Fetch transport

`PersistentFetchTransport` replaces `FetchTransport` when telemetry must survive page reloads and
network failures. It uses native IndexedDB, Web Locks, and Web Crypto; it adds no library dependencies.
It requires a secure browser context (HTTPS or localhost) with these APIs, and also works in workers
that provide them.

```ts
import { initializeFaro, PersistentFetchTransport } from '@grafana/faro-web-sdk';

const transport = new PersistentFetchTransport({
  url: 'https://collector.example/collect',
  apiKey: 'collector-api-key',
  persistence: {
    namespace: 'my-app:production',
    maxEvents: 10_000,
    maxEventAgeMs: 24 * 60 * 60 * 1000,
  },
  batching: {
    maxEvents: 50,
    sendIntervalMs: 1000,
  },
});

initializeFaro({
  app: { name: 'my-app' },
  transports: [transport],
});
```

Register this transport as the HTTP destination. Registering `FetchTransport` alongside it sends
telemetry twice. Other destinations, such as `ConsoleTransport`, can still be registered.

### Persistence and delivery

Each accepted Faro transport item has its own IndexedDB record, including its original timestamp,
user and session metadata. A trace item can contain multiple spans. Events arrive at this transport
immediately even when Faro batching is enabled: `isBatched()` returns `false`, and the transport
builds HTTP batches after persistence. Sampling, before-send filters, and user-action buffering
still happen upstream, so signals not yet handed to the transport are not persisted.

`send()` accepts one item or an array. Events arriving together share a database transaction, but
remain individually addressable records. No HTTP request begins until that transaction commits.
The queue uses indexes and bounded reads instead of loading the entire backlog for every write.

The transport automatically checks old queued events on initialization, session changes, browser
`online` and `pageshow` events, and every `sendIntervalMs`. No new event is needed to replay a prior
session. A replay preserves the original session and user; events with different metadata are
never combined in the same HTTP body. Pausing Faro stops delivery until it is unpaused. Persisted
retry deadlines remain in force across sessions and reloads.

Only a successful HTTP response removes acknowledged event IDs. Failed requests, including 4xx
responses, remain queued until retry or expiry/eviction. Retry delay grows exponentially with jitter
(default initial 1 second, capped at 60 seconds). A collector's `Retry-After` on 429 or 503 is honored
even when it exceeds that cap. A failed oldest batch delays later batches. One native Web Lock per
queue serializes delivery across tabs and workers, and a closed context automatically releases its
lock. One HTTP batch is sent at a time.

The transport reuses Fetch transport HTTP behavior, including async headers, compression, timeout,
keepalive fallback, collector exclusion from instrumentation, and session-invalid responses. Old
session responses cannot rotate the current session. Credentials and resolved headers are not
persisted; retries use the current instance's configuration. The queue partition is a SHA-256 hash
of the namespace and collector URL, so different destinations cannot drain each other's events.
Keep the namespace and URL stable across sessions to resume the same queue.

Delivery is at least once within the configured retention limits. A server can accept a request
before a browser closes without committing local deletion. An identical event batch has a stable
idempotency key across retries, but server deduplication is not guaranteed, and regrouped batches
can still cause duplicates. Browser storage eviction, unavailable storage, and termination before
an IndexedDB transaction commits can also lose events.

### Limits and lifecycle

- `persistence.maxEvents` defaults to 10,000 individual transport items, including in-flight items.
  When full, the oldest items not currently in flight are evicted. If all slots are in flight, the
  oldest incoming items are dropped instead. Use consistent limits for instances sharing a queue.
- `persistence.maxEventAgeMs` defaults to 24 hours since enqueue. Expired items are removed on enqueue
  or a queue check; items already being sent remain protected until that attempt finishes.
- `batching.maxEvents` defaults to 50 items per HTTP request. Metadata changes can produce smaller batches.
- `batching.sendIntervalMs` defaults to 1,000 milliseconds. Browsers may delay background timers.
- `requestTimeoutMs` defaults to 10,000 milliseconds per HTTP attempt. Async request preparation is
  bounded separately by the same timeout.
- `retry.initialBackoffMs` and `retry.maxBackoffMs` configure persistent retry delays. Retries continue
  until success or eviction; the volatile Fetch transport's `maxAttempts` does not apply.
- `onDrop` receives `{ reason, count }` for expiry, capacity eviction, serialization failures,
  and corrupt stored records. Corrupt records are removed without blocking valid telemetry.
- `onStorageError` receives storage/API failures. Unpersisted events are never sent. Errors are also
  reported through Faro's internal logger, without logging the telemetry payload.
- `await transport.flush()` attempts currently eligible queued events and returns when they have
  been sent, deferred, or another context owns delivery. It does not bypass backoff or pause.
- `await transport.dispose()` stops timers/listeners, aborts requests, finishes accepted writes and
  closes the database connection. Pending events remain for the next instance. Call this explicitly
  when retiring an instance; removing it from Faro's transport list alone does not dispose it.

Count limits do not bound byte usage: trace payloads and logs can vary greatly in size. Choose limits
for the application's event sizes and the collector's acceptance window for old telemetry.
