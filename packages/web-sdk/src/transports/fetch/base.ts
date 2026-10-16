import { BaseTransport, genShortID, getTransportBody, noop } from '@grafana/faro-core';
import type { BaseExtension, Config, Patterns, TransportItem } from '@grafana/faro-core';

import { getSessionManagerByConfig } from '../../instrumentations/session/sessionManager';
import { getUserSessionUpdater } from '../../instrumentations/session/sessionManager/sessionManagerUtils';
import { parseHttpDate } from '../../utils/httpDate';

import type { AttemptOutcome, DeliveryFailure } from './deliveryQueue';
import type { FetchTransportOptions } from './types';

const DEFAULT_REQUEST_TIMEOUT_MS = 10000;
const RETRYABLE_STATUS_CODES = new Set([408, 425, 429, 500, 502, 503, 504]);
const WAIT_INTERVAL_STATUS_CODES = new Set([429, 503]);
const BEACON_BODY_SIZE_LIMIT = 60000;
const MAX_KEEPALIVE_REQUESTS = 9;
const ACCEPTED = 202;

let pendingKeepaliveBodySize = 0;
let pendingKeepaliveRequests = 0;

interface KeepaliveReservation {
  keepalive: boolean;
  release: () => void;
}
class RequestTimeoutError extends Error {}

function getBodyByteSize(body: string): number {
  return typeof TextEncoder === 'undefined' ? body.length : new TextEncoder().encode(body).byteLength;
}

/** HTTP request mechanics shared by volatile and persistent delivery queues. */
export abstract class FetchTransportBase extends BaseTransport {
  protected readonly getNow: () => number;
  private readonly requestTimeoutMs: number;
  private readonly compressionEnabled: boolean;

  constructor(protected readonly options: FetchTransportOptions) {
    super();
    this.getNow = options.getNow ?? Date.now;
    this.requestTimeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
    this.compressionEnabled = (options.requestCompression ?? false) && typeof CompressionStream !== 'undefined';

    if (options.requestCompression && !this.compressionEnabled) {
      this.logWarn(
        'requestCompression is enabled but CompressionStream is not available. Falling back to uncompressed.'
      );
    }
    if (this.requestTimeoutMs > 0 && typeof AbortController === 'undefined') {
      this.logWarn('AbortController is unavailable. Requests will be sent without the configured timeout.');
    }
  }

  override getIgnoreUrls(): Patterns {
    return ([this.options.url] as Patterns).concat(this.config.ignoreUrls ?? []);
  }

  protected async prepareRequest(
    items: TransportItem[]
  ): Promise<{ requestInit: RequestInit; bodySize: number; sessionId: string | undefined }> {
    // Async headers and compression can outlive a session. Bind both the header
    // and its response handling to the payload, not the current session.
    const transportBody = getTransportBody(items);
    const sessionId = transportBody.meta.session?.id;
    const jsonBody = JSON.stringify(transportBody);

    const { headers = {}, ...requestOptions } = this.options.requestOptions ?? {};
    const { keepalive: _keepalive, signal: _signal, ...requestOptionsWithoutManagedFields } = requestOptions;
    const resolvedHeaders: Record<string, string> = {};

    for (const [key, value] of Object.entries(headers)) {
      resolvedHeaders[key] = typeof value === 'function' ? await value() : value;
    }

    let body: string | Blob = jsonBody;
    let bodySize = getBodyByteSize(jsonBody);
    const compressionHeaders: Record<string, string> = {};
    if (this.compressionEnabled) {
      body = await this.compress(jsonBody);
      bodySize = body.size;
      compressionHeaders['Content-Encoding'] = 'gzip';
    }

    return {
      bodySize,
      sessionId,
      requestInit: {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...compressionHeaders,
          ...resolvedHeaders,
          ...(this.options.apiKey ? { 'x-api-key': this.options.apiKey } : {}),
          ...(sessionId ? { 'x-faro-session-id': sessionId } : {}),
          // Unlike dynamic headers, this key is generated once and must remain stable across attempts.
          'Idempotency-Key': genShortID(20),
        },
        body,
        ...requestOptionsWithoutManagedFields,
      },
    };
  }

  protected async performAttempt(
    prepared: { requestInit: RequestInit; bodySize: number; sessionId: string | undefined },
    attemptsRemaining: number,
    unloading: boolean
  ): Promise<AttemptOutcome> {
    const callerSignal = this.options.requestOptions?.signal;
    if (callerSignal?.aborted) {
      return { kind: 'terminal', failure: { error: callerSignal.reason }, attemptsMade: 0 };
    }

    let attemptsMade = 0;
    try {
      const response = await this.fetchWithKeepaliveFallback(
        prepared.requestInit,
        prepared.bodySize,
        unloading ? true : this.options.requestOptions?.keepalive,
        attemptsRemaining,
        () => attemptsMade++
      );
      this.handleResponse(response, prepared.sessionId);

      if (response.status >= 200 && response.status < 300) {
        return { kind: 'success', attemptsMade };
      }

      const failure = { status: response.status };
      if (!RETRYABLE_STATUS_CODES.has(response.status) || unloading) {
        return { kind: 'terminal', failure, attemptsMade };
      }

      return {
        kind: 'retry',
        failure,
        attemptsMade,
        retryAfterMs: WAIT_INTERVAL_STATUS_CODES.has(response.status) ? this.getRetryAfterDelayMs(response) : undefined,
      };
    } catch (error) {
      const failure: DeliveryFailure = { error };
      if (callerSignal?.aborted || !this.isFetchNetworkError(error) || unloading) {
        return { kind: 'terminal', failure, attemptsMade };
      }
      return { kind: 'retry', failure, attemptsMade };
    }
  }

  private async fetchWithKeepaliveFallback(
    requestInit: RequestInit,
    bodySize: number,
    configuredKeepalive: boolean | undefined,
    attemptsRemaining: number,
    onFetch: () => void
  ): Promise<Response> {
    const startedAt = this.getNow();
    const reservation = this.reserveKeepalive(bodySize, configuredKeepalive);
    try {
      return await this.fetchWithTimeout(
        { ...requestInit, keepalive: reservation.keepalive },
        this.requestTimeoutMs,
        onFetch
      );
    } catch (error) {
      if (
        reservation.keepalive &&
        attemptsRemaining > 1 &&
        this.isFetchNetworkError(error) &&
        !(error instanceof RequestTimeoutError) &&
        !this.options.requestOptions?.signal?.aborted
      ) {
        this.logDebug('Retrying failed keepalive request with keepalive disabled.');
        const remainingTimeoutMs = this.requestTimeoutMs - (this.getNow() - startedAt);
        return this.fetchWithTimeout({ ...requestInit, keepalive: false }, remainingTimeoutMs, onFetch);
      }
      throw error;
    } finally {
      reservation.release();
    }
  }

  private async fetchWithTimeout(requestInit: RequestInit, timeoutMs: number, onFetch: () => void): Promise<Response> {
    const callerSignal = this.options.requestOptions?.signal;
    if (this.requestTimeoutMs <= 0 || typeof AbortController === 'undefined') {
      onFetch();
      return fetch(this.options.url, { ...requestInit, signal: callerSignal });
    }
    if (timeoutMs <= 0) {
      throw new RequestTimeoutError('Request timed out');
    }

    const controller = new AbortController();
    const abortFromCaller = () => controller.abort(callerSignal?.reason);
    callerSignal?.addEventListener('abort', abortFromCaller, { once: true });
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, timeoutMs);

    try {
      onFetch();
      return await fetch(this.options.url, { ...requestInit, signal: controller.signal });
    } catch (error) {
      if (timedOut && !callerSignal?.aborted) {
        throw new RequestTimeoutError('Request timed out');
      }
      throw error;
    } finally {
      clearTimeout(timeout);
      callerSignal?.removeEventListener('abort', abortFromCaller);
    }
  }

  private getRetryAfterDelayMs(response: Response): number | undefined {
    const value = response.headers.get('Retry-After');
    if (!value) {
      return undefined;
    }
    if (/^\d+$/.test(value)) {
      const delay = Number(value) * 1000;
      return Number.isFinite(delay) ? delay : Number.POSITIVE_INFINITY;
    }
    const now = this.getNow();
    const retryAt = parseHttpDate(value, now);
    return retryAt == null ? undefined : Math.max(0, retryAt - now);
  }

  private reserveKeepalive(bodySize: number, configuredKeepalive?: boolean): KeepaliveReservation {
    if (
      configuredKeepalive === false ||
      bodySize > BEACON_BODY_SIZE_LIMIT ||
      pendingKeepaliveBodySize + bodySize > BEACON_BODY_SIZE_LIMIT ||
      pendingKeepaliveRequests >= MAX_KEEPALIVE_REQUESTS
    ) {
      return { keepalive: false, release: noop };
    }

    pendingKeepaliveBodySize += bodySize;
    pendingKeepaliveRequests++;
    let released = false;
    return {
      keepalive: true,
      release: () => {
        if (!released) {
          released = true;
          pendingKeepaliveBodySize -= bodySize;
          pendingKeepaliveRequests--;
        }
      },
    };
  }

  private handleResponse(response: Response, requestSessionId: string | undefined): void {
    if (response.status === ACCEPTED && response.headers.get('X-Faro-Session-Status') === 'invalid') {
      this.extendFaroSession(this.config, this.logDebug.bind(this), requestSessionId);
    }
    response.text().catch(noop);
  }

  private isFetchNetworkError(error: unknown): boolean {
    return (
      error instanceof TypeError ||
      error instanceof RequestTimeoutError ||
      (error instanceof DOMException && error.name === 'AbortError')
    );
  }

  private async compress(body: string): Promise<Blob> {
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(body));
        controller.close();
      },
    }).pipeThrough(new CompressionStream('gzip'));
    const chunks: BlobPart[] = [];
    const reader = stream.getReader();
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        return new Blob(chunks);
      }
      chunks.push(value);
    }
  }

  private extendFaroSession(
    config: Config,
    logDebug: BaseExtension['logDebug'],
    requestSessionId: string | undefined
  ): void {
    const sessionTrackingConfig = config.sessionTracking;
    if (!sessionTrackingConfig?.enabled) {
      logDebug('Session expired.');
      return;
    }

    const { fetchUserSession, storeUserSession } = getSessionManagerByConfig(sessionTrackingConfig);

    // A delayed response must not rotate a newer session, including one another
    // tab has already established in shared storage.
    const currentSessionId = this.metas.value.session?.id;
    const storedSessionId = fetchUserSession()?.sessionId;
    if (!requestSessionId || requestSessionId !== currentSessionId || requestSessionId !== storedSessionId) {
      logDebug('Ignoring stale or cross-tab session-invalid response; request session no longer current.');
      return;
    }

    getUserSessionUpdater({ fetchUserSession, storeUserSession })({ forceSessionExtend: true });
    logDebug('Session expired; created new session.');
  }
}
