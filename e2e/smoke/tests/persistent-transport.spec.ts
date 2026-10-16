import { expect, type Page, test } from '@playwright/test';

import type { Faro, TransportItem } from '@grafana/faro-core';
import type * as WebSdk from '@grafana/faro-web-sdk';

interface PersistentHarness {
  sdk: typeof WebSdk;
  transport: WebSdk.PersistentFetchTransport;
  faro: Faro;
  drops: WebSdk.PersistentTransportDropEvent[];
  errors: string[];
  now: number;
}
declare global {
  interface Window {
    __persistent: PersistentHarness;
  }
}

async function load(page: Page) {
  await page.route('**/persistent-test', (route) => route.fulfill({ contentType: 'text/html', body: '<html></html>' }));
  await page.goto('/persistent-test');
  await page.addScriptTag({ url: '/bundles/faro-web-sdk.iife.js' });
}
async function setup(
  page: Page,
  options: { paused?: boolean; maxEvents?: number; maxAgeMs?: number; namespace?: string } = {}
) {
  await page.evaluate((options) => {
    const sdk = (window as unknown as { GrafanaFaroWebSdk: typeof WebSdk }).GrafanaFaroWebSdk;
    const drops: WebSdk.PersistentTransportDropEvent[] = [];
    const errors: string[] = [];
    const harness = { sdk, drops, errors, now: Date.now() } as PersistentHarness;
    harness.transport = new sdk.PersistentFetchTransport({
      url: `${location.origin}/collect`,
      persistence: {
        namespace: options.namespace ?? 'test',
        maxEvents: options.maxEvents,
        maxEventAgeMs: options.maxAgeMs,
      },
      batching: { maxEvents: 50, sendIntervalMs: 60000 },
      getNow: () => harness.now,
      getRandom: () => 1,
      onDrop: (event) => drops.push(event),
      onStorageError: (error) => errors.push(String(error)),
    });
    harness.faro = sdk.initializeFaro({
      app: { name: 'persistent-test' },
      transports: [harness.transport],
      instrumentations: [],
      metas: [],
      sessionTracking: { enabled: false },
      batching: { enabled: true, sendTimeout: 60000 },
      paused: options.paused ?? true,
      isolate: true,
    });
    window.__persistent = harness;
  }, options);
}
async function enqueue(page: Page, messages: string[], session = 'original') {
  await page.evaluate(
    async ({ messages, session }) => {
      const { transport, sdk } = window.__persistent;
      await transport.send(
        messages.map((message) => ({
          type: sdk.TransportItemType.LOG,
          payload: { message, level: sdk.LogLevel.INFO, timestamp: new Date().toISOString() },
          meta: { session: { id: session }, app: { name: 'persistent-test' } },
        }))
      );
    },
    { messages, session }
  );
}
async function queued(page: Page): Promise<TransportItem[]> {
  return page.evaluate(
    () =>
      new Promise((resolve, reject) => {
        const request = window.indexedDB.open('faro-persistent-fetch', 1);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => {
          const db = request.result;
          const read = db.transaction('events').objectStore('events').getAll();
          read.onsuccess = () => {
            db.close();
            resolve(read.result.map((event) => event.item));
          };
          read.onerror = () => {
            db.close();
            reject(read.error);
          };
        };
      })
  );
}
async function flush(page: Page) {
  await page.evaluate(async () => {
    window.__persistent.faro.config.paused = false;
    await window.__persistent.transport.flush();
  });
}

test('persists immediately with SDK batching enabled, deleting only after acknowledgement', async ({ page }) => {
  await load(page);
  await setup(page, { paused: false });
  let acknowledge: (() => void) | undefined;
  await page.route('**/collect', async (route) => {
    await new Promise<void>((resolve) => {
      acknowledge = resolve;
    });
    await route.fulfill({ status: 202, body: '{}' });
  });
  await page.evaluate(() => window.__persistent.faro.api.pushLog(['through-sdk']));
  await expect.poll(async () => (await queued(page)).length).toBe(1);
  const delivery = flush(page);
  await expect.poll(() => Boolean(acknowledge)).toBe(true);
  expect(await queued(page)).toHaveLength(1);
  acknowledge!();
  await delivery;
  expect(await queued(page)).toHaveLength(0);
});

test('replays a previous session at startup without a new event', async ({ page }) => {
  await load(page);
  await setup(page);
  await enqueue(page, ['offline'], 'previous-session');
  await page.reload();
  await page.addScriptTag({ url: '/bundles/faro-web-sdk.iife.js' });
  const bodies: Array<{ meta: { session: { id: string } }; logs: Array<{ message: string }> }> = [];
  await page.route('**/collect', async (route) => {
    bodies.push(route.request().postDataJSON());
    await route.fulfill({ status: 202, body: '{}' });
  });
  await setup(page, { paused: false });
  await expect.poll(() => bodies.length).toBe(1);
  expect(bodies[0]!.meta.session.id).toBe('previous-session');
  expect(bodies[0]!.logs[0]!.message).toBe('offline');
  await expect.poll(async () => (await queued(page)).length).toBe(0);
});

test('retains failed requests, honors Retry-After and preserves request identity', async ({ page }) => {
  await load(page);
  await setup(page);
  await enqueue(page, ['retry']);
  const attempts: Array<{ body: string | null; key: string }> = [];
  await page.route('**/collect', async (route) => {
    attempts.push({ body: route.request().postData(), key: route.request().headers()['idempotency-key']! });
    await route.fulfill({ status: attempts.length === 1 ? 503 : 202, headers: { 'Retry-After': '120' }, body: '{}' });
  });
  await flush(page);
  expect(await queued(page)).toHaveLength(1);
  await page.evaluate(() => {
    window.__persistent.now += 60000;
  });
  await flush(page);
  expect(attempts).toHaveLength(1);
  await page.evaluate(() => {
    window.__persistent.now += 60001;
  });
  await flush(page);
  expect(attempts).toHaveLength(2);
  expect(attempts[1]).toEqual(attempts[0]);
  expect(await queued(page)).toHaveLength(0);
});

test('enforces event-count and age limits independently of batches', async ({ page }) => {
  await load(page);
  await setup(page, { maxEvents: 3, maxAgeMs: 1000 });
  await enqueue(page, ['a', 'b', 'c', 'd', 'e']);
  expect((await queued(page)).map((item) => (item.payload as { message: string }).message)).toEqual(['c', 'd', 'e']);
  await page.evaluate(() => {
    window.__persistent.now += 1001;
  });
  await enqueue(page, ['fresh']);
  expect((await queued(page)).map((item) => (item.payload as { message: string }).message)).toEqual(['fresh']);
  expect(await page.evaluate(() => window.__persistent.drops)).toEqual([
    { reason: 'capacity', count: 2 },
    { reason: 'expired', count: 3 },
  ]);
});

test('separates sessions and preserves concurrently enqueued events', async ({ page }) => {
  await load(page);
  await setup(page);
  await enqueue(page, ['first'], 'one');
  let acknowledge: (() => void) | undefined;
  const sessions: string[] = [];
  await page.route('**/collect', async (route) => {
    sessions.push(route.request().postDataJSON().meta.session.id);
    if (sessions.length === 1) {
      await new Promise<void>((resolve) => {
        acknowledge = resolve;
      });
    }
    await route.fulfill({ status: 202, body: '{}' });
  });
  const delivery = flush(page);
  await expect.poll(() => Boolean(acknowledge)).toBe(true);
  await enqueue(page, ['second'], 'two');
  expect(await queued(page)).toHaveLength(2);
  acknowledge!();
  await delivery;
  expect(sessions).toEqual(['one', 'two']);
  expect(await queued(page)).toHaveLength(0);
});

test('coordinates competing tabs with a native Web Lock', async ({ page, context }) => {
  await load(page);
  await setup(page);
  await enqueue(page, ['one-owner']);
  const other = await context.newPage();
  await load(other);
  await setup(other);
  let acknowledge: (() => void) | undefined;
  let requests = 0;
  await context.route('**/collect', async (route) => {
    requests++;
    await new Promise<void>((resolve) => {
      acknowledge = resolve;
    });
    await route.fulfill({ status: 202, body: '{}' });
  });
  const delivery = flush(page);
  await expect.poll(() => Boolean(acknowledge)).toBe(true);
  await flush(other);
  expect(requests).toBe(1);
  acknowledge!();
  await delivery;
  expect(await queued(other)).toHaveLength(0);
});

test('never sends an event whose persistence failed', async ({ page }) => {
  await load(page);
  await setup(page);
  await page.evaluate(() => {
    window.indexedDB.open = () => {
      throw new DOMException('Unavailable', 'SecurityError');
    };
  });
  let requests = 0;
  await page.route('**/collect', async (route) => {
    requests++;
    await route.fulfill({ status: 202, body: '{}' });
  });
  await enqueue(page, ['not-durable']);
  await flush(page);
  expect(requests).toBe(0);
  expect(await page.evaluate(() => window.__persistent.errors)).toHaveLength(1);
});

test('a session change wakes delivery while preserving the queued session', async ({ page }) => {
  await load(page);
  await setup(page);
  await enqueue(page, ['old-session'], 'old');
  const sessions: string[] = [];
  await page.route('**/collect', async (route) => {
    sessions.push(route.request().postDataJSON().meta.session.id);
    await route.fulfill({ status: 202, body: '{}' });
  });
  await page.evaluate(() => {
    window.__persistent.faro.config.paused = false;
    window.__persistent.faro.api.setSession({ id: 'new' });
  });
  await expect.poll(() => sessions).toEqual(['old']);
  await expect.poll(async () => (await queued(page)).length).toBe(0);
});

test('removes corrupt records without blocking subsequent telemetry', async ({ page }) => {
  await load(page);
  await setup(page);
  await enqueue(page, ['corrupt', 'valid']);
  await page.evaluate(
    () =>
      new Promise<void>((resolve, reject) => {
        const request = window.indexedDB.open('faro-persistent-fetch', 1);
        request.onsuccess = () => {
          const db = request.result;
          const transaction = db.transaction('events', 'readwrite');
          const events = transaction.objectStore('events');
          const cursor = events.openCursor();
          cursor.onsuccess = () => {
            cursor.result!.update({ ...cursor.result!.value, item: null });
          };
          transaction.oncomplete = () => {
            db.close();
            resolve();
          };
          transaction.onerror = () => {
            db.close();
            reject(transaction.error);
          };
        };
      })
  );
  const messages: string[] = [];
  await page.route('**/collect', async (route) => {
    messages.push(
      ...route
        .request()
        .postDataJSON()
        .logs.map((log: { message: string }) => log.message)
    );
    await route.fulfill({ status: 202, body: '{}' });
  });
  await flush(page);
  expect(messages).toEqual(['valid']);
  expect(await queued(page)).toHaveLength(0);
  expect(await page.evaluate(() => window.__persistent.drops)).toContainEqual({ reason: 'corrupt', count: 1 });
});

test('does not evict in-flight items when new events fill the queue', async ({ page }) => {
  await load(page);
  await setup(page, { maxEvents: 1 });
  await enqueue(page, ['in-flight']);
  let acknowledge: (() => void) | undefined;
  await page.route('**/collect', async (route) => {
    await new Promise<void>((resolve) => {
      acknowledge = resolve;
    });
    await route.fulfill({ status: 202, body: '{}' });
  });
  const delivery = flush(page);
  await expect.poll(() => Boolean(acknowledge)).toBe(true);
  await enqueue(page, ['overflow']);
  expect((await queued(page)).map((item) => (item.payload as { message: string }).message)).toEqual(['in-flight']);
  acknowledge!();
  await delivery;
  expect(await queued(page)).toHaveLength(0);
  expect(await page.evaluate(() => window.__persistent.drops)).toContainEqual({ reason: 'capacity', count: 1 });
});

test('persists and delivers from a worker without window or document', async ({ page, context }) => {
  await load(page);
  const messages: string[] = [];
  await context.route('**/collect', async (route) => {
    messages.push(
      ...route
        .request()
        .postDataJSON()
        .logs.map((log: { message: string }) => log.message)
    );
    await route.fulfill({ status: 202, body: '{}' });
  });
  await context.route('**/persistent-worker.js', (route) =>
    route.fulfill({
      contentType: 'text/javascript',
      body: `
      importScripts('/bundles/faro-web-sdk.iife.js');
      const sdk = GrafanaFaroWebSdk;
      const transport = new sdk.PersistentFetchTransport({
        url: location.origin + '/collect', persistence: { namespace: 'worker' },
        batching: { sendIntervalMs: 60000 },
      });
      sdk.initializeFaro({
        app: { name: 'worker' }, transports: [transport], metas: [], instrumentations: [],
        sessionTracking: { enabled: false }, isolate: true, preventGlobalExposure: true,
      });
      (async () => {
        await transport.send({
          type: 'log', payload: { message: 'worker-event', level: 'info', timestamp: new Date().toISOString() },
          meta: { session: { id: 'worker-session' } },
        });
        await transport.flush();
        await transport.dispose();
        postMessage('done');
      })().catch(error => postMessage(String(error)));
    `,
    })
  );
  const result = await page.evaluate(
    () =>
      new Promise<string>((resolve, reject) => {
        const worker = new Worker('/persistent-worker.js');
        worker.onmessage = (event) => {
          worker.terminate();
          resolve(event.data);
        };
        worker.onerror = (error) => {
          worker.terminate();
          reject(new Error(error.message));
        };
      })
  );
  expect(result).toBe('done');
  expect(messages).toEqual(['worker-event']);
  expect(await queued(page)).toHaveLength(0);
});
