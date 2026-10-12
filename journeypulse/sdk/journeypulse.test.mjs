import { test } from "node:test";
import assert from "node:assert/strict";
import { JourneyPulseTracker } from "./journeypulse.mjs";

function response(status, results, headers = {}) {
  return { status, headers: new Headers(headers), json: async () => ({ results }) };
}
function fixture(fetchImpl, options = {}) {
  let counter = 0;
  return new JourneyPulseTracker({
    userId: "u123", sessionId: "s456", getToken: async () => "browser-session-token",
    idFactory: () => "evt_" + ++counter, flushIntervalMs: 0, baseDelayMs: 0,
    maxDelayMs: 1000, eventTarget: null, sleep: async () => {}, fetchImpl, ...options,
  });
}
const result = (event, index, status) => ({ event_id: event.event_id, index, status });

test("batches all five event names and sends only a session bearer token", async () => {
  const requests = [];
  const tracker = fixture(async (url, options) => {
    requests.push(options);
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  }, { batchSize: 2 });
  for (const name of ["session_started", "restaurant_view", "add_to_cart", "checkout_started", "order_completed"])
    tracker.track(name, {});
  const totals = await tracker.flush();
  assert.equal(requests.length, 3);
  assert.deepEqual(totals, { accepted: 5, rejected: 0, dropped: 0, pending: 0 });
  assert.equal(requests[0].headers.Authorization, "Bearer browser-session-token");
  assert.equal(requests[0].headers["X-Source-Key"], undefined);
});

test("partial batch retries only uncertain events and preserves their payloads", async () => {
  const bodies = [], reported = [];
  const tracker = fixture(async (url, options) => {
    const events = JSON.parse(options.body).events;
    bodies.push(events);
    return response(bodies.length === 1 ? 207 : 200,
      events.map((event, i) => result(event, i, bodies.length === 1 ? ["accepted", "retryable", "rejected"][i] : "accepted")));
  }, { onResult: event => reported.push(event) });
  const properties = { restaurant_id: "r1" };
  tracker.track("restaurant_view", properties);
  tracker.track("restaurant_view", properties);
  tracker.track("add_to_cart", { quantity: 0 });
  properties.restaurant_id = "changed";
  await tracker.flush();
  assert.deepEqual(bodies[1], [bodies[0][1]]);
  assert.equal(bodies[0][0].properties.restaurant_id, "r1");
  assert.deepEqual(tracker.stats(), { accepted: 2, rejected: 1, dropped: 0, pending: 0 });
  assert.equal(reported.length, 3);
});

test("network failure and lost response retry stable event IDs", async () => {
  const bodies = [];
  const tracker = fixture(async (url, options) => {
    bodies.push(options.body);
    if (bodies.length === 1) throw new Error("Network disconnected after delivery");
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  });
  tracker.track("session_started", { platform: "web" });
  await tracker.flush();
  assert.equal(bodies[0], bodies[1]);
});

test("429 honors Retry-After with bounded delay", async () => {
  const delays = [];
  let attempts = 0;
  const tracker = fixture(async (url, options) => {
    if (++attempts === 1) return response(429, undefined, { "Retry-After": "1" });
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  }, { sleep: async ms => delays.push(ms) });
  tracker.track("session_started", { platform: "web" });
  await tracker.flush();
  assert.deepEqual(delays, [1000]);
});

test("expired tokens request refresh without changing event identity", async () => {
  const refreshes = [];
  let attempts = 0;
  const tracker = fixture(async (url, options) => {
    if (++attempts === 1) return response(401);
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  }, { getToken: async ({ forceRefresh }) => { refreshes.push(forceRefresh); return "token"; } });
  tracker.track("session_started", { platform: "web" });
  await tracker.flush();
  assert.deepEqual(refreshes, [false, true]);
});

test("buffer overflow is explicit and retries are finite", async () => {
  const outcomes = [];
  const tracker = fixture(async () => { throw new Error("offline"); },
    { batchSize: 1, maxBufferSize: 1, maxAttempts: 2, onResult: result => outcomes.push(result) });
  tracker.track("restaurant_view", {});
  assert.throws(() => tracker.track("restaurant_view", {}), /buffer is full/);
  await tracker.flush();
  assert.equal(outcomes[0].reason, "retry_exhausted");
  assert.deepEqual(tracker.stats(), { accepted: 0, rejected: 0, dropped: 1, pending: 0 });
});

test("concurrent flush calls share one delivery loop", async () => {
  let finish, attempts = 0;
  const tracker = fixture(async (url, options) => {
    attempts++;
    await new Promise(resolve => { finish = resolve; });
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  });
  tracker.track("session_started", {});
  const first = tracker.flush(), second = tracker.flush();
  assert.equal(first, second);
  await new Promise(resolve => setImmediate(resolve));
  finish();
  await first;
  assert.equal(attempts, 1);
});

test("unload uses keepalive once and preserves uncertain events", async () => {
  let calls = 0, keepalive;
  const tracker = fixture(async (url, options) => {
    calls++; keepalive = options.keepalive; throw new Error("offline");
  });
  tracker.track("session_started", {});
  await tracker.flush({ keepalive: true });
  assert.equal(calls, 1);
  assert.equal(keepalive, true);
  assert.equal(tracker.stats().pending, 1);
});

test("request timeout aborts even a fetch implementation that never resolves", async () => {
  let signal;
  const tracker = fixture(async (url, options) => {
    signal = options.signal;
    return new Promise(() => {});
  }, { requestTimeoutMs: 10, maxAttempts: 1 });
  tracker.track("session_started", {});
  await tracker.flush();
  assert.equal(signal.aborted, true);
  assert.equal(tracker.stats().dropped, 1);
});

test("unknown response indices are retried rather than incorrectly acknowledged", async () => {
  let attempts = 0;
  const tracker = fixture(async (url, options) => {
    const event = JSON.parse(options.body).events[0];
    return response(200, [result(event, ++attempts === 1 ? 9 : 0, "accepted")]);
  });
  tracker.track("session_started", {});
  await tracker.flush();
  assert.equal(attempts, 2);
  assert.equal(tracker.stats().accepted, 1);
});

test("payload byte limits split batches and reject oversized individual events", async () => {
  const counts = [];
  const tracker = fixture(async (url, options) => {
    assert.ok(new TextEncoder().encode(options.body).length <= 500);
    const events = JSON.parse(options.body).events;
    counts.push(events.length);
    return response(200, events.map((event, i) => result(event, i, "accepted")));
  }, { maxBodyBytes: 500 });
  tracker.track("restaurant_view", { restaurant_id: "r1", description: "x".repeat(100) });
  tracker.track("restaurant_view", { restaurant_id: "r2", description: "x".repeat(100) });
  assert.throws(() => tracker.track("restaurant_view", { huge: "x".repeat(600) }), /byte limit/);
  await tracker.flush();
  assert.deepEqual(counts, [1, 1]);
});

test("permanent request errors report dropped events and do not loop", async () => {
  let calls = 0;
  const tracker = fixture(async () => { calls++; return response(413); });
  tracker.track("restaurant_view", {});
  await tracker.flush();
  assert.equal(calls, 1);
  assert.equal(tracker.stats().dropped, 1);
});

test("callback failures do not retry already acknowledged deliveries", async () => {
  let calls = 0;
  const tracker = fixture(async (url, options) => {
    calls++;
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  }, { onResult: () => { throw new Error("UI callback failed"); } });
  tracker.track("restaurant_view", {});
  await tracker.flush();
  assert.equal(calls, 1);
});

test("closing detaches unload handling and prevents new tracking", async () => {
  const events = [];
  const target = {
    addEventListener: (name, handler) => events.push(["add", name, handler]),
    removeEventListener: (name, handler) => events.push(["remove", name, handler]),
  };
  const tracker = fixture(async (url, options) =>
    response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted"))),
    { eventTarget: target });
  tracker.track("restaurant_view", {});
  await tracker.close({ flush: true });
  assert.equal(events[0][2], events[1][2]);
  assert.throws(() => tracker.track("restaurant_view", {}), /closed/);
  assert.equal(tracker.stats().pending, 0);
});

test("malformed result rows are treated as uncertain delivery", async () => {
  let attempts = 0;
  const tracker = fixture(async (url, options) => {
    if (++attempts === 1) return response(200, [null]);
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  });
  tracker.track("restaurant_view", {});
  await tracker.flush();
  assert.equal(attempts, 2);
  assert.equal(tracker.stats().accepted, 1);
});

test("large unload records remain buffered for a later normal flush", async () => {
  let calls = 0;
  const tracker = fixture(async (url, options) => {
    calls++;
    return response(200, JSON.parse(options.body).events.map((event, i) => result(event, i, "accepted")));
  });
  tracker.track("restaurant_view", { large: "x".repeat(61000) });
  await tracker.flush({ keepalive: true });
  assert.equal(calls, 0);
  assert.equal(tracker.stats().pending, 1);
  await tracker.flush();
  assert.equal(tracker.stats().accepted, 1);
});
