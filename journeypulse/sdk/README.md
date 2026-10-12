# JourneyPulse JavaScript SDK

A dependency-free ES module for browser instrumentation. Import [journeypulse.mjs](journeypulse.mjs) from your application's bundled assets. It tracks the five Phase 1 names; the gateway remains the authoritative validator of required properties.

## Usage

~~~javascript
import { JourneyPulseTracker } from "./journeypulse.mjs";

const tracker = new JourneyPulseTracker({
  endpoint: "http://127.0.0.1:8020/v1/events",
  userId: "u123",
  sessionId: "s456",
  getToken: async ({ forceRefresh, signal }) => {
    const response = await fetch("/api/tracking/session", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ force_refresh: forceRefresh }),
      signal,
    });
    if (!response.ok) throw new Error("Tracking session is unavailable");
    return response.json();
  },
  onResult: outcome => console.log(outcome.status, outcome.event.event_id),
});

tracker.track("session_started", { platform: "web" });
tracker.track("restaurant_view", { restaurant_id: "r88" });
tracker.track("add_to_cart", {
  cart_id: "c789", restaurant_id: "r88", item_id: "i42", quantity: 1,
});
await tracker.flush();
~~~

The application's `/api/tracking/session` handler is an integration responsibility: it derives the logged-in user and session, calls the gateway's server-only `POST /v1/sessions` with the source key and returns the scoped token. It is not a gateway route. The SDK accepts either a token string or the gateway session response object.

Source server keys and signing keys never belong in browser code. Tokens are short-lived and tied to one source, user and session. Create a new tracker when identity/session changes.

## Methods

- `track(eventName, properties, { occurredAt })`: queues a serialized snapshot and returns its stable event ID. The default time is the current ISO timestamp. Unknown event names, invalid identities, oversized events and a full buffer throw synchronously.
- `flush({ keepalive: false })`: returns a promise for a bounded pass over currently queued events. Concurrent calls share one delivery loop.
- `stats()`: reports accepted, rejected, dropped and pending event counts.
- `close({ flush: true })`: stops the automatic timer, removes the unload listener, flushes and prevents new tracking.

Property objects are copied when queued, so subsequent UI changes do not alter retries. Explicit flush is useful at demo checkpoints; the default timer also flushes automatically.

## Defaults and guarantees

| Setting | Default |
| --- | --- |
| Batch size | 20 events; maximum 50 |
| In-memory buffer | 200 events, including in-flight events |
| Automatic flush interval | 2 seconds |
| Attempts per event | 5; maximum 10 |
| Request timeout | 15 seconds, including token acquisition and response parsing |
| Request body cap | 256 KiB, counted as UTF-8 bytes |
| Initial backoff | 500 ms with jitter |
| Maximum delay | 30 seconds |
| Unload payload cap | 60,000 bytes |

Pass matching constructor options to override these values. Align client limits with your gateway configuration. No package installation is required; Node 22+ is used for tests and the server-side smoke demo.

Accepted results leave the queue. Permanently rejected events leave the queue and reach `onResult` with validation errors and `quarantine_persisted`. Only retryable or unknown results are retried. A 401 requests token refresh; network errors, 408, 429, 5xx and malformed acknowledgement responses receive bounded retries. Other request-level 4xx errors report a dropped event.

Retries use the same event ID, occurrence time and properties. If a response is lost after Kafka receives a record, a retry can create another delivery; Phase 3 implements deduplication.

Retry exhaustion reports `status: dropped, reason: retry_exhausted`; delivery may still have happened when acknowledgements were lost. A full queue throws instead of silently discarding events. Observe `onResult` and `stats` when troubleshooting.

Buffering is in memory. Refreshing/closing a page can lose queued events. The `pagehide` listener uses authorized `fetch` with `keepalive`, attempts one bounded batch and starts no unload retry loop. Oversized unload records remain pending for a later normal flush. An already running flush remains the active delivery loop. Unload delivery and asynchronous token acquisition are best effort; use a cached token where possible.

## Runnable browser probe

1. Start the gateway using the [Phase 2 guide](../../docs/journeypulse/phase-2-capture.md).
2. Issue a session from your trusted backend or the guide's PowerShell command.
3. Serve this SDK directory locally:

~~~powershell
.\.venv\Scripts\python.exe -m http.server 5173 --bind 127.0.0.1 --directory journeypulse/sdk
~~~

Open `http://localhost:5173`. In its developer console import `captureJourney` from `./browser-demo.mjs`, then call `captureJourney(session)` with the actual session response object. The helper sends five valid events and an invalid cart; the expected totals are five accepted and one rejected. No source key is needed in the browser.

For a command-line SDK probe, run `node scripts/demo_journeypulse_sdk.mjs`. This script is server-side Node code and obtains its source key from the environment.

## Tests

~~~powershell
node --test journeypulse/sdk/journeypulse.test.mjs
~~~

Tests cover selective retries, identity/payload stability, token refresh, finite attempts, overflow, timeouts, malformed responses, callback failures, byte limits and unload behavior.
