/** Server-side SDK smoke demo. Source credentials never belong in browser code. */
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { JourneyPulseTracker } from "../journeypulse/sdk/journeypulse.mjs";

const url = process.env.JOURNEYPULSE_GATEWAY_URL || "http://127.0.0.1:8020";
const identity = {
  source_id: process.env.JOURNEYPULSE_SOURCE_ID || "demo-app",
  user_id: "sdk_" + randomUUID().replaceAll("-", ""),
  session_id: "session_" + randomUUID().replaceAll("-", ""),
};
let session;
async function getToken({ forceRefresh = false, signal } = {}) {
  if (!session || forceRefresh || session.expires_at * 1000 <= Date.now() + 10000) {
    const response = await fetch(url + "/v1/sessions", {
      method: "POST", headers: { "Content-Type": "application/json",
        "X-Source-Key": process.env.JOURNEYPULSE_SOURCE_KEY || "journeypulse-local-source-key" },
      body: JSON.stringify(identity), signal,
    });
    if (!response.ok) throw new Error("Session issuance failed: HTTP " + response.status);
    session = await response.json();
  }
  return session.access_token;
}
const outcomes = [];
const tracker = new JourneyPulseTracker({
  endpoint: url + "/v1/events", userId: identity.user_id, sessionId: identity.session_id,
  getToken, flushIntervalMs: 0, eventTarget: null, onResult: result => outcomes.push(result),
});
const examples = JSON.parse(await readFile(
  new URL("../contracts/journeypulse/examples/valid-journey.json", import.meta.url), "utf8"));
for (const event of examples) tracker.track(event.event_name, event.properties);
tracker.track("add_to_cart", { cart_id: "c789", restaurant_id: "r88", quantity: 0 });
await tracker.close({ flush: true });
console.log(JSON.stringify({ identity, totals: tracker.stats(), outcomes }, null, 2));
if (tracker.stats().accepted !== 5 || tracker.stats().rejected !== 1 || tracker.stats().dropped !== 0
    || !outcomes.find(result => result.status === "rejected")?.quarantine_persisted) process.exitCode = 1;
