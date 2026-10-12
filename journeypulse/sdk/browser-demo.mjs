/** Run from a browser console with a session object issued by the trusted backend. */
import { JourneyPulseTracker } from "./journeypulse.mjs";

export async function captureJourney(session, endpoint = "http://127.0.0.1:8020/v1/events") {
  const outcomes = [];
  const tracker = new JourneyPulseTracker({
    endpoint, userId: session.user_id, sessionId: session.session_id,
    getToken: async () => session.access_token,
    flushIntervalMs: 0, onResult: result => outcomes.push(result),
  });
  tracker.track("session_started", { platform: "web" });
  tracker.track("restaurant_view", { restaurant_id: "r88" });
  tracker.track("add_to_cart", { cart_id: "c789", restaurant_id: "r88", item_id: "i42", quantity: 1 });
  tracker.track("checkout_started", { cart_id: "c789" });
  tracker.track("order_completed", { order_id: "o001", cart_id: "c789", restaurant_id: "r88" });
  tracker.track("add_to_cart", { cart_id: "c789", restaurant_id: "r88", quantity: 0 });
  const totals = await tracker.close({ flush: true });
  return { totals, outcomes };
}
