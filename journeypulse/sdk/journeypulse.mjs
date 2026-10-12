/** Small browser SDK: bounded in-memory batching, stable IDs and selective retries. */
const EVENT_NAMES = new Set([
  "session_started", "restaurant_view", "add_to_cart", "checkout_started", "order_completed",
]);
const bytes = value => new TextEncoder().encode(value).length;
const validId = value => typeof value === "string" && value === value.trim()
  && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}$/.test(value);

export class JourneyPulseTracker {
  constructor({
    endpoint = "http://127.0.0.1:8020/v1/events", userId, sessionId, getToken,
    batchSize = 20, maxBufferSize = 200, maxAttempts = 5,
    maxBodyBytes = 262144, flushIntervalMs = 2000, requestTimeoutMs = 15000,
    baseDelayMs = 500, maxDelayMs = 30000, onResult = () => {},
    fetchImpl = globalThis.fetch?.bind(globalThis),
    idFactory = () => globalThis.crypto.randomUUID(),
    sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
    random = Math.random, eventTarget = globalThis.window,
  }) {
    if (!validId(userId) || !validId(sessionId) || typeof getToken !== "function"
        || typeof fetchImpl !== "function") throw new TypeError("Valid identity, getToken and fetch are required");
    for (const [name, value] of Object.entries({ batchSize, maxBufferSize, maxAttempts,
        maxBodyBytes, requestTimeoutMs })) {
      if (!Number.isInteger(value) || value <= 0) throw new RangeError(name + " must be a positive integer");
    }
    if (batchSize > 50 || maxBufferSize < batchSize || maxAttempts > 10
        || maxBodyBytes > 262144 || !Number.isFinite(flushIntervalMs) || flushIntervalMs < 0
        || !Number.isFinite(baseDelayMs) || baseDelayMs < 0
        || !Number.isFinite(maxDelayMs) || maxDelayMs < baseDelayMs) throw new RangeError("Invalid SDK limits");
    Object.assign(this, { endpoint, userId, sessionId, getToken, batchSize, maxBufferSize,
      maxAttempts, maxBodyBytes, requestTimeoutMs, baseDelayMs, maxDelayMs, onResult,
      fetchImpl, idFactory, sleep, random, eventTarget });
    this.queue = [];
    this.inflight = null;
    this.closed = false;
    this.forceRefresh = false;
    this.totals = { accepted: 0, rejected: 0, dropped: 0 };
    this.onPageHide = () => { void this.flush({ keepalive: true }); };
    eventTarget?.addEventListener?.("pagehide", this.onPageHide);
    if (flushIntervalMs > 0) {
      this.timer = setInterval(() => { void this.flush(); }, flushIntervalMs);
      this.timer.unref?.();
    }
  }

  track(eventName, properties, { occurredAt = new Date().toISOString() } = {}) {
    if (this.closed) throw new Error("Tracker is closed");
    if (!EVENT_NAMES.has(eventName)) throw new TypeError("Unregistered JourneyPulse event");
    if (!properties || typeof properties !== "object" || Array.isArray(properties))
      throw new TypeError("Event properties must be an object");
    if (this.queue.length >= this.maxBufferSize) throw new RangeError("JourneyPulse buffer is full");
    const event = JSON.parse(JSON.stringify({
      event_id: this.idFactory(), event_name: eventName, schema_version: 1,
      user_id: this.userId, session_id: this.sessionId, occurred_at: occurredAt, properties,
    }));
    if (!validId(event.event_id)) throw new TypeError("idFactory returned an invalid event ID");
    if (this.queue.some(entry => entry.event.event_id === event.event_id))
      throw new Error("idFactory returned a duplicate queued event ID");
    if (bytes(JSON.stringify({ events: [event] })) > this.maxBodyBytes)
      throw new RangeError("Event exceeds the request byte limit");
    this.queue.push({ event, attempts: 0 });
    return event.event_id;
  }

  stats() { return { ...this.totals, pending: this.queue.length }; }

  flush({ keepalive = false } = {}) {
    if (this.inflight) return this.inflight;
    this.inflight = this._drain(keepalive).finally(() => { this.inflight = null; });
    return this.inflight;
  }

  async close({ flush = false } = {}) {
    this.closed = true;
    clearInterval(this.timer);
    this.eventTarget?.removeEventListener?.("pagehide", this.onPageHide);
    if (flush) await this.flush();
    return this.stats();
  }

  _finish(entry, result) {
    this.queue = this.queue.filter(item => item !== entry);
    this.totals[result.status]++;
    try { this.onResult({ event: JSON.parse(JSON.stringify(entry.event)), ...result }); }
    catch { /* Consumer callbacks must not cause an acknowledged event to be retried. */ }
  }

  _batch(eligible, limit) {
    const batch = [];
    for (const entry of this.queue) {
      if (!eligible.has(entry)) continue;
      if (batch.length >= this.batchSize) break;
      if (bytes(JSON.stringify({ events: [...batch.map(e => e.event), entry.event] })) > limit) break;
      batch.push(entry);
    }
    return batch;
  }

  async _request(batch, keepalive) {
    const controller = new AbortController();
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error("Gateway request timed out"));
      }, this.requestTimeoutMs);
    });
    try {
      return await Promise.race([(async () => {
        const tokenResult = await this.getToken({
          forceRefresh: this.forceRefresh, signal: controller.signal,
        });
        const token = typeof tokenResult === "string" ? tokenResult : tokenResult?.access_token;
        if (!token) throw new Error("getToken did not return an access token");
        this.forceRefresh = false;
        const response = await this.fetchImpl(this.endpoint, {
          method: "POST", headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
          body: JSON.stringify({ events: batch.map(entry => entry.event) }),
          signal: controller.signal, keepalive,
        });
        let body = null;
        try { body = await response.json(); } catch { /* Unknown response is retried. */ }
        return { response, body };
      })(), timeout]);
    } finally { clearTimeout(timer); }
  }

  async _drain(keepalive) {
    const eligible = new Set(this.queue);
    let retryAfter = 0;
    while (this.queue.some(entry => eligible.has(entry))) {
      const batch = this._batch(eligible, keepalive ? Math.min(this.maxBodyBytes, 60000) : this.maxBodyBytes);
      // A too-large unload record remains queued for a later regular flush.
      if (!batch.length) break;
      for (const entry of batch) entry.attempts++;
      let reply = null;
      try { reply = await this._request(batch, keepalive); } catch { /* Delivery is unknown. */ }
      const results = reply?.body?.results;
      const validResults = Array.isArray(results) && results.length === batch.length
        && results.every((result, index) => result && typeof result === "object" && result.index === index
          && result.event_id === batch[index].event.event_id
          && ["accepted", "rejected", "retryable"].includes(result.status));
      retryAfter = 0;
      if (reply) {
        const header = reply.response.headers?.get?.("Retry-After");
        if (header) {
          const seconds = Number(header);
          retryAfter = Number.isFinite(seconds) ? Math.max(0, seconds * 1000)
            : Math.max(0, Date.parse(header) - Date.now()) || 0;
        }
      }
      if (validResults) {
        results.forEach((result, index) => {
          if (result.status !== "retryable") this._finish(batch[index], result);
        });
      } else if (reply?.response.status === 401) {
        this.forceRefresh = true;
      } else if (reply && reply.response.status >= 400 && reply.response.status < 500
                 && ![408, 429].includes(reply.response.status)) {
        batch.forEach(entry => this._finish(entry, {
          status: "dropped", reason: "permanent_request_error", http_status: reply.response.status,
          detail: reply.body?.detail,
        }));
      }
      const pending = batch.filter(entry => this.queue.includes(entry));
      pending.filter(entry => entry.attempts >= this.maxAttempts).forEach(entry => this._finish(entry, {
        status: "dropped", reason: "retry_exhausted",
      }));
      if (keepalive) break; // Unload is best effort; do not schedule an unload retry loop.
      const retrying = pending.filter(entry => this.queue.includes(entry));
      if (retrying.length) {
        const attempt = Math.max(...retrying.map(entry => entry.attempts));
        const jitter = Math.min(1, Math.max(0, this.random()));
        const delay = Math.min(this.maxDelayMs,
          Math.max(retryAfter, this.baseDelayMs * 2 ** (attempt - 1) * (0.5 + jitter)));
        await this.sleep(delay);
      }
    }
    return this.stats();
  }
}
