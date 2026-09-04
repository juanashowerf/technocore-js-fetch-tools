// src/fetch-with-rate-limit.js
// A token-bucket rate limiter wrapper around fetch.
// Useful when an agent must call a peer that enforces per-second/per-minute quotas
// (e.g. technocore rooms, public APIs). The limiter queues calls and only resolves
// once a token is available, preserving backpressure without crashing the caller.
//
// Usage:
//   const fetchRL = createRateLimitedFetch(globalThis.fetch, { tokensPerSecond: 5, burst: 10 });
//   const res = await fetchRL('https://example.org/x', { method: 'POST', body: '...' });
//
// Notes:
//   * Self-contained, no dependencies. Works in modern browsers and Node >=18.
//   * The bucket refills continuously at tokensPerSecond up to `burst`.
//   * `acquire()` returns a Promise that resolves when a token is granted.
//   * On timeout (option `waitTimeoutMs` > 0) the call rejects with RateLimitTimeoutError
//     so callers can decide whether to drop or retry instead of stalling forever.
//   * Optional `onWait(ms)` hook lets callers log/observe backpressure.

class RateLimitTimeoutError extends Error {
  constructor(waitedMs) {
    super(`Rate limit: no token available within ${waitedMs}ms`);
    this.name = 'RateLimitTimeoutError';
    this.code = 'RATE_LIMIT_TIMEOUT';
    this.waitedMs = waitedMs;
  }
}

function createRateLimitedFetch(baseFetch, options = {}) {
  if (typeof baseFetch !== 'function') {
    throw new TypeError('createRateLimitedFetch: baseFetch must be a function');
  }
  const tokensPerSecond = Number.isFinite(options.tokensPerSecond)
    ? options.tokensPerSecond
    : 5;
  const burst = Number.isFinite(options.burst) ? options.burst : tokensPerSecond;
  const waitTimeoutMs = Number.isFinite(options.waitTimeoutMs)
    ? options.waitTimeoutMs
    : 0; // 0 disables timeout
  const onWait = typeof options.onWait === 'function' ? options.onWait : null;

  if (!(tokensPerSecond > 0)) {
    throw new RangeError('tokensPerSecond must be > 0');
  }
  if (!(burst > 0)) {
    throw new RangeError('burst must be > 0');
  }

  let tokens = burst;
  let lastRefill = Date.now();
  const waiters = []; // { resolve, reject, deadline, startedAt }

  function refill() {
    const now = Date.now();
    const elapsedMs = now - lastRefill;
    if (elapsedMs <= 0) return;
    const refillAmount = (elapsedMs / 1000) * tokensPerSecond;
    if (refillAmount <= 0) return;
    tokens = Math.min(burst, tokens + refillAmount);
    lastRefill = now;
  }

  function scheduleDrain() {
    // Compute exact ms until the next token is available, then wake.
    refill();
    if (tokens >= 1) {
      // Hand a token to the longest-waiting caller immediately.
      const next = waiters.shift();
      if (next) grant(next);
      return;
    }
    if (waiters.length === 0) return;
    const deficit = 1 - tokens;
    const msUntilToken = (deficit / tokensPerSecond) * 1000;
    setTimeout(scheduleDrain, Math.max(1, Math.ceil(msUntilToken)));
  }

  function grant(waiter) {
    tokens -= 1;
    if (onWait) {
      try { onWait(Date.now() - waiter.startedAt); } catch (_) {}
    }
    waiter.resolve();
  }

  function expireTimedOutWaiters() {
    if (waitTimeoutMs <= 0) return;
    const now = Date.now();
    for (let i = waiters.length - 1; i >= 0; i--) {
      const w = waiters[i];
      if (w.deadline !== Infinity && now >= w.deadline) {
        w.reject(new RateLimitTimeoutError(now - w.startedAt));
        waiters.splice(i, 1);
      }
    }
  }

  function acquire() {
    refill();
    if (tokens >= 1 && waiters.length === 0) {
      tokens -= 1;
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const startedAt = Date.now();
      const deadline = waitTimeoutMs > 0 ? startedAt + waitTimeoutMs : Infinity;
      waiters.push({ resolve, reject, deadline, startedAt });
      scheduleDrain();
    });
  }

  const limitedFetch = function rateLimitedFetch(input, init) {
    return acquire().then(() => baseFetch(input, init));
  };

  // Expose introspection helpers so callers can observe pressure.
  limitedFetch.getStats = function getStats() {
    refill();
    return {
      tokensAvailable: tokens,
      burst,
      tokensPerSecond,
      queuedWaiters: waiters.length,
    };
  };
  limitedFetch.drain = function drain() {
    // Resolve or reject all queued waiters; useful in shutdown.
    expireTimedOutWaiters();
    while (waiters.length) {
      const w = waiters.shift();
      w.reject(new RateLimitTimeoutError(Date.now() - w.startedAt));
    }
    tokens = burst;
    lastRefill = Date.now();
  };

  // Run timeout reaper on a low-frequency interval when configured.
  if (waitTimeoutMs > 0) {
    const interval = Math.min(1000, Math.max(50, Math.floor(waitTimeoutMs / 4)));
    const handle = setInterval(expireTimedOutWaiters, interval);
    // Don't keep the event loop alive solely for the reaper.
    if (typeof handle.unref === 'function') handle.unref();
  }

  return limitedFetch;
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { createRateLimitedFetch, RateLimitTimeoutError };
}
if (typeof globalThis !== 'undefined') {
  globalThis.createRateLimitedFetch = createRateLimitedFetch;
  globalThis.RateLimitTimeoutError = RateLimitTimeoutError;
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
