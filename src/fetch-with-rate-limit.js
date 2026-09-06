/**
 * fetch-with-rate-limit.js
 *
 * A token-bucket rate limiter wrapper around the global fetch(). Drop-in for
 * fetch-only browser/JS agents that need to respect outbound request budgets
 * (per-host or global) without any build step or external dependency.
 *
 *   const fetch = makeRateLimitedFetch(globalThis.fetch, {
 *     tokensPerSecond: 5,        // sustained rate
 *     burst: 10,                 // bucket capacity (max queued wait)
 *     key: req => new URL(req.url).host, // per-host bucket key (or omit for global)
 *     maxQueue: 1000,            // refuse (reject) once queued requests exceed this
 *     onRefuse: ({key, queueSize}) => { /* hook *\/ },
 *   });
 *
 *   await fetch('https://api.example.com/v1/things');
 *
 * Notes:
 *   - Pure ES2020, works in browsers, Workers, Deno, and modern Node (18+).
 *   - Returns the underlying Response object unchanged.
 *   - AbortSignal on the input Request is honored; abort removes a queued
 *     waiter and decrements the queue counter.
 *   - The limiter NEVER mutates the caller's Request; it forwards the
 *     original signal and body untouched.
 */

export function makeRateLimitedFetch(baseFetch, options = {}) {
  if (typeof baseFetch !== 'function') {
    throw new TypeError('makeRateLimitedFetch: baseFetch must be a function');
  }

  const tokensPerSecond = Number(options.tokensPerSecond);
  if (!Number.isFinite(tokensPerSecond) || tokensPerSecond <= 0) {
    throw new RangeError('makeRateLimitedFetch: tokensPerSecond must be > 0');
  }
  const burst = Number.isFinite(options.burst) && options.burst > 0
    ? options.burst
    : tokensPerSecond;
  const refillIntervalMs = 1000 / tokensPerSecond;
  const keyOf = typeof options.key === 'function' ? options.key : () => '__global__';
  const maxQueue = Number.isFinite(options.maxQueue) && options.maxQueue > 0
    ? options.maxQueue
    : Infinity;
  const onRefuse = typeof options.onRefuse === 'function' ? options.onRefuse : null;

  /** @type {Map<string, {tokens:number, last:number, queue:Array, size:number}>} */
  const buckets = new Map();

  function getBucket(key) {
    let b = buckets.get(key);
    if (!b) {
      b = { tokens: burst, last: Date.now(), queue: [], size: 0 };
      buckets.set(key, b);
    }
    return b;
  }

  function refill(bucket) {
    const now = Date.now();
    const elapsed = now - bucket.last;
    if (elapsed <= 0) return;
    const gained = elapsed / refillIntervalMs;
    bucket.tokens = Math.min(burst, bucket.tokens + gained);
    bucket.last = now;
  }

  function pump(key) {
    const bucket = getBucket(key);
    refill(bucket);
    while (bucket.queue.length > 0 && bucket.tokens >= 1) {
      bucket.tokens -= 1;
      const { resolve, reject, input, init } = bucket.queue.shift();
      bucket.size -= 1;
      // Schedule the actual fetch on the next microtask so the caller never
      // observes re-entrancy from a synchronous loop.
      queueMicrotask(() => {
        Promise.resolve()
          .then(() => baseFetch(input, init))
          .then(resolve, reject);
      });
    }
    if (bucket.queue.length > 0) {
      // Time until at least one token is available.
      const waitMs = Math.max(1, (1 - bucket.tokens) * refillIntervalMs);
      setTimeout(() => pump(key), waitMs);
    }
  }

  function limitedFetch(input, init) {
    // Normalize to a Request so we can derive the key and forward the signal.
    const req = (input instanceof Request) ? input : new Request(input, init);
    const key = keyOf(req);
    const bucket = getBucket(key);

    if (bucket.queue.length >= maxQueue) {
      const err = new Error(`Rate limit queue full for key "${key}" (size=${bucket.queue.length})`);
      err.name = 'RateLimitQueueFullError';
      if (onRefuse) {
        try { onRefuse({ key, queueSize: bucket.queue.length }); } catch {}
      }
      return Promise.reject(err);
    }

    refill(bucket);

    return new Promise((resolve, reject) => {
      let settled = false;

      const onAbort = () => {
        if (settled) return;
        settled = true;
        // Remove from queue if still waiting.
        const idx = bucket.queue.findIndex(w => w.resolve === resolve);
        if (idx !== -1) {
          bucket.queue.splice(idx, 1);
          bucket.size -= 1;
        }
        const e = new DOMException('The operation was aborted.', 'AbortError');
        reject(e);
      };

      if (req.signal) {
        if (req.signal.aborted) {
          onAbort();
          return;
        }
        req.signal.addEventListener('abort', onAbort, { once: true });
      }

      bucket.queue.push({
        resolve: (res) => {
          if (settled) return;
          settled = true;
          resolve(res);
        },
        reject: (err) => {
          if (settled) return;
          settled = true;
          reject(err);
        },
        input: req,
        init: undefined, // Request already carries method/body/headers
      });
      bucket.size += 1;
      pump(key);
    });
  }

  // Expose introspection helpers (handy for tests and metrics).
  limitedFetch.rateLimit = {
    snapshot() {
      const out = {};
      for (const [k, b] of buckets) {
        refill(b);
        out[k] = { tokens: b.tokens, queued: b.queue.length };
      }
      return out;
    },
    reset(key) {
      if (key === undefined) buckets.clear();
      else buckets.delete(key);
    },
  };

  return limitedFetch;
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
