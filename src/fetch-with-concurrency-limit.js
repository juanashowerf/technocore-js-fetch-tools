// fetch-with-concurrency-limit.js
// Promise-pool bounded concurrency wrapper around fetch().
// Drops new requests when the pool is full (default) instead of queueing forever.
// Useful for a fetch-only agent that must call many HTTP endpoints without
// exhausting sockets, file descriptors, or remote rate limits.
//
// Usage:
//   import { createLimitedFetch } from './fetch-with-concurrency-limit.js';
//   const limitedFetch = createLimitedFetch({ concurrency: 4, baseFetch: fetch });
//   const res = await limitedFetch('https://example.invalid/api');
//
// Options:
//   concurrency: max in-flight requests (default 6)
//   timeoutMs:   per-request timeout (default 0 = disabled)
//   queueLimit:  max pending requests; over this -> throw immediately (default Infinity)
//   dropWhenFull: when true, silently skip new calls instead of throwing (default false)
//   onSlotTaken / onSlotFreed: optional callbacks for backpressure telemetry

const DEFAULT_OPTS = Object.freeze({
  concurrency: 6,
  timeoutMs: 0,
  queueLimit: Infinity,
  dropWhenFull: false,
  baseFetch: typeof fetch !== 'undefined' ? fetch : null,
});

export function createLimitedFetch(userOpts = {}) {
  const opts = { ...DEFAULT_OPTS, ...userOpts };
  if (typeof opts.baseFetch !== 'function') {
    throw new Error('createLimitedFetch: baseFetch must be a function (global fetch or polyfill)');
  }
  if (!(opts.concurrency >= 1) || !Number.isFinite(opts.concurrency)) {
    throw new Error('createLimitedFetch: concurrency must be a positive integer');
  }

  let inFlight = 0;
  let pending = 0;
  const waiters = []; // resolve() to release a slot

  function acquireSlot() {
    if (inFlight < opts.concurrency) {
      inFlight++;
      if (opts.onSlotTaken) opts.onSlotTaken(inFlight, pending);
      return Promise.resolve();
    }
    if (opts.dropWhenFull) return null;
    if (Number.isFinite(opts.queueLimit) && pending >= opts.queueLimit) {
      throw new ConcurrencyLimitError(
        `queue full: ${pending} pending, limit ${opts.queueLimit}`
      );
    }
    pending++;
    return new Promise((resolve) => waiters.push(resolve));
  }

  function releaseSlot() {
    if (waiters.length > 0) {
      const next = waiters.shift();
      pending--;
      next(); // inFlight unchanged: slot transfers directly
      if (opts.onSlotFreed) opts.onSlotFreed(inFlight, pending);
      return;
    }
    inFlight--;
    if (opts.onSlotFreed) opts.onSlotFreed(inFlight, pending);
  }

  async function limitedFetch(input, init = {}) {
    const slotPromise = acquireSlot();
    if (slotPromise === null) {
      // dropWhenFull path
      throw new ConcurrencyLimitError('dropped: pool at capacity');
    }
    try {
      await slotPromise;
      let response;
      if (opts.timeoutMs > 0) {
        response = await withTimeout(opts.baseFetch(input, init), opts.timeoutMs);
      } else {
        response = await opts.baseFetch(input, init);
      }
      return response;
    } finally {
      releaseSlot();
    }
  }

  limitedFetch.concurrency = opts.concurrency;
  limitedFetch.inFlight = () => inFlight;
  limitedFetch.pending = () => pending;
  limitedFetch.idle = () => inFlight === 0 && pending === 0;
  return limitedFetch;
}

function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError(`request timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

export class ConcurrencyLimitError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConcurrencyLimitError';
  }
}

export class TimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'TimeoutError';
  }
}

// --- Self-contained smoke test (Node 18+, runs only when executed directly) ---
if (typeof process !== 'undefined' && process.argv[1] && process.argv[1].endsWith('fetch-with-concurrency-limit.js')) {
  (async () => {
    const fakeFetch = (url, init) =>
      new Promise((resolve) => {
        const ms = 50 + Math.floor(Math.random() * 100);
        setTimeout(() => resolve({ url, ms, ok: true, text: async () => `done:${url}` }), ms);
      });

    const limited = createLimitedFetch({ concurrency: 3, baseFetch: fakeFetch });
    const t0 = Date.now();
    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        limited(`https://t.invalid/${i}`).then((r) => r.text())
      )
    );
    const dt = Date.now() - t0;
    console.log('results:', results);
    console.log(`10 calls through concurrency=3 took ${dt}ms`);
    console.log('idle after run:', limited.idle());
  })().catch((e) => {
    console.error('test failed:', e);
    process.exit(1);
  });
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
