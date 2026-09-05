/**
 * fetch-with-coalescing.js
 *
 * Coalesce concurrent identical in-flight fetch requests so that N callers
 * asking for the same resource share a single underlying HTTP request and
 * a single parsed response. Useful when multiple agents in the same process
 * (or a UI with many subscribers) hammer the same endpoint.
 *
 * - Pure browser/Node 18+ — no build step, no dependencies.
 * - Uses AbortController so that if ANY caller aborts, the shared request
 *   is aborted (the underlying network call is shared, not the abort state).
 * - Optional response transformer (default: response.clone().json()).
 * - LRU-ish eviction by maxEntries to bound memory.
 *
 * Usage:
 *   const fetchC = makeCoalescingFetch(globalThis.fetch);
 *   const [a, b] = await Promise.all([
 *     fetchC('https://api.example.com/v1/items/42'),
 *     fetchC('https://api.example.com/v1/items/42'),
 *   ]);
 *   // Only ONE network request was made; both promises resolve to the same data.
 */

export function makeCoalescingFetch(baseFetch, opts = {}) {
  const maxEntries = opts.maxEntries ?? 100;
  const transform = opts.transform || (async (res) => res.clone().json());
  const inFlight = new Map(); // key -> { controller, responsePromise, refCount }

  function keyOf(input, init) {
    const url = typeof input === 'string' ? input : input.url;
    const method = (init && init.method) || (typeof input === 'object' && input.method) || 'GET';
    // Only safe to coalesce idempotent, body-less reads.
    if (method.toUpperCase() !== 'GET' && method.toUpperCase() !== 'HEAD') return null;
    // Body requests cannot be safely shared across callers.
    if (init && init.body) return null;
    return method.toUpperCase() + ' ' + url;
  }

  function evictIfNeeded() {
    while (inFlight.size > maxEntries) {
      const oldestKey = inFlight.keys().next().value;
      const entry = inFlight.get(oldestKey);
      // Don't evict entries that still have callers.
      if (entry.refCount > 0) break;
      inFlight.delete(oldestKey);
    }
  }

  async function coalescedFetch(input, init) {
    const key = keyOf(input, init);
    if (!key) return transform(await baseFetch(input, init));

    const existing = inFlight.get(key);
    if (existing) {
      existing.refCount++;
      try {
        const res = await existing.responsePromise;
        if (res.aborted) throw Object.assign(new Error('aborted'), { name: 'AbortError' });
        return transform(res);
      } finally {
        existing.refCount--;
        if (existing.refCount <= 0) inFlight.delete(key);
      }
    }

    // New entry: create a controller that will be aborted if any caller aborts.
    const controller = new AbortController();
    const callerControllers = new Set();
    const callInit = { ...(init || {}), signal: controller.signal };

    const responsePromise = baseFetch(input, callInit)
      .catch((err) => {
        // Propagate to all waiters, then drop the entry.
        inFlight.delete(key);
        throw err;
      });

    const entry = { controller, responsePromise, refCount: 1 };
    inFlight.set(key, entry);
    evictIfNeeded();

    // Per-caller abort: merge into the shared controller.
    if (init && init.signal) {
      if (init.signal.aborted) controller.abort();
      else {
        const callerCtrl = { aborted: false };
        const onAbort = () => { if (!controller.signal.aborted) controller.abort(); };
        init.signal.addEventListener('abort', onAbort, { once: true });
        callerControllers.add(callerCtrl);
      }
    }

    try {
      const res = await responsePromise;
      if (controller.signal.aborted) throw Object.assign(new Error('aborted'), { name: 'AbortError' });
      return transform(res);
    } finally {
      inFlight.delete(key);
    }
  }

  // Expose introspection for tests / debugging.
  coalescedFetch._inflight = () => Array.from(inFlight.keys());
  coalescedFetch._size = () => inFlight.size;
  return coalescedFetch;
}

// Self-contained smoke test when run directly (node src/fetch-with-coalescing.js).
if (typeof process !== 'undefined' && process.argv[1] && process.argv[1].endsWith('fetch-with-coalescing.js')) {
  let calls = 0;
  const fakeFetch = async (url) => {
    calls++;
    await new Promise((r) => setTimeout(r, 10));
    return new Response(JSON.stringify({ url, n: calls }), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    });
  };
  const fc = makeCoalescingFetch(fakeFetch);
  const t0 = Date.now();
  const results = await Promise.all([fc('https://x/a'), fc('https://x/a'), fc('https://x/b')]);
  console.log('calls=' + calls, 'results=' + JSON.stringify(results), 'ms=' + (Date.now() - t0));
  if (calls !== 2) { console.error('FAIL: expected 2 underlying calls, got ' + calls); process.exit(1); }
  if (results[0].url !== 'https://x/a' || results[1].url !== 'https://x/a') {
    console.error('FAIL: shared result wrong'); process.exit(1);
  }
  console.log('OK');
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
