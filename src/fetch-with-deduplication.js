/**
 * fetch-with-deduplication.js
 *
 * Wraps a fetch function so that identical in-flight requests are coalesced
 * into a single underlying call. Useful for agents that react to bursts of
 * duplicate stimuli (e.g. many peers asking for the same /agents endpoint at
 * almost the same time) and want to avoid hammering upstream services.
 *
 * The dedupe key is derived from method + URL + JSON-serialized body.
 * The first caller registers a Promise; concurrent callers attach to the same
 * Promise. When the underlying fetch settles (success OR failure), all waiters
 * resolve/reject together and the entry is cleared, so subsequent calls run
 * fresh.
 *
 * Pure JS, no build step. Works in Node 18+ (global fetch) and modern browsers.
 *
 *   import fetchDedupe from './fetch-with-deduplication.js';
 *   const fetch = fetchDedupe(globalThis.fetch);
 *
 *   // Two parallel calls with identical args => one network request.
 *   const [a, b] = await Promise.all([fetch(u), fetch(u)]);
 */

const DEFAULT_KEY = (method, url, body) => {
  let bodyKey = '';
  if (body !== undefined && body !== null) {
    if (typeof body === 'string') bodyKey = body;
    else {
      try { bodyKey = JSON.stringify(body); }
      catch { bodyKey = String(body); }
    }
  }
  return method.toUpperCase() + ' ' + url + ' :: ' + bodyKey;
};

export default function fetchWithDeduplication(baseFetch, opts = {}) {
  if (typeof baseFetch !== 'function') {
    throw new TypeError('fetchWithDeduplication: baseFetch must be a function');
  }
  const keyFn = typeof opts.keyFn === 'function' ? opts.keyFn : DEFAULT_KEY;
  const inflight = new Map();
  const stats = { hits: 0, misses: 0 };

  const dedupedFetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : (input && input.url) || String(input);
    const method = (init.method || (input && input.method) || 'GET');
    const body = init.body !== undefined ? init.body : (input && input.body);
    const key = keyFn(method, url, body);

    const existing = inflight.get(key);
    if (existing) {
      stats.hits += 1;
      return existing.promise;
    }
    stats.misses += 1;

    // Build a single shared promise; attach settle handlers to clear the slot
    // so the next call after completion runs a fresh request.
    let resolveOuter, rejectOuter;
    const promise = new Promise((res, rej) => { resolveOuter = res; rejectOuter = rej; });
    inflight.set(key, { promise });

    baseFetch(input, init).then(
      (res) => {
        // If upstream returned a Response with a streaming body, downstream
        // callers each need their own readable copy. Clone before resolving.
        try {
          if (res && typeof res.clone === 'function') resolveOuter(res.clone());
          else resolveOuter(res);
        } catch (e) {
          rejectOuter(e);
        } finally {
          inflight.delete(key);
        }
      },
      (err) => {
        inflight.delete(key);
        rejectOuter(err);
      }
    );

    return promise;
  };

  // Expose inspection helpers — handy in tests and agent self-reports.
  dedupedFetch.inflightSize = () => inflight.size;
  dedupedFetch.stats = () => ({ ...stats });
  dedupedFetch.clear = () => inflight.clear();

  return dedupedFetch;
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
