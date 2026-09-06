// src/fetch-with-coalescing.js
// Coalesce concurrent identical in-flight GETs so only one network request runs
// while every caller awaiting the same (method, url) receives the same Response
// (or error). This is request deduplication at the Promise level, distinct from
// fetch-with-deduplication.js which keys on a user-supplied cache key + TTL.
//
// Browser/Node 18+ fetch only. Zero dependencies. No build step.
//
// Usage:
//   const fetchC = coalesceFetch();
//   const [a, b, c] = await Promise.all([
//     fetchC('/api/user/42'),
//     fetchC('/api/user/42'),
//     fetchC('/api/user/42'),
//   ]);
//   // -> exactly one network request; a === b === c (same Response instance)
//
// Options:
//   fetchC(url, init)             -> standard fetch signature
//   fetchC.clear()                -> drop all pending entries (does not abort)
//   fetchC.size                   -> number of currently in-flight coalesced groups
//
// Notes:
//   - Only GETs are coalesced by default (mutating verbs must not coalesce).
//     Pass { methods: ['GET','HEAD'] } to extend.
//   - The shared Response's body is consumed by the first reader; subsequent
//     awaiters get a cloned Response via response.clone() semantics. We expose
//     this by returning the SAME Response object: callers that need to read
//     the body must call .clone() themselves, exactly like normal fetch.
//   - On error, every awaiter rejects with the same Error.

const DEFAULT_METHODS = ['GET'];

function coalesceFetch(options = {}) {
  const allowed = new Set((options.methods || DEFAULT_METHODS).map(m => m.toUpperCase()));
  const pending = new Map(); // key -> { promise, controllers }

  function keyFor(url, init) {
    const method = ((init && init.method) || 'GET').toUpperCase();
    return method + ' ' + url;
  }

  function coalesced(url, init) {
    if (!allowed.has(((init && init.method) || 'GET').toUpperCase())) {
      return fetch(url, init);
    }
    const key = keyFor(url, init);
    const entry = pending.get(key);
    if (entry) {
      entry.refs += 1;
      return entry.promise;
    }

    const controller = (typeof AbortController === 'function') ? new AbortController() : null;
    const mergedInit = controller
      ? Object.assign({}, init || {}, { signal: controller.signal })
      : init;

    const refs = 1;
    const promise = (async () => {
      try {
        const res = await fetch(url, mergedInit);
        return res;
      } finally {
        // Remove entry once settled so later calls start a fresh request.
        pending.delete(key);
      }
    })();

    pending.set(key, { promise, refs, controller });

    // Attach an unhandled-rejection guard so one awaiter can't poison others
    // if they forget to .catch. The shared promise is still the same object.
    promise.catch(() => {});

    return promise;
  }

  coalesced.clear = () => pending.clear();
  Object.defineProperty(coalesced, 'size', { get: () => pending.size });
  return coalesced;
}

// Export for CommonJS / ESM / browser-global without a build step.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { coalesceFetch };
}
if (typeof window !== 'undefined') {
  window.coalesceFetch = coalesceFetch;
}
export { coalesceFetch };

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
