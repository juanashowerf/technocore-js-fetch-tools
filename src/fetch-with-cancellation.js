/**
 * fetch-with-cancellation.js
 *
 * A drop-in wrapper around the standard `fetch` API that adds first-class
 * cancellation, including cooperative cancellation across composed fetch
 * pipelines. Designed for fetch-only browser/JS agents that need to abort
 * in-flight network work without abandoning the underlying request promise.
 *
 * Why cancellation matters for fetch-only agents:
 *   - Long-polling and streaming responses can keep connections open
 *     indefinitely, wasting sockets and quota.
 *   - User navigation away or a higher-priority task should be able to
 *     stop pending requests cleanly.
 *   - Composing wrappers (retry, backpressure, circuit-breaker) makes it
 *     tricky to cancel the *outer* operation; this helper propagates abort
 *     to every layer.
 *
 * Features:
 *   - Returns { response, cancel } so callers can abort after dispatch.
 *   - Honors an external AbortSignal AND exposes its own signal.
 *   - AbortError is normalized to a tagged error for easy `instanceof` checks.
 *   - Optional `cleanup` hook fires on cancel/complete/error.
 *
 * Usage:
 *   import { fetchWithCancellation, FetchCancelledError } from './fetch-with-cancellation.js';
 *
 *   const { response, cancel } = fetchWithCancellation(url, init);
 *   setTimeout(cancel, 5000); // give up after 5s
 *   try {
 *     const text = await response.then(r => r.text());
 *   } catch (e) {
 *     if (e instanceof FetchCancelledError) console.log('aborted');
 *   }
 */

export class FetchCancelledError extends Error {
  constructor(reason = 'aborted') {
    super(`Fetch cancelled: ${reason}`);
    this.name = 'FetchCancelledError';
    this.code = 'FETCH_CANCELLED';
  }
}

/**
 * fetchWithCancellation(input, init = {}, opts = {}) -> { response, cancel, signal }
 *
 * @param {RequestInfo|string} input   URL or Request passed through to fetch.
 * @param {RequestInit} [init]         Standard fetch init object.
 * @param {object} [opts]
 * @param {AbortSignal} [opts.signal]  External signal; cancellation propagates both ways.
 * @param {string} [opts.reason]       Reason string recorded on cancel.
 * @param {() => void} [opts.onCleanup] Invoked once on cancel/complete/error.
 *
 * Returns an object:
 *   - response: Promise<Response> that rejects with FetchCancelledError on cancel.
 *   - cancel(reason?): imperative cancel function.
 *   - signal: AbortSignal owned by this call (link with `signal` for composition).
 */
export function fetchWithCancellation(input, init = {}, opts = {}) {
  const externalSignal = opts.signal || (init && init.signal) || undefined;
  const reason0 = opts.reason || 'caller-requested';
  const onCleanup = typeof opts.onCleanup === 'function' ? opts.onCleanup : null;

  const controller = new AbortController();
  let cancelled = false;
  let cleanupDone = false;

  function fireCleanup(err) {
    if (cleanupDone) return;
    cleanupDone = true;
    if (onCleanup) {
      try { onCleanup(err); } catch (_) { /* swallow handler errors */ }
    }
  }

  function cancel(reason) {
    if (cancelled) return;
    cancelled = true;
    try { controller.abort(reason || reason0); } catch (_) { /* already aborted */ }
    fireCleanup(new FetchCancelledError(reason || reason0));
  }

  // Bridge: if external signal fires, cancel us too.
  let externalListener;
  if (externalSignal) {
    if (externalSignal.aborted) {
      // Already aborted before we even started.
      cancel('external-signal-pre-aborted');
    } else {
      externalListener = () => cancel('external-signal');
      externalSignal.addEventListener('abort', externalListener, { once: true });
    }
  }

  const initWithSignal = { ...init, signal: controller.signal };

  const response = fetch(input, initWithSignal).then(
    (res) => {
      fireCleanup(null);
      if (externalListener && externalSignal) {
        externalSignal.removeEventListener('abort', externalListener);
      }
      return res;
    },
    (err) => {
      fireCleanup(err);
      if (externalListener && externalSignal) {
        externalSignal.removeEventListener('abort', externalListener);
      }
      // Normalize AbortError to FetchCancelledError so callers have one type to check.
      if (err && (err.name === 'AbortError' || err.code === 'ABORT_ERR')) {
        throw new FetchCancelledError('fetch-abort');
      }
      throw err;
    }
  );

  return { response, cancel, signal: controller.signal };
}

/* ------------------------------------------------------------------
 * Composed example: cancel a retry chain.
 *
 *   import { fetchWithCancellation } from './fetch-with-cancellation.js';
 *
 *   async function fetchWithRetry(url, attempts = 3) {
 *     const { response, cancel } = fetchWithCancellation(url);
 *     try {
 *       const res = await response;
 *       if (!res.ok && attempts > 1) {
 *         cancel('retry');
 *         return fetchWithRetry(url, attempts - 1);
 *       }
 *       return res;
 *     } catch (e) {
 *       if (attempts > 1) return fetchWithRetry(url, attempts - 1);
 *       throw e;
 *     }
 *   }
 *
 * The `cancel()` handle lets a higher layer (timeout, circuit-breaker,
 * user navigation) abort the whole chain without leaking sockets.
 * ------------------------------------------------------------------ */

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
