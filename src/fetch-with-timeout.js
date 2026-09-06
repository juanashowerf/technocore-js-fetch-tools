/**
 * fetch-with-timeout.js
 *
 * A drop-in fetch wrapper that enforces a per-request timeout using only
 * the standard AbortController API. Works in modern browsers and Node >= 17
 * (which exposes globalThis.fetch and AbortController). No build step.
 *
 * Why this matters for fetch-only agents:
 *   - Native fetch has no timeout; a hung server can pin an agent forever.
 *   - Promises.race(setTimeout) leaks the underlying request and prevents
 *     the socket from being reused.
 *   - This wrapper properly signals cancellation so the body stream is
 *     released and pooled connections can recover.
 *
 * Usage:
 *   import { fetchWithTimeout, TimeoutError } from './fetch-with-timeout.js';
 *   const res = await fetchWithTimeout('https://example.com/api', {
 *     timeoutMs: 5000,
 *     fetchOptions: { method: 'POST', body: JSON.stringify(data) },
 *   });
 *
 *   // Or compose with your own signal:
 *   const ctrl = new AbortController();
 *   ctrl.abort(new Error('user navigated away'));
 *   try {
 *     await fetchWithTimeout(url, { signal: ctrl.signal, timeoutMs: 3000 });
 *   } catch (e) {
 *     if (e instanceof TimeoutError) console.log('slow upstream');
 *     else throw e;
 *   }
 */

export class TimeoutError extends DOMException {
  constructor(ms) {
    super(`Request timed out after ${ms}ms`, 'TimeoutError');
    this.name = 'TimeoutError';
    this.timeoutMs = ms;
  }
}

/**
 * Perform a fetch with an enforced timeout.
 *
 * @param {string|Request} input   URL or Request object.
 * @param {object}  opts
 * @param {number}  [opts.timeoutMs=10000]   Max wait before aborting.
 * @param {AbortSignal} [opts.signal]        Caller-provided abort signal.
 * @param {object}  [opts.fetchOptions]      Extra init passed to fetch().
 * @param {typeof fetch} [opts.fetcher]      Injectable fetch (tests, polyfills).
 * @returns {Promise<Response>}
 */
export async function fetchWithTimeout(input, opts = {}) {
  const {
    timeoutMs = 10_000,
    signal: externalSignal,
    fetchOptions = {},
    fetcher = (typeof fetch === 'function')
      ? fetch
      : (typeof globalThis !== 'undefined' && globalThis.fetch) || null,
  } = opts;

  if (!fetcher) throw new Error('No fetch implementation available in this runtime');
  if (!(timeoutMs > 0)) throw new TypeError('timeoutMs must be a positive number');

  // Merge caller signal with our timeout signal.
  const controller = new AbortController();
  let timedOut = false;

  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort(new TimeoutError(timeoutMs));
  }, timeoutMs);

  // If the caller aborts, propagate immediately and clear the timer.
  const onExternalAbort = () => controller.abort(externalSignal.reason);
  if (externalSignal) {
    if (externalSignal.aborted) {
      clearTimeout(timer);
      throw externalSignal.reason ?? new DOMException('Aborted', 'AbortError');
    }
    externalSignal.addEventListener('abort', onExternalAbort, { once: true });
  }

  try {
    const init = { ...fetchOptions, signal: controller.signal };
    const response = await fetcher(input, init);

    // Defensive: make sure the response body is also cancelled if the
    // caller aborts while we are streaming.
    if (externalSignal) {
      externalSignal.addEventListener('abort', () => {
        try { response.body && response.body.cancel(); } catch (_) { /* noop */ }
      }, { once: true });
    }
    return response;
  } catch (err) {
    if (timedOut && (err && (err.name === 'AbortError' || err instanceof TimeoutError))) {
      // Some runtimes surface their own AbortError on timeout; normalize it.
      throw new TimeoutError(timeoutMs);
    }
    throw err;
  } finally {
    clearTimeout(timer);
    if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
  }
}

// Example / smoke test (run with: node src/fetch-with-timeout.js)
if (typeof require !== 'undefined' && require.main === module) {
  (async () => {
    const cases = [
      { url: 'https://httpbin.org/delay/1', timeout: 3000, expect: 'ok' },
      { url: 'https://httpbin.org/delay/5', timeout: 1000, expect: 'timeout' },
      { url: 'https://httpbin.org/status/500', timeout: 2000, expect: 'http500' },
    ];
    for (const c of cases) {
      const t0 = Date.now();
      try {
        const r = await fetchWithTimeout(c.url, { timeoutMs: c.timeout });
        console.log(`OK  ${c.url} -> ${r.status} in ${Date.now() - t0}ms`);
      } catch (e) {
        console.log(`ERR ${c.url} -> ${e.name} (${e.message}) in ${Date.now() - t0}ms`);
      }
    }
  })();
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
