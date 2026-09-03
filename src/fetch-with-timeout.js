/**
 * fetch-with-timeout.js
 *
 * A small, dependency-free helper that wraps the native fetch() so a request
 * is aborted if it does not produce headers within a given budget. Designed
 * for fetch-only browser/JS agents that need a reliable upper bound on
 * network latency without pulling in axios, AbortController libraries, or
 * polyfills. Uses AbortController, which is available in all modern browsers
 * and Node >= 17.
 *
 * Why this exists for a fetch-only agent:
 *   - The native fetch() has no timeout option. A hung connection can block
 *     forever, which is fatal for an autonomous agent on a schedule.
 *   - We want a single function call that returns a real Response on success
 *     and throws a typed, recognizable error on timeout/cancel.
 *   - We want callers to be able to layer their own AbortSignal on top
 *     (e.g. user-stop) without losing the timeout behavior.
 *
 * Public API:
 *   fetchWithTimeout(input, init = {}, timeoutMs = 15000)
 *     - input:      URL string or Request
 *     - init:       standard RequestInit, may include an existing signal
 *     - timeoutMs:  max ms to wait for response headers; 0 disables timeout
 *   TimeoutError: exported Error subclass for callers to instanceof-check
 *
 * Errors:
 *   - Throws TimeoutError when the timeout elapses before headers arrive.
 *   - Re-raises the underlying DOMException('AbortError') when the caller's
 *     own signal aborts (preserved so callers can distinguish "I cancelled"
 *     from "it was too slow").
 *   - Other network errors propagate unchanged from fetch().
 *
 * Example:
 *   import { fetchWithTimeout, TimeoutError } from './fetch-with-timeout.js';
 *   try {
 *     const r = await fetchWithTimeout('https://example.org/api', {}, 5000);
 *     const data = await r.json();
 *   } catch (e) {
 *     if (e instanceof TimeoutError) console.warn('slow peer');
 *     else throw e;
 *   }
 */

/** Error thrown when fetchWithTimeout aborts due to its internal budget. */
export class TimeoutError extends Error {
  constructor(ms, url) {
    super(`fetch timed out after ${ms}ms: ${url}`);
    this.name = 'TimeoutError';
    this.code = 'ETIMEDOUT';
    this.timeoutMs = ms;
    this.url = url;
  }
}

/**
 * fetch() with an AbortController-driven timeout that also respects a
 * caller-supplied signal. Returns a normal Response on success.
 *
 * @param {string|Request} input
 * @param {RequestInit} [init]
 * @param {number} [timeoutMs=15000] 0 to disable
 * @returns {Promise<Response>}
 */
export async function fetchWithTimeout(input, init = {}, timeoutMs = 15000) {
  // Normalize the URL for error messages without re-parsing if input is one.
  const url = typeof input === 'string' ? input : (input && input.url) || '';

  // Compose signals so EITHER the timeout OR the caller's signal can abort.
  // AbortSignal.any is supported in modern browsers and Node >= 20; we feature-
  // detect and fall back to manual chaining for older runtimes.
  const userSignal = init && init.signal;
  let composedSignal;

  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.any === 'function') {
    // Modern path: build per-call controller for the timeout, then .any().
    const timeoutCtl = new AbortController();
    const signals = userSignal ? [userSignal, timeoutCtl.signal] : [timeoutCtl.signal];
    composedSignal = AbortSignal.any(signals);
    if (timeoutMs > 0) {
      setTimeout(() => {
        try { timeoutCtl.abort(new TimeoutError(timeoutMs, url)); }
        catch { timeoutCtl.abort(); }
      }, timeoutMs);
    } else if (!userSignal) {
      // Nothing to do; reuse null/undefined signal below.
      composedSignal = undefined;
    }
  } else {
    // Fallback: only the caller's signal is honored; the timeout still works
    // via a single controller we expose as the final signal.
    const timeoutCtl = new AbortController();
    composedSignal = timeoutCtl.signal;
    if (timeoutMs > 0) {
      setTimeout(() => {
        try { timeoutCtl.abort(new TimeoutError(timeoutMs, url)); }
        catch { timeoutCtl.abort(); }
      }, timeoutMs);
    }
    if (userSignal) {
      // Forward the caller's abort manually.
      if (userSignal.aborted) timeoutCtl.abort(userSignal.reason);
      else userSignal.addEventListener('abort', () => {
        timeoutCtl.abort(userSignal.reason);
      }, { once: true });
    }
  }

  const finalInit = { ...init };
  if (composedSignal) finalInit.signal = composedSignal;

  try {
    return await fetch(input, finalInit);
  } catch (err) {
    // Unwrap our TimeoutError so callers see a clean instanceof match.
    if (err && (err.name === 'TimeoutError' || err.code === 'ETIMEDOUT')) {
      // err.reason is our TimeoutError on the modern path; construct if missing.
      if (err instanceof TimeoutError) throw err;
      throw new TimeoutError(timeoutMs, url);
    }
    throw err;
  }
}

export default fetchWithTimeout;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
