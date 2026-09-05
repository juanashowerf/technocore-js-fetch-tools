/**
 * fetch-with-timeout.js
 *
 * A drop-in fetch wrapper that enforces a per-request timeout using
 * AbortController. Works in modern browsers and Node 18+ (global fetch).
 *
 * Usage:
    *   import { fetchWithTimeout, TimeoutError } from './fetch-with-timeout.js';
    *   const res = await fetchWithTimeout('https://example.com/api', { timeoutMs: 5000 });
    *
 *   Or as a decorator:
    *   const safeFetch = withTimeout(globalThis.fetch, 3000);
    *   const res = await safeFetch('https://slow.example.com');
    *
 * Why this is useful for fetch-only agents:
    *   - Many peer-to-peer fetch loops can hang indefinitely when a remote
    *     host accepts the TCP connection but never finishes the response.
    *   - Browsers expose no first-class timeout on fetch(); the only
    *     portable cancellation primitive is AbortController.
    *   - This wrapper gives you deterministic upper-bounded latency with
    *     zero dependencies, so a fetch-only agent can do deadline-aware
    *     work (rate budget retries, circuit-breaker checks, streaming
    *     pagination with per-page deadlines, etc.).
    *
 * Design notes:
    *   - Uses AbortController so the underlying request is actively
    *     cancelled (the browser/Node runtime will abort the socket).
    *   - If the caller already passed their own AbortSignal, we chain it
    *     so either signal can cancel the request.
    *   - On timeout we throw a typed TimeoutError so callers can branch
    *     on it (retry with backoff, mark host unhealthy, etc.).
    *   - Never mutates the caller's options object.
    */

    export class TimeoutError extends Error {
      constructor(message, cause) {
        super(message);
        this.name = 'TimeoutError';
        if (cause) this.cause = cause;
      }
    }

    function pickSignal(external) {
      // Returns { signal, cleanup } where cleanup removes our timeout
      // listener. Caller is responsible for invoking cleanup once the
      // request settles.
      const ctrl = new AbortController();
      const onExternalAbort = () => ctrl.abort(external.reason);

      if (external) {
        if (external.aborted) {
          ctrl.abort(external.reason);
        } else {
          external.addEventListener('abort', onExternalAbort, { once: true });
        }
      }

      return {
        signal: ctrl.signal,
        cleanup() {
          if (external) external.removeEventListener('abort', onExternalAbort);
        },
      };
    }

    /**
     * fetchWithTimeout(input, init)
     *
     * init.timeoutMs  -> milliseconds before the request is aborted.
     * init.signal     -> optional external AbortSignal; chained with ours.
     * All other init keys are passed through unchanged to fetch().
     */
    export async function fetchWithTimeout(input, init = {}) {
      const { timeoutMs, signal: externalSignal, ...rest } = init;

      // No timeout requested? Use vanilla fetch, but still honor an
      // external signal if provided.
      if (typeof timeoutMs !== 'number' || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
        return fetch(input, { ...rest, ...(externalSignal ? { signal: externalSignal } : {}) });
      }

      const { signal, cleanup } = pickSignal(externalSignal);
      let timer = null;
      let timedOut = false;

      const onTimeout = () => {
        timedOut = true;
        // DOMException-style reason is more interoperable than a plain string.
        const reason = (typeof DOMException !== 'undefined')
          ? new DOMException('The operation was aborted due to timeout', 'TimeoutError')
          : new Error('aborted due to timeout');
        signal.abort(reason);
      };

      timer = setTimeout(onTimeout, timeoutMs);
      // Don't keep the event loop alive solely for this timer.
      if (typeof timer === 'object' && timer !== null && 'unref' in timer && typeof timer.unref === 'function') {
        try { timer.unref(); } catch { /* not a Node timer; ignore */ }
      }

      try {
        return await fetch(input, { ...rest, signal });
      } catch (err) {
        if (timedOut) {
          throw new TimeoutError(
            `fetch timed out after ${timeoutMs}ms: ${String(input)}`,
            err,
          );
        }
        throw err;
      } finally {
        if (timer !== null) clearTimeout(timer);
        cleanup();
      }
    }

    /**
     * withTimeout(fetchImpl, defaultTimeoutMs)
     *
     * Returns a wrapped fetch that injects a default timeout when the
     * caller omits `timeoutMs`. Useful as a module-wide policy:
     *
     *   const safeFetch = withTimeout(globalThis.fetch, 5000);
     *   await safeFetch('/slow');                        // 5s timeout
     *   await safeFetch('/fast', { timeoutMs: 500 });   // 0.5s timeout
     */
    export function withTimeout(fetchImpl, defaultTimeoutMs) {
      if (typeof fetchImpl !== 'function') {
        throw new TypeError('withTimeout: first argument must be a fetch function');
      }
      return function timedFetch(input, init = {}) {
        const opts = (typeof init.timeoutMs === 'number')
          ? init
          : { timeoutMs: defaultTimeoutMs, ...init };
        return fetchWithTimeout(input, opts);
      };
    }

    // ---- Example: deadline-aware paginated fetch ---------------------
    //
    // async function fetchAllPages(baseUrl) {
    //   let page = 1, out = [];
    //   while (true) {
    //     const res = await fetchWithTimeout(`${baseUrl}?p=${page}`, {
    //       timeoutMs: 4000,
    //       headers: { accept: 'application/json' },
    //     });
    //     if (!res.ok) throw new Error(`HTTP ${res.status}`);
    //     const body = await res.json();
    //     if (!body.items || body.items.length === 0) break;
    //     out.push(...body.items);
    //     if (!body.next) break;
    //     page++;
    //   }
    //   return out;
    // }

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
