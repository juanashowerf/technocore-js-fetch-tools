/**
 * fetch-with-timeout.js
 *
 * Drop-in enhancement over the global fetch() that enforces a hard wall-clock
 * deadline on every call. Designed so a fetch-only agent (no AbortController
 * wiring, no streaming complexity to worry about) gets a reliable upper bound
 * on network latency.
 *
 * Why this matters:
 *   - Default fetch() in browsers has NO timeout. A stalled TCP socket, a
 *     slow DNS lookup, or a hung server keeps the request pending forever.
 *   - Many public APIs expose AbortController, but agents that only ship a
 *     minimal fetch polyfill still need a defensive timeout story.
 *   - Combining with other tools in this repo (retry, circuit-breaker,
 *     rate-limit) is straightforward because this module exports a plain
 *     async function with the same shape as fetch().
 *
 * Usage:
 *   import fetchTimeout from './fetch-with-timeout.js';
 *
 *   // 5 second default
 *   const r = await fetchTimeout('https://example.com/api');
 *
 *   // Per-call override
 *   const r = await fetchTimeout('https://slow.example.com', { timeoutMs: 8000 });
 *
 *   // Caller-supplied signal still wins (caller can cancel earlier)
 *   const ctrl = new AbortController();
 *   setTimeout(() => ctrl.abort(), 2000);
 *   const r = await fetchTimeout(url, { signal: ctrl.signal, timeoutMs: 10000 });
 *
 *   // Streaming responses: timeout fires when the *headers* haven't arrived
 *   // by the deadline, not when the body is slow. Pass streamTimeoutMs:true
 *   // to instead bound the *whole* transfer (body included).
 *
 * Errors:
 *   - On timeout a DOMException with name 'TimeoutError' is thrown, matching
 *     the WHATWG Fetch spec shape (so polyfill users get a familiar type).
 *   - Underlying fetch errors (network failure, abort) propagate unchanged.
 *
 * Browser-only; relies on AbortController and DOMException. Node 18+ has
 * both globally, so the same file runs in either environment.
 */

const DEFAULT_TIMEOUT_MS = 10_000;

/**
 * @param {string|Request} input
 * @param {object} [init]
 * @param {number} [init.timeoutMs=10000] Max ms to wait for response headers.
 * @param {boolean} [init.streamTimeoutMs=false] If true, timeout also covers
 *        the body download. If false (default), only header arrival is bounded.
 * @param {AbortSignal} [init.signal] Caller-supplied cancellation signal.
 * @returns {Promise<Response>}
 */
export default async function fetchWithTimeout(input, init = {}) {
  const {
    timeoutMs = DEFAULT_TIMEOUT_MS,
    streamTimeoutMs = false,
    signal: externalSignal,
    ...rest
  } = init;

  // Compose external + internal signals so either side can cancel.
  const controller = new AbortController();
  let timedOut = false;
  let externalAbortHandler = null;

  const timeoutHandle = setTimeout(() => {
    timedOut = true;
    // Prefer the spec name 'TimeoutError' for ergonomic catch sites.
    const err = new DOMException(
      `fetch timed out after ${timeoutMs}ms`,
      'TimeoutError'
    );
    controller.abort(err);
  }, timeoutMs);

  if (externalSignal) {
    if (externalSignal.aborted) {
      clearTimeout(timeoutHandle);
      throw externalSignal.reason ?? new DOMException('Aborted', 'AbortError');
    }
    externalAbortHandler = () => {
      clearTimeout(timeoutHandle);
      controller.abort(externalSignal.reason ?? new DOMException('Aborted', 'AbortError'));
    };
    externalSignal.addEventListener('abort', externalAbortHandler, { once: true });
  }

  try {
    const response = await fetch(input, { ...rest, signal: controller.signal });

    // Optional: also bound the body download. Useful for huge payloads on
    // flaky connections. We re-arm a timer that aborts if no body chunk
    // arrives within `timeoutMs` of the previous one.
    if (streamTimeoutMs && response.body) {
      return await enforceBodyTimeout(response, timeoutMs, controller, timeoutHandle);
    }

    return response;
  } catch (err) {
    // Re-throw with a clearer type if we were the ones who aborted.
    if (timedOut) {
      throw new DOMException(
        `fetch timed out after ${timeoutMs}ms`,
        'TimeoutError'
      );
    }
    throw err;
  } finally {
    clearTimeout(timeoutHandle);
    if (externalSignal && externalAbortHandler) {
      externalSignal.removeEventListener('abort', externalAbortHandler);
    }
  }
}

/**
 * Wraps a streaming Response so an idle body (no chunks for `idleMs`) aborts.
 * Returns a new Response with the same status/headers but a tee'd body that
 * throws TimeoutError if the source stalls.
 */
async function enforceBodyTimeout(response, idleMs, controller, parentHandle) {
  const reader = response.body.getReader();
  const stream = new ReadableStream({
    async pull(ctrl) {
      // Reset the idle timer on each successful read.
      clearTimeout(parentHandle);
      const next = setTimeout(() => {
        controller.abort(new DOMException('Body read timed out', 'TimeoutError'));
        reader.cancel().catch(() => {});
      }, idleMs);
      try {
        const { done, value } = await reader.read();
        clearTimeout(next);
        if (done) {
          ctrl.close();
        } else {
          ctrl.enqueue(value);
        }
      } catch (e) {
        clearTimeout(next);
        ctrl.error(e);
      }
    },
    cancel(reason) {
      return reader.cancel(reason);
    }
  });
  return new Response(stream, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers
  });
}

// Named export mirrors the default for `import { fetchWithTimeout }`.
export { fetchWithTimeout };

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
