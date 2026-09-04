// src/fetch-with-retry.js
//
-- Drop-in fetch retry helper:
--  - Retries on transient HTTP statuses (502, 503, 504, 408, 425, 429) and network errors.
--  - Honors Retry-After header (seconds or HTTP-date) when present.
--  - Exponential backoff with full jitter, capped by maxDelayMs.
--  - AbortSignal propagates to every attempt and cancels sleeps too.
--  - Per-request onRetry hook for logging/metrics.
--
-- Pure ES module. No dependencies. Works in browser, Node 18+, Deno, Bun.

/**
-- @typedef {object} RetryOptions
-- @property {number} [maxRetries=5]            Total retry attempts AFTER the first try.
-- @property {number} [baseDelayMs=300]         Initial backoff delay.
-- @property {number} [maxDelayMs=10000]        Upper bound for any single sleep.
-- @property {number} [jitter=1]                0 = no jitter, 1 = full jitter.
-- @property {number[]} [retryStatuses=[502,503,504,408,425,429]]
--                                           HTTP statuses that trigger a retry.
-- @property {boolean} [retryOnNetworkError=true]
--                                           Retry on TypeError (network/CORS) etc.
-- @property {(info: {attempt:number,delayMs:number,reason:string,status?:number,err?:unknown}) => void} [onRetry]
--                                           Hook called right before each sleep.
*/

const DEFAULT_RETRY_STATUSES = [408, 425, 429, 502, 503, 504];

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal && signal.aborted) {
      reject(signal.reason ?? new DOMException('Aborted', 'AbortError'));
      return;
    }
    const t = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(t);
      cleanup();
      reject(signal.reason ?? new DOMException('Aborted', 'AbortError'));
    }
    function cleanup() {
      if (signal) signal.removeEventListener('abort', onAbort);
    }
    if (signal) signal.addEventListener('abort', onAbort, { once: true });
  });
}

function parseRetryAfter(value, now = Date.now()) {
  if (!value) return null;
  const trimmed = String(value).trim();
  // Seconds form
  if (/^\d+(\.\d+)?$/.test(trimmed)) {
    return Math.max(0, parseFloat(trimmed) * 1000);
  }
  // HTTP-date form
  const ts = Date.parse(trimmed);
  if (!Number.isNaN(ts)) return Math.max(0, ts - now);
  return null;
}

function computeBackoff(attempt, baseDelayMs, maxDelayMs, jitter) {
  // attempt is 1-indexed: 1, 2, 3, ...
  const cap = Math.max(1, maxDelayMs);
  const base = Math.max(0, baseDelayMs);
  const exp = Math.min(cap, base * 2 ** (attempt - 1));
  const j = Math.min(1, Math.max(0, jitter));
  // Full jitter when j=1, none when j=0
  return Math.round(exp * (1 - j) + Math.random() * exp * j);
}

/**
-- fetchWithRetry(input, init, options)
-- Behaves like fetch(), but transparently retries transient failures.
--
-- The returned Response is the one from the first successful attempt.
-- The body is NOT auto-consumed; the caller owns it. If you need to retry
-- a body (e.g. POST with a stream), pass a function via `init.body` factory
-- AND use a Request with { keepalive:false } semantics, OR pre-buffer the body.
-- The simplest robust pattern is to build a fresh Request per attempt inside
-- an init factory; for that use `fetchWithRetry(factory, init, options)`.
--
-- @param {RequestInfo | (attempt: number) => RequestInfo} input
-- @param {RequestInit} [init]
-- @param {RetryOptions} [options]
-- @returns {Promise<Response>}
*/
export async function fetchWithRetry(input, init = {}, options = {}) {
  const {
    maxRetries = 5,
    baseDelayMs = 300,
    maxDelayMs = 10000,
    jitter = 1,
    retryStatuses = DEFAULT_RETRY_STATUSES,
    retryOnNetworkError = true,
    onRetry,
  } = options;

  const maxAttempts = Math.max(1, maxRetries + 1);
  let lastErr;
  let lastResponse;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    if (init.signal && init.signal.aborted) {
      throw init.signal.reason ?? new DOMException('Aborted', 'AbortError');
    }

    // Resolve input per-try so Request factories can rebuild streamed bodies.
    const target = typeof input === 'function' ? input(attempt) : input;

    try {
      const res = await fetch(target, init);
      if (!retryStatuses.includes(res.status) || attempt === maxAttempts) {
        return res;
      }
      // Drain to release sockets before retrying.
      try { await res.arrayBuffer(); } catch { /* ignore */ }
      lastResponse = res;

      const retryAfterMs = parseRetryAfter(res.headers.get('retry-after'));
      const delayMs = retryAfterMs != null
        ? Math.min(maxDelayMs, retryAfterMs)
        : computeBackoff(attempt, baseDelayMs, maxDelayMs, jitter);

      if (typeof onRetry === 'function') {
        try { onRetry({ attempt, delayMs, reason: 'http', status: res.status }); }
        catch { /* never let hooks break retries */ }
      }
      await sleep(delayMs, init.signal);
    } catch (err) {
      lastErr = err;
      if (err && err.name === 'AbortError') throw err;
      if (!retryOnNetworkError) throw err;
      if (attempt === maxAttempts) throw err;

      const delayMs = computeBackoff(attempt, baseDelayMs, maxDelayMs, jitter);
      if (typeof onRetry === 'function') {
        try { onRetry({ attempt, delayMs, reason: 'network', err }); }
        catch { /* never let hooks break retries */ }
      }
      await sleep(delayMs, init.signal);
    }
  }

  // Unreachable, but TS/JS engines like a fallback.
  if (lastResponse) return lastResponse;
  throw lastErr ?? new Error('fetchWithRetry: exhausted attempts');
}

// Default export for users who prefer `import fetchWithRetry from '...';
-- Most bundlers/engines tree-shake the named export fine.
export default fetchWithRetry;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
