// fetch-with-retry.js
// A small, dependency-free wrapper that retries a fetch() under transient failure
// conditions (network errors, 5xx, 408 Request Timeout, 429 Too Many Requests).
// Uses exponential backoff with full jitter, honors an AbortSignal so the whole
// retry chain can be cancelled, and surfaces the final Response (or throw) to the
// caller. Designed for fetch-only browser/JS agents that need resilience without
// pulling in axios, p-retry, or similar libraries.
//
// Usage:
//   import { fetchWithRetry } from './fetch-with-retry.js';
//   const ctrl = new AbortController();
//   const res = await fetchWithRetry('https://api.example.com/data', {
//     signal: ctrl.signal,
//     retries: 5,
//     baseDelayMs: 250,
//     maxDelayMs: 8000,
//     retryOn: [408, 429, 500, 502, 503, 504],
//     onRetry: ({ attempt, status, delayMs, willRetry }) => { ... },
//   });
//   const json = await res.json();
//
// Notes:
// - Retries on network errors (TypeError thrown by fetch) by default.
// - `retryOn` defaults to [408, 429, 500, 502, 503, 504].
// - Delay is min(maxDelayMs, baseDelayMs * 2^attempt) * random() (full jitter).
// - Respects Retry-After header when present (seconds or HTTP-date).
// - AbortSignal aborts between attempts immediately and cancels in-flight fetch.

const DEFAULT_RETRY_ON = [408, 429, 500, 502, 503, 504];

function sleep(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal && signal.aborted) {
      return reject(makeAbortError(signal.reason));
    }
    const t = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      cleanup();
      reject(makeAbortError(signal.reason));
    };
    const cleanup = () => {
      if (signal) signal.removeEventListener('abort', onAbort);
    };
    if (signal) signal.addEventListener('abort', onAbort, { once: true });
  });
}

function makeAbortError(reason) {
  const err = new Error(reason ? String(reason) : 'aborted');
  err.name = 'AbortError';
  err.code = 20; // DOMException.ABORT_ERR
  return err;
}

function parseRetryAfter(value, now = Date.now()) {
  if (!value) return null;
  const asNum = Number(value);
  if (Number.isFinite(asNum)) return Math.max(0, asNum * 1000);
  const asDate = Date.parse(value);
  if (!Number.isNaN(asDate)) return Math.max(0, asDate - now);
  return null;
}

function computeBackoff({ attempt, baseDelayMs, maxDelayMs }) {
  const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
  return Math.floor(Math.random() * exp); // full jitter
}

function isRetryableStatus(status, retryOn) {
  return retryOn.includes(status);
}

export async function fetchWithRetry(url, init = {}) {
  const {
    retries = 4,
    baseDelayMs = 250,
    maxDelayMs = 8000,
    retryOn = DEFAULT_RETRY_ON,
    onRetry,
    signal,
    ...fetchInit
  } = init;

  let attempt = 0;
  // Own controller so we can cancel in-flight fetch when the caller's signal aborts.
  const ctrl = new AbortController();
  let onCallerAbort;
  if (signal) {
    if (signal.aborted) ctrl.abort(signal.reason);
    else {
      onCallerAbort = () => ctrl.abort(signal.reason);
      signal.addEventListener('abort', onCallerAbort, { once: true });
    }
  }

  try {
    // Loop forever; we break on success or on non-retryable terminal condition.
    // Each iteration consumes one attempt; the first call counts as attempt 0.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const res = await fetch(url, { ...fetchInit, signal: ctrl.signal });
        if (!res.ok && isRetryableStatus(res.status, retryOn) && attempt < retries) {
          const retryAfterMs = parseRetryAfter(res.headers.get('retry-after'));
          // Drain body to avoid leaking sockets before retrying.
          try { await res.arrayBuffer(); } catch { /* ignore */ }
          const delay = retryAfterMs != null ? retryAfterMs : computeBackoff({ attempt, baseDelayMs, maxDelayMs });
          if (typeof onRetry === 'function') {
            try { onRetry({ attempt, status: res.status, delayMs: delay, willRetry: true }); } catch { /* ignore */ }
          }
          await sleep(delay, ctrl.signal);
          attempt += 1;
          continue;
        }
        return res; // success OR non-retryable (caller decides)
      } catch (err) {
        // Network error or abort.
        const aborted = err && (err.name === 'AbortError' || ctrl.signal.aborted);
        if (aborted) throw err;
        if (attempt >= retries) throw err;
        const delay = computeBackoff({ attempt, baseDelayMs, maxDelayMs });
        if (typeof onRetry === 'function') {
          try { onRetry({ attempt, error: err, delayMs: delay, willRetry: true }); } catch { /* ignore */ }
        }
        await sleep(delay, ctrl.signal);
        attempt += 1;
      }
    }
  } finally {
    if (signal && onCallerAbort) signal.removeEventListener('abort', onCallerAbort);
  }
}

// Convenience export: builds a callable that retries any URL with fixed options.
export function createRetryingFetch(defaults = {}) {
  return (url, init) => fetchWithRetry(url, { ...defaults, ...init });
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
