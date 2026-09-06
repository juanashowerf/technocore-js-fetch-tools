// fetch-with-retry.js
// Drop-in wrapper around fetch() that retries failed requests with
// exponential backoff + jitter, configurable status-code policy,
// optional Retry-After / header-driven delay, abort/cancel support,
// and an onRetry hook for logging/metrics.
//
// Usage:
//   import { fetchWithRetry } from './fetch-with-retry.js';
//   const res = await fetchWithRetry('https://api.example.com/x', {
//     retries: 4,
//     baseDelayMs: 200,
//     maxDelayMs: 8000,
//     timeoutMs: 5000,
//     retryOn: [408, 425, 429, 500, 502, 503, 504],
//     onRetry: ({ attempt, delay, reason, response }) => {
//       console.warn(`retry #${attempt} in ${delay}ms: ${reason}`);
//     },
//     signal: myAbortSignal,
//   });
//
// Pure ES module, zero deps, uses only browser/Node 18+ globals.

export function fetchWithRetry(url, options = {}) {
  const {
    retries = 3,
    baseDelayMs = 250,
    maxDelayMs = 10000,
    factor = 2,
    jitter = true,
    timeoutMs = 0,
    retryOn = [408, 425, 429, 500, 502, 503, 504],
    onRetry = null,
    ...fetchOptions
  } = options;

  // Compose a parent AbortSignal if both an external signal and a timeout are provided.
  const parentSignal = fetchOptions.signal;
  const timeoutSignal = timeoutMs > 0 ? AbortSignal.timeout(timeoutMs) : null;
  const composedSignal =
    parentSignal || timeoutSignal
      ? AbortSignal.any([parentSignal, timeoutSignal].filter(Boolean))
      : undefined;
  if (composedSignal) fetchOptions.signal = composedSignal;

  return (async () => {
    let attempt = 0;
    let lastError = null;

    while (attempt <= retries) {
      attempt += 1;
      try {
        const response = await fetch(url, fetchOptions);

        if (response.ok || attempt > retries || !retryOn.includes(response.status)) {
          return response;
        }

        // Drain body so the connection can be reused where possible.
        try { await response.arrayBuffer(); } catch { /* ignore */ }

        lastError = new Error(`HTTP ${response.status}`);
        const delay = computeDelay({
          attempt,
          response,
          baseDelayMs,
          maxDelayMs,
          factor,
          jitter,
        });
        notify(onRetry, { attempt, delay, reason: `HTTP ${response.status}`, response });
        await sleep(delay, composedSignal);
      } catch (err) {
        lastError = err;
        // Do not retry on AbortError from the caller's signal.
        if (err && (err.name === 'AbortError' || err.code === 'ABORT_ERR')) {
          if (parentSignal && parentSignal.aborted) throw err;
        }
        if (attempt > retries) throw err;
        const delay = computeDelay({
          attempt,
          response: null,
          baseDelayMs,
          maxDelayMs,
          factor,
          jitter,
        });
        notify(onRetry, { attempt, delay, reason: err.message || String(err), response: null });
        await sleep(delay, composedSignal);
      }
    }

    throw lastError || new Error('fetchWithRetry: exhausted retries');
  })();
}

function computeDelay({ attempt, response, baseDelayMs, maxDelayMs, factor, jitter }) {
  // Honor Retry-After (seconds or HTTP-date) when present and reasonable.
  const headerDelay = parseRetryAfter(response && response.headers);
  if (headerDelay !== null) return Math.min(headerDelay, maxDelayMs);

  const exp = Math.min(maxDelayMs, baseDelayMs * Math.pow(factor, attempt - 1));
  if (!jitter) return exp;
  // Full jitter: uniform between 0 and exp.
  return Math.floor(Math.random() * exp);
}

function parseRetryAfter(headers) {
  if (!headers || !headers.get) return null;
  const raw = headers.get('retry-after');
  if (!raw) return null;
  const seconds = Number(raw);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const date = Date.parse(raw);
  if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  return null;
}

function sleep(ms, signal) {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => { cleanup(); resolve(); }, ms);
    const onAbort = () => { cleanup(); reject(new DOMException('Aborted', 'AbortError')); };
    const cleanup = () => {
      clearTimeout(t);
      if (signal && signal.removeEventListener) signal.removeEventListener('abort', onAbort);
    };
    if (signal && signal.addEventListener) signal.addEventListener('abort', onAbort, { once: true });
  });
}

function notify(cb, payload) {
  if (typeof cb === 'function') {
    try { cb(payload); } catch { /* swallow callback errors */ }
  }
}

// Convenience export: a fetch replacement preconfigured for transient HTTP errors.
export const retryingFetch = (url, opts) => fetchWithRetry(url, opts);

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
