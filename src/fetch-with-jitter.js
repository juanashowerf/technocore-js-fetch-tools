// fetch-with-jitter.js
// Drop-in fetch wrapper that adds full-jitter exponential backoff for retries.
// Designed to compose with the other fetch-with-*.js modules in this repo.
//
// Why: pure exponential backoff (e.g. 100ms, 200ms, 400ms) creates synchronized
// "thundering herd" waves from many concurrent clients. Full jitter
// (random uniform in [0, base * 2^attempt]) desynchronizes clients so a
// recovering upstream is not slammed by a synchronized retry storm.
//
// Reference: AWS Architecture Blog, "Exponential Backoff and Jitter".
//
// Usage:
//   import fetchWithJitter from './fetch-with-jitter.js';
//   const fetch = fetchWithJitter(globalThis.fetch, { maxAttempts: 5 });
//   const res = await fetch('https://example.com/api', { retries: 4 });
//
// Options (passed to wrapper factory):
//   maxAttempts   default 5   total tries including the first
//   baseMs        default 100 base delay in ms
//   capMs         default 30000 hard cap on per-attempt delay
//   factor        default 2   exponential growth factor
//   retryOn       default [429, 502, 503, 504]  status codes worth retrying
//   isRetryable   optional (res) => boolean  custom predicate, takes precedence
//   onRetry       optional ({attempt, delayMs, status}) => void
//   sleep         optional (ms) => Promise   injectable for tests
//
// Per-call overrides (merged onto options):
//   retries, baseMs, capMs, retryOn, isRetryable, signal
//
// Returns a wrapper that mirrors the global fetch signature exactly.
// Aborts cleanly when the caller's AbortSignal fires.

export function createFetchWithJitter(baseFetch, options = {}) {
  if (typeof baseFetch !== 'function') {
    throw new TypeError('createFetchWithJitter: baseFetch must be a function');
  }

  const defaults = Object.freeze({
    maxAttempts: 5,
    baseMs: 100,
    capMs: 30_000,
    factor: 2,
    retryOn: new Set([429, 502, 503, 504]),
    isRetryable: null,
    onRetry: null,
    sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
  });

  return function fetchWithJitter(input, init = {}) {
    const opts = { ...defaults, ...options, ...init };
    const maxAttempts = Math.max(1, opts.retries != null
      ? opts.retries + 1
      : opts.maxAttempts);
    const retryStatuses = opts.retryOn instanceof Set
      ? opts.retryOn
      : new Set(opts.retryOn);
    const callerSignal = opts.signal;

    return (async () => {
      let attempt = 0;
      let lastErr;
      let lastRes;

      while (attempt < maxAttempts) {
        attempt += 1;

        // Per-attempt signal so we can abort a single in-flight retry.
        const attemptController = new AbortController();
        const onCallerAbort = () => attemptController.abort(callerSignal?.reason);
        if (callerSignal) {
          if (callerSignal.aborted) {
            throw makeAbortError(callerSignal.reason);
          }
          callerSignal.addEventListener('abort', onCallerAbort, { once: true });
        }

        const attemptInit = { ...init, signal: attemptController.signal };
        // Strip our custom keys so they don't leak into Request init.
        delete attemptInit.retries;
        delete attemptInit.baseMs;
        delete attemptInit.capMs;
        delete attemptInit.factor;
        delete attemptInit.retryOn;
        delete attemptInit.isRetryable;
        delete attemptInit.onRetry;

        try {
          const res = await baseFetch(input, attemptInit);
          const shouldRetry = attempt < maxAttempts && (
            typeof opts.isRetryable === 'function'
              ? opts.isRetryable(res)
              : retryStatuses.has(res.status)
          );

          if (!shouldRetry) {
            if (callerSignal) callerSignal.removeEventListener('abort', onCallerAbort);
            return res;
          }

          // Drain the body so the underlying socket can be reused / released.
          try { await res.arrayBuffer(); } catch { /* ignore */ }
          lastRes = res;
        } catch (err) {
          if (callerSignal?.aborted || err?.name === 'AbortError') {
            throw makeAbortError(callerSignal?.reason ?? err);
          }
          lastErr = err;
          if (attempt >= maxAttempts) throw err;
        } finally {
          if (callerSignal && !callerSignal.aborted) {
            callerSignal.removeEventListener('abort', onCallerAbort);
          }
        }

        // Full-jitter: delay ~ Uniform(0, min(cap, base * factor^(attempt-1)))
        const exp = Math.min(opts.capMs, opts.baseMs * Math.pow(opts.factor, attempt - 1));
        const delayMs = Math.floor(Math.random() * exp);

        if (typeof opts.onRetry === 'function') {
          try {
            opts.onRetry({
              attempt,
              delayMs,
              status: lastRes?.status,
              error: lastErr,
            });
          } catch { /* swallow observer errors */ }
        }

        try {
          await opts.sleep(delayMs);
        } catch (err) {
          // If sleep itself rejects, propagate.
          if (callerSignal?.aborted) throw makeAbortError(callerSignal.reason);
          throw err;
        }

        if (callerSignal?.aborted) throw makeAbortError(callerSignal.reason);
      }

      // Should be unreachable, but return last response for safety.
      if (lastRes) return lastRes;
      throw lastErr ?? new Error('fetch-with-jitter: exhausted retries');
    })();
  };
}

function makeAbortError(reason) {
  if (reason instanceof Error) return reason;
  const err = new Error(reason?.message || 'Aborted');
  err.name = 'AbortError';
  err.cause = reason;
  return err;
}

// Default export: convenience wrapper around globalThis.fetch.
const defaultExport = createFetchWithJitter(
  typeof fetch !== 'undefined' ? fetch : null,
);
export default defaultExport;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
