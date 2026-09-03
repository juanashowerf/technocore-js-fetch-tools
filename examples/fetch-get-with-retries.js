// examples/fetch-get-with-retries.js
//
// Robust GET helper for a fetch-only agent: retry on transient network errors
// and 5xx responses, with exponential backoff and jitter. No build step,
// no dependencies — drop this file in and `node` it, or import the function.
//
// Why this exists:
//   The built-in `fetch` in Node 18+ (and modern browsers) does not retry.
//   A "fetch-only" agent will hit flaky endpoints, rate limits, or brief
//   502/503/504 windows from upstream proxies. This helper gives a single
//   deterministic function to call so the agent does not have to reimplement
//   retry logic per task.
//
// Usage:
//   const { getWithRetries } = require('./examples/fetch-get-with-retries');
//   const res = await getWithRetries('https://api.example.com/things', {
//     attempts: 5,
//     baseDelayMs: 250,
//     maxDelayMs: 8000,
//     timeoutMs: 10_000,
//     headers: { 'accept': 'application/json' },
//   });
//   if (!res.ok) throw new Error(`HTTP ${res.status}`);
//   const data = await res.json();
//
// Notes:
//   - Retries on: AbortError (timeout), TypeError ("fetch failed"), and
//     HTTP 408, 425, 429, 500, 502, 503, 504.
//   - Does NOT retry on: 4xx other than the above (caller's problem,
//     retrying won't help) or on successful 2xx/3xx.
//   - Delay = min(maxDelayMs, baseDelayMs * 2^(attempt-1)) * (0.5 + rand*0.5)
//     so a swarm of agents does not synchronize retries.
//   - `onRetry` callback is optional, useful for logging from the agent.

async function getWithRetries(url, opts = {}) {
  const {
    attempts = 4,
    baseDelayMs = 300,
    maxDelayMs = 8000,
    timeoutMs = 10_000,
    headers = {},
    onRetry = null,
  } = opts;

  if (typeof fetch !== 'function') {
    throw new Error('global fetch is not available; use Node 18+ or a browser');
  }
  if (!Number.isInteger(attempts) || attempts < 1) {
    throw new Error('attempts must be a positive integer');
  }

  const RETRY_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);

  let lastErr = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method: 'GET',
        headers,
        signal: controller.signal,
        // `redirect: 'follow'` is the default; be explicit for clarity.
        redirect: 'follow',
      });
      clearTimeout(timer);

      if (res.ok) return res;

      // Drain the body so the connection can be reused on some runtimes.
      try { await res.arrayBuffer(); } catch (_) { /* ignore */ }

      if (!RETRY_STATUS.has(res.status) || attempt === attempts) {
        return res; // surface the final non-retryable response to the caller
      }
      lastErr = new Error(`HTTP ${res.status} ${res.statusText || ''}`.trim());
    } catch (err) {
      clearTimeout(timer);
      lastErr = err;
      // AbortError (timeout) and TypeError ("fetch failed") are retryable.
      const retryable =
        err && (err.name === 'AbortError' || err.name === 'TypeError');
      if (!retryable || attempt === attempts) throw err;
    }

    const exp = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
    const jittered = Math.floor(exp * (0.5 + Math.random() * 0.5));
    if (typeof onRetry === 'function') {
      try { onRetry({ attempt, delayMs: jittered, error: lastErr }); } catch (_) {}
    }
    await new Promise(r => setTimeout(r, jittered));
  }

  // Unreachable: the loop either returns, returns a non-ok Response, or throws.
  throw lastErr || new Error('getWithRetries: exhausted attempts');
}

// --- CLI demo: `node examples/fetch-get-with-retries.js <url>`
if (require.main === module) {
  const url = process.argv[2];
  if (!url) {
    console.error('usage: node examples/fetch-get-with-retries.js <url>');
    process.exit(2);
  }
  getWithRetries(url, {
    onRetry: ({ attempt, delayMs, error }) =>
      console.error(`retry ${attempt} after ${delayMs}ms: ${error && error.message}`),
  })
    .then(async (res) => {
      console.error(`status: ${res.status}`);
      const text = await res.text();
      console.log(text.length > 4000 ? text.slice(0, 4000) + '…' : text);
    })
    .catch((err) => {
      console.error('failed:', err && err.message);
      process.exit(1);
    });
}

module.exports = { getWithRetries };

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
