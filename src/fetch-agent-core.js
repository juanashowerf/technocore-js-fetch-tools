/**
 * fetch-agent-core — Zero-dependency fetch toolkit for autonomous browser agents.
 *
 * Every autonomous agent that lives entirely in the browser or a no-build JS runtime
 * needs exactly three primitives beyond raw fetch(): resilience, concurrency control,
 * and streaming. This module supplies all three in a single, copy-pasteable file
 * with no dependencies — not even a package.json.
 *
 * Exports:
 *   resilientFetch(url, init)          — fetch with retry + exponential backoff
 *   createQueue(maxConcurrent)         — concurrency-limited request queue
 *   streamLines(response, onLine)      — async line-by-line response body reader
 *   fetchWithTimeout(url, init, ms)    — fetch that rejects after a deadline
 *   agentFetch(url, init)              — all four combined
 *
 * Usage:
 *   import { agentFetch, resilientFetch, createQueue } from './fetch-agent-core.js';
 */

// ---------------------------------------------------------------------------
// 1. Resilient fetch — automatic retry with exponential backoff
// ---------------------------------------------------------------------------

/**
 * Wraps fetch() with automatic retries on transient failures.
 *
 * Retries on: network errors (TypeError / "Failed to fetch"), 429, 503, 504.
 * Does NOT retry on: 4xx client errors (except 429), redirects, successful responses.
 *
 * @param {RequestInfo|URL} url
 * @param {RequestInit} [init]
 * @param {object} [opts]
 * @param {number} [opts.maxRetries=3]
 * @param {number} [opts.baseDelayMs=400]
 * @param {number} [opts.maxDelayMs=10000]
 * @param {function(number, Error|Response):void} [opts.onRetry]
 * @returns {Promise<Response>}
 */
export async function resilientFetch(url, init = {}, opts = {}) {
  const {
    maxRetries = 3,
    baseDelayMs = 400,
    maxDelayMs = 10000,
    onRetry = null,
  } = opts;

  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, init);

      // Retry on server-side or rate-limit statuses.
      if (response.status === 429 || response.status === 503 || response.status === 504) {
        if (attempt === maxRetries) return response; // give caller the last response
        lastError = new Error(`Retriable status ${response.status}`);
        lastError.response = response;
      } else {
        return response; // success, client error, or redirect — don't retry
      }
    } catch (err) {
      // Network failures (DNS, TCP, TLS, "Failed to fetch") are TypeError in browsers.
      if (attempt === maxRetries) throw err;
      lastError = err;
    }

    const delay = Math.min(baseDelayMs * 2 ** attempt, maxDelayMs);
    const jitter = delay * (0.5 + Math.random() * 0.5);

    if (onRetry) {
      try { onRetry(attempt + 1, lastError); } catch { /* swallow */ }
    }

    await new Promise((r) => setTimeout(r, jitter));
  }

  throw lastError;
}

// ---------------------------------------------------------------------------
// 2. Concurrency-limited request queue
// ---------------------------------------------------------------------------

/**
 * Creates a FIFO task queue that runs at most `maxConcurrent` promises
 * simultaneously. The rest are enqueued and started as slots free up.
 *
 * @param {number} maxConcurrent
 * @returns {{ enqueue: function(function():Promise):Promise, pending: number, queued: number }}
 */
export function createQueue(maxConcurrent = 4) {
  let running = 0;
  const pending = [];

  function dequeue() {
    if (pending.length === 0 || running >= maxConcurrent) return;
    const { task, resolve, reject } = pending.shift();
    running++;
    task()
      .then(resolve, reject)
      .finally(() => {
        running--;
        dequeue();
      });
  }

  function enqueue(task) {
    const p = new Promise((resolve, reject) => {
      pending.push({ task, resolve, reject });
    });
    dequeue();
    return p;
  }

  return {
    enqueue,
    get pending() {
      return running;
    },
    get queued() {
      return pending.length;
    },
  };
}

// ---------------------------------------------------------------------------
// 3. Streaming line-by-line reader
// ---------------------------------------------------------------------------

/**
 * Reads a Response body as UTF-8 text, calling `onLine(line)` for each
 * complete line. Handles both `\n` and `\r\n` line endings. The final
 * partial line (no trailing newline) is also emitted.
 *
 * Useful for SSE, NDJSON, or any text-stream API.
 *
 * @param {Response} response — must have a readable body (not already consumed).
 * @param {function(string):void|Promise<void>} onLine — called per line.
 * @param {object} [opts]
 * @param {AbortSignal} [opts.signal] — abort to stop reading early.
 * @returns {Promise<void>}
 */
export async function streamLines(response, onLine, opts = {}) {
  const { signal } = opts;
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let remainder = '';

  try {
    while (true) {
      if (signal?.aborted) {
        reader.cancel();
        break;
      }

      const { done, value } = await reader.read();
      if (done) break;

      const chunk = remainder + decoder.decode(value, { stream: true });
      const lines = chunk.split('\n');

      // All but the last segment are complete lines.
      remainder = lines.pop();

      for (const line of lines) {
        const trimmed = line.endsWith('\r') ? line.slice(0, -1) : line;
        await onLine(trimmed);
      }
    }

    // Emit any trailing content.
    if (remainder) {
      await onLine(remainder);
    }
  } finally {
    reader.releaseLock();
  }
}

// ---------------------------------------------------------------------------
// 4. Fetch with deadline
// ---------------------------------------------------------------------------

/**
 * fetch() that rejects with a timeout error after `ms` milliseconds.
 * Uses AbortController; the underlying request is aborted on timeout.
 *
 * @param {RequestInfo|URL} url
 * @param {RequestInit} [init]
 * @param {number} ms — deadline in milliseconds.
 * @returns {Promise<Response>}
 */
export async function fetchWithTimeout(url, init = {}, ms = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), ms);

  try {
    const response = await fetch(url, {
      ...init,
      signal: controller.signal,
    });
    return response;
  } catch (err) {
    if (err.name === 'AbortError' && controller.signal.aborted && !init.signal?.aborted) {
      throw new DOMException(`Request timed out after ${ms}ms`, 'TimeoutError');
    }
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// 5. All-in-one agent fetch
// ---------------------------------------------------------------------------

/**
 * Combines timeout, retries, and optional queueing into a single call.
 * This is the primary export for an agent that wants best-effort delivery
 * with minimal ceremony.
 *
 * @param {RequestInfo|URL} url
 * @param {RequestInit} [init]
 * @param {object} [opts]
 * @param {number} [opts.timeoutMs=20000]
 * @param {number} [opts.maxRetries=2]
 * @param {number} [opts.baseDelayMs=500]
 * @param {object} [opts.queue] — a queue from createQueue(); wraps call in enqueue.
 * @param {function(number, Error|Response):void} [opts.onRetry]
 * @returns {Promise<Response>}
 */
export async function agentFetch(url, init = {}, opts = {}) {
  const {
    timeoutMs = 20000,
    maxRetries = 2,
    baseDelayMs = 500,
    queue = null,
    onRetry = null,
  } = opts;

  const task = () =>
    resilientFetch(url, init, {
      maxRetries,
      baseDelayMs,
      onRetry,
    });

  const request = queue ? queue.enqueue(task) : task();

  // Wrap the (possibly queued) promise in a timeout.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await Promise.race([
      request,
      new Promise((_, reject) => {
        const onAbort = () => {
          controller.signal.removeEventListener('abort', onAbort);
          reject(new DOMException(`Request timed out after ${timeoutMs}ms`, 'TimeoutError'));
        };
        controller.signal.addEventListener('abort', onAbort);
      }),
    ]);
    return response;
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// Usage example (remove or keep as documentation):
// ---------------------------------------------------------------------------

/*
  // Basic agent fetch with all defaults:
  const res = await agentFetch('https://api.example.com/data');
  const json = await res.json();

  // With a concurrency cap (max 3 in-flight):
  const queue = createQueue(3);
  const [a, b, c] = await Promise.all([
    agentFetch('/a', {}, { queue }),
    agentFetch('/b', {}, { queue }),
    agentFetch('/c', {}, { queue }),
  ]);

  // Streaming NDJSON from an SSE endpoint:
  const res = await agentFetch('https://api.example.com/stream');
  await streamLines(res, (line) => {
    if (!line) return;
    const event = JSON.parse(line);
    console.log(event);
  });

  // Raw resilient fetch with custom retry callback:
  const res = await resilientFetch('https://flaky.example.com', {}, {
    maxRetries: 5,
    baseDelayMs: 200,
    onRetry: (n, err) => console.warn(`Retry ${n}:`, err.message),
  });
*/

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
