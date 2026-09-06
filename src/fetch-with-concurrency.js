// fetch-with-concurrency.js
// A no-build, browser-native fetch wrapper that throttles concurrent requests
// using a simple in-memory queue and configurable worker pool. Useful when a
// fetch-only agent must call many endpoints in parallel without tripping
// remote rate limits or overwhelming the browser's connection pool.
//
// Usage:
//   import { fetchLimited, fetchAllLimited } from './fetch-with-concurrency.js';
//   const r = await fetchLimited('https://a.example/x', {}, { concurrency: 4 });
//   const results = await fetchAllLimited(['/a','/b','/c'], {}, { concurrency: 3 });
//
// Notes:
// - Zero dependencies. Works in any modern browser with native fetch + AbortController.
// - Aborts in-flight requests if the caller's signal aborts.
// - Resolves with the same Response object as fetch; callers read body as usual.

const STATE = Symbol.for('fetch-with-concurrency.state');

function getState() {
  if (!globalThis[STATE]) globalThis[STATE] = { pool: null };
  return globalThis[STATE];
}

// Promise-based mutex queue: at most `limit` tasks run at once.
function createPool(limit) {
  const tasks = [];
  let active = 0;

  const next = () => {
    if (active >= limit) return;
    const slot = tasks.shift();
    if (!slot) return;
    active += 1;
    slot().finally(() => {
      active -= 1;
      next();
    });
  };

  return {
    run(fn) {
      return new Promise((resolve, reject) => {
        tasks.push(() => {
          try {
            resolve(fn());
          } catch (e) {
            reject(e);
          }
          return Promise.resolve();
        });
        next();
      });
    },
    get pending() { return tasks.length; },
    get active() { return active; }
  };
}

function getPool(limit) {
  const st = getState();
  if (!st.pool || st.pool.limit !== limit) {
    st.pool = { limit, pool: createPool(limit) };
  }
  return st.pool.pool;
}

// Internal: run a single fetch under the pool, with abort composition.
async function runOne(url, opts, limit, externalSignal) {
  const pool = getPool(limit);
  const internal = new AbortController();
  const onAbort = () => internal.abort(externalSignal?.reason);
  if (externalSignal) {
    if (externalSignal.aborted) internal.abort(externalSignal.reason);
    else externalSignal.addEventListener('abort', onAbort, { once: true });
  }
  try {
    return await pool.run(() =>
      fetch(url, { ...opts, signal: internal.signal })
    );
  } finally {
    if (externalSignal) externalSignal.removeEventListener('abort', onAbort);
  }
}

// Single fetch with concurrency control. Useful when you want to mix many
// parallel calls across the same limit without managing the queue yourself.
export async function fetchLimited(url, opts = {}, { concurrency = 4 } = {}) {
  const limit = Math.max(1, concurrency | 0);
  return runOne(url, opts, limit, opts?.signal);
}

// Parallel map: runs `urls` through the shared pool, preserving input order.
// Returns an array of { status, ok, value, error } so one failure doesn't sink
// the whole batch.
export async function fetchAllLimited(urls, opts = {}, { concurrency = 4 } = {}) {
  const limit = Math.max(1, concurrency | 0);
  const externalSignal = opts?.signal;
  const tasks = urls.map(async (entry) => {
    const url = typeof entry === 'string' ? entry : entry.url;
    const itemOpts = typeof entry === 'string' ? opts : { ...opts, ...entry.opts };
    try {
      const res = await runOne(url, itemOpts, limit, externalSignal);
      return { status: res.status, ok: res.ok, value: res, error: null };
    } catch (err) {
      return { status: 0, ok: false, value: null, error: err };
    }
  });
  return Promise.all(tasks);
}

// Inspect queue depth (active + waiting). Useful for backpressure decisions.
export function concurrencyStats(concurrency = 4) {
  const limit = Math.max(1, concurrency | 0);
  const pool = getPool(limit);
  return { limit, active: pool.active, pending: pool.pending };
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
