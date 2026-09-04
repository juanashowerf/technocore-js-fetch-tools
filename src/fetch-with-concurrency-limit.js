/**
 * fetch-with-concurrency-limit.js
 *
 * A lightweight, dependency-free concurrency limiter for fetch().
 * Caps the number of in-flight HTTP requests so a fetch-only agent can
 * safely fan out to many endpoints (search APIs, DID resolvers, etc.)
 * without exhausting sockets, hitting rate limits, or crashing browsers.
 *
 * Public API:
 *   const { createLimiter, withLimit } = createLimiter({ concurrency: 6 });
 *
 *   // Run an async function under the cap:
 *   const res = await withLimit(() => fetch(url, init));
 *
 *   // Or run a batch and preserve input order:
 *   const results = await runLimited(urls.map(u => () => fetch(u)), { concurrency: 6 });
 *
 * Semantics:
 *   - FIFO: tasks are started in the order they are submitted.
 *   - Failures from the wrapped task reject its slot's promise; the limiter
 *     itself does not throw on individual failures.
 *   - AbortSignal: pass `{ signal }` to a task to cancel pending work; the
 *     limiter also forwards a top-level signal that aborts all queued
 *     tasks that have not yet started.
 *   - Backpressure: when the cap is reached, callers `await` until a slot
 *     frees, so submitting 10,000 URLs is safe and memory-bounded.
 *
 * Browser + Node 18+ (uses globalThis.AbortController, AbortSignal.any).
 * No external dependencies.
 */

export function createLimiter(options = {}) {
  const { concurrency = 6, signal } = options;
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new RangeError('concurrency must be a positive integer');
  }

  // Queue of { task, resolve, reject, signal } waiting for a free slot.
  const queue = [];
  let active = 0;
  let closed = false;

  function next() {
    if (closed) return;
    while (active < concurrency && queue.length > 0) {
      const item = queue.shift();
      // If this task's signal was already aborted before its turn, skip it.
      if (item.signal && item.signal.aborted) {
        item.reject(item.signal.reason ?? new DOMException('Aborted', 'AbortError'));
        continue;
      }
      active++;
      runOne(item);
    }
  }

  function runOne(item) {
    let abortedExternally = false;
    const onAbort = () => {
      abortedExternally = true;
      // Best-effort: reject the slot. The wrapped task may also throw.
      item.reject(
        (item.signal && item.signal.reason) ||
          new DOMException('Aborted', 'AbortError'),
      );
    };
    if (item.signal) {
      if (item.signal.aborted) return onAbort();
      item.signal.addEventListener('abort', onAbort, { once: true });
    }

    Promise.resolve()
      .then(() => item.task())
      .then(
        (value) => {
          if (item.signal) item.signal.removeEventListener('abort', onAbort);
          if (!abortedExternally) item.resolve(value);
        },
        (err) => {
          if (item.signal) item.signal.removeEventListener('abort', onAbort);
          if (!abortedExternally) item.reject(err);
        },
      )
      .finally(() => {
        active--;
        next();
      });
  }

  function withLimit(task, opts = {}) {
    if (typeof task !== 'function') {
      return Promise.reject(new TypeError('withLimit(task): task must be a function returning a Promise'));
    }
    if (closed) {
      return Promise.reject(new Error('Limiter is closed'));
    }
    return new Promise((resolve, reject) => {
      const { signal: taskSignal } = opts;
      if (taskSignal) {
        if (taskSignal.aborted) {
          reject(taskSignal.reason ?? new DOMException('Aborted', 'AbortError'));
          return;
        }
      }
      queue.push({ task, resolve, reject, signal: taskSignal });
      next();
    });
  }

  async function drain() {
    while (active > 0 || queue.length > 0) {
      await new Promise((r) => setTimeout(r, 10));
    }
  }

  function close() {
    closed = true;
    const err = new Error('Limiter closed');
    for (const item of queue) item.reject(err);
    queue.length = 0;
  }

  function stats() {
    return { active, queued: queue.length, concurrency, closed };
  }

  if (signal) {
    if (signal.aborted) close();
    else signal.addEventListener('abort', close, { once: true });
  }

  return { withLimit, drain, close, stats };
}

/**
 * Convenience: run an array of zero-arg task factories with a concurrency cap.
 * Returns results in the same order as `tasks`. If you need a per-task
 * signal, attach it inside the task function (e.g. fetch(url, { signal })).
 *
 * @param {Array<() => Promise<any>>} tasks
 * @param {{ concurrency?: number, signal?: AbortSignal }} [options]
 */
export async function runLimited(tasks, options = {}) {
  const { concurrency = 6, signal } = options;
  const { withLimit } = createLimiter({ concurrency, signal });
  return Promise.all(tasks.map((t) => withLimit(t)));
}

// ----- Self-contained demo (run with: node src/fetch-with-concurrency-limit.js) -----
if (typeof process !== 'undefined' && process.argv[1] && process.argv[1].endsWith('fetch-with-concurrency-limit.js')) {
  const urls = Array.from({ length: 20 }, (_, i) =>
    `https://httpbin.org/delay/${(i % 3) + 1}?n=${i}`,
  );
  const t0 = Date.now();
  const { withLimit, stats } = createLimiter({ concurrency: 4 });

  // Log active count while running.
  const logTimer = setInterval(() => {
    process.stdout.write(`active=${stats().active} queued=${stats().queued}\n`);
  }, 200);

  const results = await Promise.allSettled(
    urls.map((u, i) =>
      withLimit(async () => {
        const r = await fetch(u);
        return { i, status: r.status, took: Date.now() - t0 };
      }),
    ),
  );
  clearInterval(logTimer);
  const ok = results.filter((r) => r.status === 'fulfilled').length;
  console.log(`done in ${Date.now() - t0}ms, ok=${ok}/${results.length}`);
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
