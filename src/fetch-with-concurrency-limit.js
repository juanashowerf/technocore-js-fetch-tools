/**
 * fetch-with-concurrency-limit.js
 *
 * A drop-in fetch wrapper that caps the number of in-flight HTTP requests
 * at once. Useful for fetch-only agents (Node 18+ / browser / Workers) that
 * must respect a server's stated concurrency ceiling, avoid triggering rate
 * limits, or just keep memory + sockets bounded.
 *
 * No build step. No polyfills. Just native fetch + a tiny FIFO scheduler.
 *
 * Usage:
 *   import fetchLimited from "./fetch-with-concurrency-limit.js";
 *
 *   const fetch = fetchLimited(globalThis.fetch, { maxConcurrent: 4 });
 *
 *   // Now every call through this `fetch` respects the cap:
 *   const results = await Promise.all([
 *     fetch("/api/a"), fetch("/api/b"), fetch("/api/c"),
 *     fetch("/api/d"), fetch("/api/e"), fetch("/api/f"),
 *   ]);
 *
 * Options:
 *   maxConcurrent: number  // hard cap on simultaneous requests (default 6)
 *   queueSize:     number  // soft cap on waiting queue; rejects past this
 *                           //   with a QueueFullError (default Infinity)
 *   onSchedule:    fn      // optional (task) => void, fires when a request
 *                           //   is admitted to the active set
 *   onRun:         fn      // optional (task) => void, fires when the
 *                           //   underlying fetch resolves/rejects
 *
 * Error types:
 *   QueueFullError      - thrown synchronously if queueSize is exceeded
 *   AbortError rethrow  - if the caller's AbortSignal fires while queued
 *
 * Notes:
 *   - AbortSignal is honored both while queued and while running. If the
 *     caller aborts while still waiting for a slot, the call rejects
 *     immediately with an AbortError and never consumes a slot.
 *   - Each call returns a Promise; rejections propagate from the underlying
 *     fetch unchanged.
 *   - The wrapper preserves `this` and any extra Response methods consumers
 *     might use (e.g. response.clone()).
 */

export class QueueFullError extends Error {
  constructor(maxQueue) {
    super(`Concurrency queue is full (size limit: ${maxQueue})`);
    this.name = "QueueFullError";
    this.code = "EQUEUEFULL";
  }
}

export default function withConcurrencyLimit(baseFetch, options = {}) {
  if (typeof baseFetch !== "function") {
    throw new TypeError("withConcurrencyLimit: baseFetch must be a function");
  }

  const maxConcurrent = Math.max(1, options.maxConcurrent ?? 6);
  const maxQueue = options.queueSize ?? Infinity;
  const onSchedule = typeof options.onSchedule === "function" ? options.onSchedule : null;
  const onRun = typeof options.onRun === "function" ? options.onRun : null;

  // Tracks callers that are parked waiting for a slot.
  // Each entry: { input, init, resolve, reject, signal, scheduled }
  const queue = [];
  let active = 0;

  function pump() {
    while (active < maxConcurrent && queue.length > 0) {
      const task = queue.shift();
      if (task.signal && task.signal.aborted) {
        // Caller already gave up; skip without burning a slot.
        task.reject(
          (typeof DOMException !== "undefined"
            ? new DOMException("Aborted", "AbortError")
            : Object.assign(new Error("Aborted"), { name: "AbortError" }))
        );
        continue;
      }
      active += 1;
      if (onSchedule) {
        try { onSchedule(task); } catch { /* listener errors are non-fatal */ }
      }
      runTask(task);
    }
  }

  function runTask(task) {
    const { input, init, signal, resolve, reject } = task;

    let removeAbortListener = () => {};
    if (signal) {
      const onAbort = () => {
        // Best-effort: nothing to abort yet if we never issued fetch;
        // if it already issued, the underlying fetch will reject and
        // the listener will be cleaned up there.
        reject(
          (typeof DOMException !== "undefined"
            ? new DOMException("Aborted", "AbortError")
            : Object.assign(new Error("Aborted"), { name: "AbortError" }))
        );
        // We must NOT decrement active here; the in-flight fetch may still
        // settle. Let the .then handlers below release the slot.
      };
      if (signal.aborted) {
        onAbort();
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
      removeAbortListener = () => signal.removeEventListener("abort", onAbort);
    }

    baseFetch(input, init)
      .then(
        (res) => {
          removeAbortListener();
          resolve(res);
          if (onRun) {
            try { onRun(task); } catch { /* ignore */ }
          }
          active -= 1;
          pump();
        },
        (err) => {
          removeAbortListener();
          reject(err);
          if (onRun) {
            try { onRun(task); } catch { /* ignore */ }
          }
          active -= 1;
          pump();
        }
      );
  }

  function limitedFetch(input, init) {
    const signal = init && init.signal;

    if (signal && signal.aborted) {
      return Promise.reject(
        typeof DOMException !== "undefined"
          ? new DOMException("Aborted", "AbortError")
          : Object.assign(new Error("Aborted"), { name: "AbortError" })
      );
    }

    // If we have headroom, run immediately (still asynchronous, but no queue).
    if (active < maxConcurrent && queue.length === 0) {
      return new Promise((resolve, reject) => {
        active += 1;
        if (onSchedule) {
          try { onSchedule({ input, init, signal }); } catch { /* ignore */ }
        }
        runTask({ input, init, signal, resolve, reject });
      });
    }

    if (queue.length >= maxQueue) {
      return Promise.reject(new QueueFullError(maxQueue));
    }

    return new Promise((resolve, reject) => {
      queue.push({ input, init, signal, resolve, reject, scheduled: false });
      // Try to pump in case slots freed up between our length check and push.
      // This is safe: pump() will only act on items already in the queue.
      queueMicrotask(pump);
    });
  }

  // Expose live counters for observability / tests. Read-only views.
  Object.defineProperties(limitedFetch, {
    active:     { get: () => active },
    queued:     { get: () => queue.length },
    pending:    { get: () => active + queue.length },
    maxConcurrent: { get: () => maxConcurrent },
    maxQueue:   { get: () => maxQueue },
  });

  return limitedFetch;
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
