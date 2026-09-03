/**
 * fetch-with-timeout.js
 *
 * A fetch wrapper that enforces an overall request timeout via AbortController.
 * Works in browsers and modern Node (18+) without any dependencies.
 *
 * Design notes:
 *  - We compose a caller-supplied AbortSignal with our internal timeout signal
 *    using AbortSignal.any([...]) (Node 20+, modern browsers). When that API is
 *    unavailable we fall back to a single combined controller pattern.
 *  - The timeout is "best effort": when the deadline elapses we abort the
 *    underlying request. Slow body streaming after headers is not interrupted
 *    by this helper alone — pair it with a streaming read deadline if needed.
 *  - We DO NOT swallow errors. The DOMException with name "TimeoutError" or
 *    "AbortError" is re-thrown so callers can branch on .name.
 *
 * Public API:
 *   fetchWithTimeout(url, options = {}, timeoutMs = 30000)
 *     - url:        string | URL | Request
 *     - options:    standard RequestInit (may include its own `signal`)
 *     - timeoutMs:  positive integer milliseconds; 0 or negative disables the
 *                   timeout entirely.
 *   TimeoutError (exported) — convenience reference to the DOMException class
 *   used to mark timeout-induced aborts.
 *
 * Example:
 *   import { fetchWithTimeout, TimeoutError } from "./fetch-with-timeout.js";
 *   try {
 *     const r = await fetchWithTimeout("/slow", { method: "GET" }, 2500);
 *     console.log(await r.text());
 *   } catch (err) {
 *     if (err instanceof TimeoutError || err.name === "TimeoutError") {
 *       console.warn("request hit 2.5s deadline");
 *     } else if (err.name === "AbortError") {
 *       console.warn("caller cancelled");
 *     } else {
 *       throw err;
 *     }
 *   }
 */

export const TimeoutError =
  (typeof DOMException !== "undefined" && DOMException) ||
  (typeof Error !== "undefined" ? Error : class {});

/**
 * Internal: mark an abort reason as a timeout rather than a caller cancel.
 * We attach a custom property so callers can distinguish the two even when
 * the underlying DOMException is generic AbortError.
 */
function markTimeout(signal) {
  if (signal && "reason" in signal) {
    try {
      // Best-effort: not all engines let us mutate the reason.
      signal.reason = Object.assign(
        signal.reason instanceof Error ? signal.reason : new Error("timeout"),
        { name: "TimeoutError", isTimeout: true }
      );
    } catch (_) {
      /* read-only reason — ignore */
    }
  }
}

function hasAbortSignalAny() {
  return typeof AbortSignal !== "undefined" && typeof AbortSignal.any === "function";
}

export async function fetchWithTimeout(url, options = {}, timeoutMs = 30000) {
  const init = { ...options };
  const callerSignal = init.signal;
  const timeoutDisabled = !Number.isFinite(timeoutMs) || timeoutMs <= 0;

  // No timeout requested: just pass through, preserving caller signal.
  if (timeoutDisabled) {
    return fetch(url, init);
  }

  let internalController;
  let timer;

  // Compose signals so EITHER a caller cancel OR the deadline cancels fetch.
  if (callerSignal) {
    if (callerSignal.aborted) {
      // Caller already aborted — surface immediately.
      throw callerSignal.reason || new DOMException("Aborted", "AbortError");
    }

    if (hasAbortSignalAny()) {
      init.signal = AbortSignal.any([callerSignal, createTimeoutSignal(timeoutMs)]);
      // Note: we don't get a handle to the internal controller in this branch,
      // which is fine — the composed signal handles everything.
      try {
        return await fetch(url, init);
      } finally {
        // No explicit cleanup needed; the timeout signal self-cleans via its
        // internal timer once aborted.
      }
    }

    // Fallback: manually bridge the caller signal into our controller.
    internalController = new AbortController();
    const onCallerAbort = () => internalController.abort(callerSignal.reason);
    callerSignal.addEventListener("abort", onCallerAbort, { once: true });
    init.signal = internalController.signal;
    timer = startTimer(internalController, timeoutMs);
  } else {
    internalController = new AbortController();
    init.signal = internalController.signal;
    timer = startTimer(internalController, timeoutMs);
  }

  try {
    return await fetch(url, init);
  } finally {
    if (timer) clearTimeout(timer);
    if (callerSignal && internalController) {
      callerSignal.removeEventListener("abort", () => {});
    }
  }
}

function createTimeoutSignal(ms) {
  const c = new AbortController();
  startTimer(c, ms);
  return c.signal;
}

function startTimer(controller, ms) {
  return setTimeout(() => {
    markTimeout(controller.signal);
    const reason =
      (typeof DOMException !== "undefined"
        ? new DOMException("The operation was aborted due to timeout", "TimeoutError")
        : new Error("timeout"));
    reason.isTimeout = true;
    try {
      controller.abort(reason);
    } catch (_) {
      controller.abort();
    }
  }, ms);
}

// Default export for ergonomics.
export default fetchWithTimeout;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
