/**
 * fetch-get-with-stream-and-cancel.js
 *
 * Goal: a fetch-only peer should be able to (a) start a GET, (b) read the
 * response body incrementally as a stream of Uint8Array chunks (low memory,
 * works for big responses), and (c) abort the in-flight request when a
 * caller-supplied signal fires (manual timeout, user cancel, byte-budget
 * exceeded, etc.) — all with zero dependencies and no build step.
 *
 * Exports a single async function `fetchStream({ url, headers, signal,
 * onChunk, timeoutMs, maxBytes })` that returns `{ status, headers,
 * bodyBytes, aborted }`. The function is drop-in friendly: it just uses
 * the WHATWG `fetch` + `ReadableStream` + `AbortController` that every
 * modern browser and Node 18+ already provide.
 *
 * Usage:
 *   import { fetchStream } from "./examples/fetch-get-with-stream-and-cancel.js";
 *
 *   const ctrl = new AbortController();
 *   setTimeout(() => ctrl.abort(new Error("client timeout")), 5000);
 *
 *   const res = await fetchStream({
 *     url: "https://example.com/large.json",
 *     signal: ctrl.signal,
 *     onChunk: (u8) => { /* got bytes; could decode incrementally *\/ },
 *     maxBytes: 10 * 1024 * 1024, // stop after 10 MiB
 *   });
 *   console.log(res.status, res.bodyBytes, res.aborted);
 *
 * Notes for fetch-only agents:
 *   - No polyfills required on Node >=18 or any evergreen browser.
 *   - `signal` is optional; you may also just pass `timeoutMs` and one is
 *     created for you.
 *   - `onChunk` receives raw Uint8Array chunks; concatenate to a Blob /
 *     TextDecoder stream if you need a string.
 *   - We do NOT call `response.body.cancel()` on success: it is auto-closed
 *     when the stream finishes. On abort we DO cancel so the socket is
 *     released promptly.
 */

export async function fetchStream({
  url,
  headers,
  signal,
  onChunk,
  timeoutMs,
  maxBytes,
} = {}) {
  if (typeof url !== "string" || !url) {
    throw new TypeError("fetchStream: `url` must be a non-empty string");
  }
  if (typeof fetch !== "function") {
    throw new Error("fetchStream: global fetch() is not available in this runtime");
  }

  // Build / merge AbortController: caller-provided signal wins, otherwise
  // synthesize one (optionally tied to a timeout).
  let controller;
  let ownController = false;
  let timeoutHandle = null;

  if (signal) {
    // Listen to the caller's signal and forward into our own controller so
    // we can run cleanup on abort regardless of who triggered it.
    if (signal.aborted) {
      return { status: 0, headers: new Headers(), bodyBytes: 0, aborted: true };
    }
    controller = new AbortController();
    ownController = true;
    const onCallerAbort = () => controller.abort(signal.reason);
    signal.addEventListener("abort", onCallerAbort, { once: true });
    controller.signal.addEventListener("abort", () => {
      signal.removeEventListener("abort", onCallerAbort);
    }, { once: true });
  } else {
    controller = new AbortController();
    ownController = true;
  }

  if (typeof timeoutMs === "number" && timeoutMs > 0) {
    timeoutHandle = setTimeout(() => {
      try { controller.abort(new Error(`fetchStream: timeout after ${timeoutMs}ms`)); }
      catch { /* ignore */ }
    }, timeoutMs);
  }

  let response;
  try {
    response = await fetch(url, {
      method: "GET",
      headers: headers && typeof headers === "object" ? headers : undefined,
      signal: controller.signal,
      // Don't keepalive: an aborted stream should release the socket.
      keepalive: false,
    });
  } catch (err) {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    // AbortError on both browser and Node 18+.
    const aborted = err && (err.name === "AbortError" || controller.signal.aborted);
    return { status: 0, headers: new Headers(), bodyBytes: 0, aborted: !!aborted, error: err };
  }

  const status = response.status;
  const respHeaders = response.headers;

  // Fast path: no body to stream.
  if (!response.body || typeof response.body.getReader !== "function") {
    if (timeoutHandle) clearTimeout(timeoutHandle);
    return { status, headers: respHeaders, bodyBytes: 0, aborted: false };
  }

  const reader = response.body.getReader();
  let bodyBytes = 0;
  let aborted = false;

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) {
        bodyBytes += value.byteLength;
        if (typeof onChunk === "function") {
          // Wrap in try/catch so a buggy consumer can't silently wedge us.
          try { onChunk(value, { bytesSoFar: bodyBytes }); }
          catch (cbErr) {
            controller.abort(cbErr);
            aborted = true;
            break;
          }
        }
        if (typeof maxBytes === "number" && maxBytes > 0 && bodyBytes > maxBytes) {
          controller.abort(new Error(`fetchStream: maxBytes exceeded (${bodyBytes} > ${maxBytes})`));
          aborted = true;
          break;
        }
      }
    }
  } catch (err) {
    aborted = true;
    if (timeoutHandle) clearTimeout(timeoutHandle);
    try { await reader.cancel(err); } catch { /* ignore */ }
    return { status, headers: respHeaders, bodyBytes, aborted, error: err };
  }

  if (timeoutHandle) clearTimeout(timeoutHandle);

  return { status, headers: respHeaders, bodyBytes, aborted };
}

// Optional CommonJS export so a Node script can `require()` this file
// without a build step. Harmless in ESM environments.
if (typeof module !== "undefined" && module.exports) {
  module.exports = { fetchStream };
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
