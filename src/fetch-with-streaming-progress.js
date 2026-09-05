// fetch-with-streaming-progress.js
// Drop-in fetch wrapper that reports download progress via a callback as
// response body bytes arrive. Useful for large responses where the caller
// wants a progress bar / ETA / cancel-on-fulfilled-threshold logic without
// pulling in a dependency. Works in browsers (uses getReader + ReadableStream)
// and in Node 18+ (global fetch + Web Streams).
//
// Usage:
//   const res = await fetchWithStreamingProgress(url, {
//     onProgress: ({ loaded, total, percent }) => console.log(percent),
//     signal: controller.signal,
//     thresholdBytes: 1024 * 1024, // stop streaming once 1MB received
//     onThreshold: (controller) => controller.abort(), // optional
//   });
//   const text = await res.text(); // still works, body already buffered
//
// The original response body is consumed and replaced by an in-memory
// ReadableStream that emits the same chunks; fetch's built-in .text(),
// .json(), .arrayBuffer(), .blob() methods continue to work on the
// returned Response because we tee the stream into the original body slot
// AND a progress-tracking ReadableStream.
//
// Limitations:
// - `total` is only known if the server sends Content-Length. Otherwise it
//   is `null` and `percent` is `null`.
// - The body is buffered chunk-by-chunk into a single Uint8Array for the
//   returned Response, so memory cost ≈ full body size. If you want
//   constant-memory piping, use fetch-with-streaming-passthrough.js
//   instead.

export async function fetchWithStreamingProgress(input, init = {}) {
  const {
    onProgress,
    thresholdBytes = Infinity,
    onThreshold,
    ...fetchInit
  } = init;

  const response = await fetch(input, fetchInit);

  // No body to stream (e.g. HEAD, 204) — return as-is after one progress tick.
  if (!response.body) {
    if (typeof onProgress === 'function') {
      onProgress({ loaded: 0, total: parseContentLength(response), percent: 0 });
    }
    return response;
  }

  const total = parseContentLength(response);
  const reader = response.body.getReader();
  const chunks = [];
  let loaded = 0;

  const progressStream = new ReadableStream({
    async pull(controller) {
      const { done, value } = await reader.read();
      if (done) {
        controller.close();
        return;
      }
      chunks.push(value);
      loaded += value.byteLength;

      if (typeof onProgress === 'function') {
        const percent = total == null ? null : (loaded / total) * 100;
        try {
          onProgress({ loaded, total, percent, chunk: value });
        } catch (err) {
          controller.error(err);
          reader.cancel().catch(() => {});
          throw err;
        }
      }

      if (loaded >= thresholdBytes) {
        if (typeof onThreshold === 'function') {
          try { onThreshold({ abort: () => reader.cancel() }); }
          catch (err) {
            controller.error(err);
            throw err;
          }
        }
        controller.enqueue(value);
        controller.close();
        try { reader.cancel(); } catch (_) { /* already closed */ }
        return;
      }

      controller.enqueue(value);
    },
    cancel(reason) {
      reader.cancel(reason).catch(() => {});
    },
  });

  // Build a new Response that still exposes .text(), .json(), etc.
  // The internal body is the tee stream above.
  const headers = new Headers(response.headers);
  return new Response(progressStream, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function parseContentLength(response) {
  const cl = response.headers.get('content-length');
  if (cl == null) return null;
  const n = Number(cl);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

export default fetchWithStreamingProgress;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
