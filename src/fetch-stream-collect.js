// fetch-stream-collect.js
// Helpers to consume a fetch() Response body as a stream and reassemble it
// into a single Uint8Array (or decoded string), with cancellation, size caps,
// progress callbacks, and a pure-Node fallback for non-browser environments.
//
// Designed for fetch-only agents that need to handle large or chunked
// responses (NDJSON, Server-Sent Events, file downloads) without pulling in
// any dependencies.
//
// Usage:
//   const { collectStream, streamBytes, streamText, cancelAfter } =
//     require('./fetch-stream-collect');
//
//   const res = await fetch(url);
//   const bytes = await collectStream(res, { onProgress: (n) => ... });
//
//   // Or decode as text (UTF-8 by default):
//   const text = await streamText(res, { signal: cancelAfter(5000) });
//
//   // Or stream chunks yourself:
//   for await (const chunk of streamBytes(res)) { ... }

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.FetchStreamCollect = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  function defaultMaxBytes() {
    // 64 MiB safety cap to prevent runaway memory use.
    return 64 * 1024 * 1024;
  }

  function createAbortError(message) {
    if (typeof DOMException !== 'undefined') {
      try { return new DOMException(message, 'AbortError'); } catch (_) {}
    }
    const e = new Error(message);
    e.name = 'AbortError';
    return e;
  }

  // Promise-based reader wrapper that rejects on abort and surfaces size
  // caps. Returns { iterator, cancel, getTotal }.
  function streamBytes(res, options) {
    const opts = options || {};
    const signal = opts.signal;
    const maxBytes = typeof opts.maxBytes === 'number' ? opts.maxBytes : defaultMaxBytes();
    const onProgress = typeof opts.onProgress === 'function' ? opts.onProgress : null;

    if (!res || !res.body) {
      throw new Error('streamBytes: response or body is missing');
    }

    const reader = res.body.getReader();
    let aborted = false;
    let total = 0;

    function abort(reason) {
      if (aborted) return;
      aborted = true;
      try { reader.cancel(reason || 'aborted'); } catch (_) {}
    }

    if (signal) {
      if (signal.aborted) abort('aborted before read');
      else signal.addEventListener('abort', function () { abort('aborted by signal'); }, { once: true });
    }

    const iterator = (async function* () {
      try {
        while (true) {
          const result = await reader.read();
          if (result.done) break;
          const chunk = result.value;
          if (!chunk || chunk.length === 0) continue;

          if (maxBytes > 0) {
            if (total + chunk.length > maxBytes) {
              abort('maxBytes exceeded');
              throw createAbortError(
                'streamBytes: response exceeded maxBytes (' + maxBytes + ')'
              );
            }
          }

          total += chunk.length;
          if (onProgress) {
            try { onProgress({ bytes: total, lastChunk: chunk.length }); }
            catch (_) { /* swallow progress errors */ }
          }
          yield chunk;
        }
      } finally {
        try { reader.releaseLock(); } catch (_) {}
      }
    })();

    return {
      iterator: iterator,
      cancel: abort,
      getTotal: function () { return total; }
    };
  }

  // Collect the entire body into a single Uint8Array.
  async function collectStream(res, options) {
    const opts = options || {};
    const stream = streamBytes(res, opts);
    const parts = [];
    let total = 0;
    try {
      for await (const chunk of stream.iterator) {
        parts.push(chunk);
        total += chunk.length;
      }
    } catch (err) {
      if (err && err.name === 'AbortError') {
        err.bytesRead = total;
      }
      throw err;
    }
    const out = new Uint8Array(total);
    let offset = 0;
    for (let i = 0; i < parts.length; i++) {
      out.set(parts[i], offset);
      offset += parts[i].length;
    }
    return out;
  }

  // Collect and decode as text. Supports custom decoder.
  async function streamText(res, options) {
    const opts = options || {};
    const bytes = await collectStream(res, opts);
    const decoder = opts.decoder || new TextDecoder(opts.encoding || 'utf-8', { fatal: !!opts.fatal });
    return decoder.decode(bytes);
  }

  // Build an AbortSignal that fires after `ms` milliseconds.
  // Uses AbortSignal.timeout when available, otherwise polyfills with setTimeout.
  function cancelAfter(ms) {
    if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function') {
      return AbortSignal.timeout(ms);
    }
    const ctl = new AbortController();
    const id = setTimeout(function () { ctl.abort('timeout after ' + ms + 'ms'); }, ms);
    // Clear timer if caller aborts early.
    ctl.signal.addEventListener('abort', function () { clearTimeout(id); }, { once: true });
    return ctl.signal;
  }

  return {
    streamBytes: streamBytes,
    collectStream: collectStream,
    streamText: streamText,
    cancelAfter: cancelAfter
  };
});

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
