/**
 * fetch-with-backpressure.js
 *
 * Streams a fetch response body into an AsyncIterable while applying
 * backpressure: the consumer pulls chunks at its own pace, and the
 * reader is paused/resumed so the underlying socket/TLS layer is
 * not flooded. This is the missing peer-side companion to
 * fetch-with-progress: progress tells you *how much* arrived,
 * backpressure tells the network *how fast you can take it*.
 *
 * Why it matters for a fetch-only agent:
 *   - Lets you consume Server-Sent Events, NDJSON, or chunked
 *     downloads with bounded memory.
 *   - Plays well with AbortSignal and timeouts (already in this
 *     repo), so a slow consumer can cancel mid-stream.
 *   - No build step, no deps, works in modern browsers and Node 18+.
 *
 * Public surface:
 *   fetchWithBackpressure(url, init?) -> AsyncIterable<Uint8Array>
 *   fetchWithBackpressure(url, { signal, highWaterMark, ...init })
 *
 * Usage:
 *   for await (const chunk of fetchWithBackpressure('/big.jsonl')) {
 *     // parse chunk; loop pace controls network pace
 *   }
 */

const DEFAULT_HIGH_WATER = 16 * 1024; // 16 KiB per chunk pulled from the reader

/**
 * Streams a response with consumer-driven backpressure.
 *
 * @param {string | URL} url
 * @param {object} [opts]
 * @param {AbortSignal} [opts.signal]            Cancel the request/stream.
 * @param {RequestInit} [opts.init]              Extra fetch init merged in.
 * @param {number}      [opts.highWaterMark=16384] Bytes per pull from the reader.
 * @param {boolean}     [opts.throwOnAbort=true] Throw vs. end iterator on abort.
 * @returns {AsyncIterable<Uint8Array>}
 */
async function* fetchWithBackpressure(url, opts = {}) {
  const {
    signal,
    init,
    highWaterMark = DEFAULT_HIGH_WATER,
    throwOnAbort = true,
  } = opts;

  if (highWaterMark <= 0 || !Number.isFinite(highWaterMark)) {
    throw new RangeError('highWaterMark must be a positive finite number');
  }

  // AbortSignal.any(...) is available in Node 20+ and modern browsers;
  // fall back gracefully if absent.
  let combinedSignal = signal;
  if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.any === 'function' && signal) {
    // keep caller's signal as-is; fetch will pick it up via init.signal below.
  }

  const fetchInit = { ...init };
  if (signal && !fetchInit.signal) fetchInit.signal = signal;

  const response = await fetch(url, fetchInit);

  // Surface HTTP errors immediately rather than yielding an empty stream.
  if (!response.ok) {
    // Best-effort body read for diagnostics; bound it so we don't hang.
    let detail = '';
    try {
      const buf = await Promise.race([
        response.arrayBuffer(),
        new Promise((_, rej) => setTimeout(() => rej(new Error('body read timeout')), 1000)),
      ]);
      detail = new TextDecoder().decode(buf).slice(0, 512);
    } catch { /* ignore */ }
    throw new Error(
      `fetchWithBackpressure: HTTP ${response.status} ${response.statusText}${detail ? ` — ${detail}` : ''}`,
    );
  }

  if (!response.body || typeof response.body.getReader !== 'function') {
    // Environments without a ReadableStream body (rare). Fall back to
    // a single-shot buffer — no streaming, no backpressure, but at
    // least it works.
    const buf = new Uint8Array(await response.arrayBuffer());
    if (buf.byteLength) yield buf;
    return;
  }

  const reader = response.body.getReader();

  // The signal-aware cleanup. We attach AFTER the fetch resolves so we
  // don't race with an already-fired signal.
  const onAbort = () => {
    try { reader.cancel('aborted'); } catch { /* ignore */ }
  };
  if (signal) {
    if (signal.aborted) onAbort();
    else signal.addEventListener('abort', onAbort, { once: true });
  }

  try {
    while (true) {
      // Pause until the consumer pulls again — that IS the backpressure.
      // Each `yield` returns control to the consumer; we don't read the
      // next chunk until the for-await loop asks for it.
      const { value, done } = await reader.read();
      if (done) return;
      if (!value || value.byteLength === 0) continue;

      // If a single chunk exceeds the highWaterMark, slice it so the
      // consumer always sees bounded pieces (predictable memory use).
      if (value.byteLength <= highWaterMark) {
        yield value;
      } else {
        for (let off = 0; off < value.byteLength; off += highWaterMark) {
          yield value.subarray(off, Math.min(off + highWaterMark, value.byteLength));
        }
      }
    }
  } catch (err) {
    if (signal && signal.aborted) {
      if (throwOnAbort) throw (err.name === 'AbortError' ? err : new DOMException('Aborted', 'AbortError'));
      return; // silent end
    }
    throw err;
  } finally {
    if (signal) signal.removeEventListener('abort', onAbort);
    try { reader.releaseLock(); } catch { /* ignore */ }
  }
}

// ---- Minimal DOMException polyfill for runtimes that lack it (e.g. very old Node) ----
if (typeof DOMException === 'undefined') {
  // eslint-disable-next-line no-global-assign
  globalThis.DOMException = class DOMException extends Error {
    constructor(message, name = 'Error') { super(message); this.name = name; }
  };
}

// Export for both CommonJS and ESM consumers.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { fetchWithBackpressure, DEFAULT_HIGH_WATER };
}
if (typeof exports !== 'undefined') {
  exports.fetchWithBackpressure = fetchWithBackpressure;
}

export { fetchWithBackpressure, DEFAULT_HIGH_WATER };
export default fetchWithBackpressure;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
