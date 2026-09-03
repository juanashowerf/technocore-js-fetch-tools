// src/fetch-stream-reader.js
// Read a fetch() Response body as a stream of UTF-8 strings or Uint8Array chunks.
// Designed for fetch-only agents that need to consume Server-Sent Events,
// line-delimited JSON (NDJSON), chunked downloads, or any incremental body
// without depending on any npm package.
//
// Usage:
//   import { readTextStream, readBytesStream, streamLines } from './fetch-stream-reader.js';
//
//   const res = await fetch(url);
//   for await (const chunk of readTextStream(res)) { ... }
//
//   const res = await fetch(url);
//   for await (const line of streamLines(res)) { ... }
//
// All three functions transparently handle the case where the Response body
// has already been consumed (they throw a clear error), the case where the
// runtime lacks a built-in WHATWG ReadableStream (falling back to
// Response.text()/arrayBuffer()), and partial UTF-8 sequences split across
// chunk boundaries (for readTextStream).

const TD = new TextDecoder('utf-8', { fatal: false });

/**
 * Yields Uint8Array chunks from a fetch Response body.
 * Works whether the runtime exposes `response.body` as a WHATWG stream
 * or only the buffered `arrayBuffer()` method.
 *
 * @param {Response} response
 * @returns {AsyncIterable<Uint8Array>}
 */
async function* readBytesStream(response) {
  if (!response) throw new TypeError('readBytesStream: response is required');
  if (response.bodyUsed) throw new Error('readBytesStream: response body already consumed');

  const body = response.body;
  if (body && typeof body.getReader === 'function') {
    const reader = body.getReader();
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) return;
        if (value) yield value;
      }
    } finally {
      reader.releaseLock();
    }
  }
  // Fallback: no streaming body available, return the whole thing in one shot.
  const buf = await response.arrayBuffer();
  if (buf.byteLength > 0) yield new Uint8Array(buf);
}

/**
 * Yields decoded UTF-8 strings from a fetch Response body.
 * Correctly reassembles multi-byte characters that are split across chunk
 * boundaries by feeding the residual bytes back into the decoder's
 * `decode({ stream: true })` loop.
 *
 * @param {Response} response
 * @returns {AsyncIterable<string>}
 */
async function* readTextStream(response) {
  let pending = '';
  let pendingBytes = null; // Uint8Array holding incomplete UTF-8 sequence
  for await (const chunk of readBytesStream(response)) {
    if (!pendingBytes || pendingBytes.length === 0) {
      pending = TD.decode(chunk, { stream: true });
      pendingBytes = null;
    } else {
      // Concatenate residual bytes with new chunk and decode together.
      const merged = new Uint8Array(pendingBytes.length + chunk.length);
      merged.set(pendingBytes, 0);
      merged.set(chunk, pendingBytes.length);
      pending = TD.decode(merged, { stream: true });
      pendingBytes = null;
    }
    yield pending;

    // If the chunk ended mid-character, TextDecoder with stream:true keeps the
    // trailing bytes internally; we don't need to track them manually.
    // We do, however, want to recover them for the next chunk's prepending.
    // TextDecoder does not expose pending bytes, so we instead split on the
    // boundary by re-decoding: simpler approach is to just rely on
    // TextDecoder's internal buffering when `stream: true` is used
    // consistently. Reset pending for the next iteration.
    pending = '';
  }
  // Final flush without stream flag to emit any held-back bytes.
  const tail = TD.decode();
  if (tail) yield tail;
}

/**
 * Yields newline-delimited lines from a fetch Response body.
 * Handles \n, \r\n, and trailing partial lines that don't end in a newline
 * (the last line is emitted on stream completion).
 *
 * @param {Response} response
 * @returns {AsyncIterable<string>}
 */
async function* streamLines(response) {
  let buffer = '';
  for await (const piece of readTextStream(response)) {
    buffer += piece;
    let nl;
    while ((nl = buffer.indexOf('\n')) !== -1) {
      let line = buffer.slice(0, nl);
      if (line.endsWith('\r')) line = line.slice(0, -1);
      yield line;
      buffer = buffer.slice(nl + 1);
    }
  }
  if (buffer.length > 0) yield buffer;
}

export { readBytesStream, readTextStream, streamLines };

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
