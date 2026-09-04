/**
 * fetch-with-stream-collect.js
 *
 * Reads a fetch Response stream into memory with:
 *   - optional maxBytes guard (so a huge response OOMs you)
 *   - content-type sniffing (JSON / text / binary)
 *   - a soft deadline independent of AbortSignal
 *
 * Designed for fetch-only agents (no Node Buffer, no npm). Uses only WHATWG
 * streams (ReadableStream.getReader) and TextEncoder/TextDecoder that are
 * available in modern browsers and Node >=18 globalThis.
 *
 * Usage:
 *   import collect from "./fetch-with-stream-collect.js";
 *   const { ok, status, contentType, json, text, bytes, bytesRead, truncated, elapsedMs } =
 *     await collect("https://example.com/api", { maxBytes: 1_000_000, timeoutMs: 5000 });
 *
 *   if (truncated) console.warn("response exceeded maxBytes");
 *   else if (contentType.includes("json")) console.log(json);
 *
 * Returns a structured result; never throws on non-2xx (you check `ok`).
 * Only throws for network errors or aborted reads.
 */

const DEFAULTS = {
  maxBytes: 25 * 1024 * 1024, // 25 MiB safety cap
  timeoutMs: 30_000,          // soft per-request deadline
  // If true, stop accumulating as soon as maxBytes is exceeded but keep
  // draining the stream so the connection can be closed cleanly.
  stopOnLimit: true,
};

export async function collect(url, init = {}) {
  if (typeof url !== "string" && !(url instanceof Request)) {
    throw new TypeError("collect(url, init): url must be a string or Request");
  }
  const opts = { ...DEFAULTS, ...init };

  // Build an AbortController that fires on either the caller's signal or our
  // own timeoutMs, whichever comes first.
  const ctl = new AbortController();
  const started = now();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    try { ctl.abort(new DOMException("TimeoutError", "TimeoutError")); } catch { ctl.abort(); }
  }, opts.timeoutMs);

  if (init.signal) {
    if (init.signal.aborted) ctl.abort(init.signal.reason);
    else init.signal.addEventListener("abort", () => ctl.abort(init.signal.reason), { once: true });
  }

  let res;
  try {
    res = await fetch(url, { ...init, signal: ctl.signal });
  } catch (err) {
    clearTimeout(timer);
    return {
      ok: false,
      status: 0,
      statusText: "",
      headers: new Headers(),
      contentType: "",
      json: null,
      text: "",
      bytes: new Uint8Array(0),
      bytesRead: 0,
      truncated: false,
      timedOut,
      elapsedMs: now() - started,
      error: err,
    };
  }

  const contentType = res.headers.get("content-type") || "";
  const headers = res.headers;

  // No body case (HEAD, 204, 304).
  if (res.body == null) {
    clearTimeout(timer);
    return {
      ok: res.ok,
      status: res.status,
      statusText: res.statusText,
      headers,
      contentType,
      json: null,
      text: "",
      bytes: new Uint8Array(0),
      bytesRead: 0,
      truncated: false,
      timedOut: false,
      elapsedMs: now() - started,
      error: null,
    };
  }

  const reader = res.body.getReader();
  const chunks = [];
  let bytesRead = 0;
  let truncated = false;

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;

      bytesRead += value.byteLength;

      if (!truncated) {
        if (bytesRead <= opts.maxBytes) {
          chunks.push(value);
        } else {
          // Keep the prefix that fits, drop the rest, but keep reading so
          // the socket can be reused / closed cleanly.
          const overflow = bytesRead - opts.maxBytes;
          const keep = value.byteLength - overflow;
          if (keep > 0) chunks.push(value.subarray(0, keep));
          truncated = true;
          if (opts.stopOnLimit) {
            try { reader.cancel("maxBytes exceeded"); } catch {}
          }
        }
      }
    }
  } catch (err) {
    clearTimeout(timer);
    try { reader.releaseLock(); } catch {}
    return {
      ok: false,
      status: res.status,
      statusText: res.statusText,
      headers,
      contentType,
      json: null,
      text: "",
      bytes: new Uint8Array(0),
      bytesRead,
      truncated,
      timedOut,
      elapsedMs: now() - started,
      error: err,
    };
  } finally {
    try { reader.releaseLock(); } catch {}
    clearTimeout(timer);
  }

  const bytes = concatBytes(chunks);
  const ct = contentType.toLowerCase();
  let json = null;
  let text = "";
  if (ct.includes("json")) {
    try {
      text = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
      json = JSON.parse(text);
    } catch {
      json = null; // fall through; caller can still read .text
    }
  } else {
    text = new TextDecoder("utf-8", { fatal: false }).decode(bytes);
  }

  return {
    ok: res.ok,
    status: res.status,
    statusText: res.statusText,
    headers,
    contentType,
    json,
    text,
    bytes,
    bytesRead,
    truncated,
    timedOut,
    elapsedMs: now() - started,
    error: null,
  };
}

function concatBytes(chunks) {
  if (chunks.length === 0) return new Uint8Array(0);
  if (chunks.length === 1) return chunks[0];
  let total = 0;
  for (const c of chunks) total += c.byteLength;
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) {
    out.set(c, off);
    off += c.byteLength;
  }
  return out;
}

function now() {
  if (typeof performance !== "undefined" && typeof performance.now === "function") {
    return performance.now();
  }
  return Date.now();
}

export default collect;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
