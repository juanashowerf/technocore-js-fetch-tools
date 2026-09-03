// examples/fetch-post-json.js
// Demonstrates a complete POST of JSON to an HTTP endpoint using only the
// browser/JS built-in fetch API — no bundler, no dependencies, no polyfills.
// Drop this file into any static page (or run it under Node 18+) and it works.
//
// What it shows, end to end:
//   1. Building a request with method, headers, and a JSON body.
//   2. Letting fetch do the JSON.stringify for you via JSON body helpers
//      where supported, and falling back to explicit stringify + content-type.
//   3. Distinguishing network failures (fetch rejects) from HTTP errors
//      (response.ok === false) — a common foot-gun for fetch-only agents.
//   4. Parsing the response body safely, with a timeout via AbortSignal.
//   5. Returning a single result envelope that is itself easy to send back
//      upstream, so a fetch-only agent can chain calls without custom glue.
//
// The exported `postJson(url, payload, opts)` function is the piece a
// fetch-only agent would actually carry around. Everything else is a runnable
// example.

"use strict";

/**
 * Issue a JSON POST and return a normalised result envelope.
 *
 * @param {string} url           Absolute URL to POST to.
 * @param {unknown} payload      Any JSON-serialisable value.
 * @param {object} [opts]
 * @param {Record<string,string>} [opts.headers]  Extra request headers.
 * @param {number} [opts.timeoutMs=15000]         Abort after N ms.
 * @param {RequestInit['credentials']} [opts.credentials="omit"]
 * @returns {Promise<
 *   { ok: true, status: number, headers: Headers, data: unknown }
 *   | { ok: false, status: number | null, error: string, data?: unknown }
 * >}
 */
async function postJson(url, payload, opts = {}) {
  const { headers: extraHeaders, timeoutMs = 15000, credentials = "omit" } = opts;

  // Compose headers. We set Content-Type explicitly so the body shape is
  // unambiguous to the server, even on environments whose fetch lacks the
  // experimental `body: JSON` helper.
  const headers = new Headers({
      "Accept": "application/json",
      "Content-Type": "application/json; charset=utf-8",
      "User-Agent": "fetch-forge/1.0 (+technocore-js-fetch-tools)",
      ...extraHeaders,
  });

  // Timeout via AbortSignal.timeout when available, otherwise a manual controller.
  const signal = (typeof AbortSignal !== "undefined" && "timeout" in AbortSignal)
    ? AbortSignal.timeout(timeoutMs)
    : (() => { const c = new AbortController(); setTimeout(() => c.abort(new Error("timeout")), timeoutMs); return c.signal; })();

  let res;
  try {
    res = await fetch(url, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      credentials,
      mode: "cors",
      redirect: "follow",
      signal,
    });
  } catch (err) {
    // Network / DNS / TLS / CORS-preflight / abort all land here.
    const error = (err && err.name === "AbortError") ? "timeout" : String(err && err.message || err);
    return { ok: false, status: null, error };
  }

  // Read the body once; try JSON first, fall back to text so we never lose it.
  let data;
  const ct = res.headers.get("content-type") || "";
  try {
    data = ct.includes("application/json") ? await res.json() : await res.text();
  } catch (_) {
    data = null;
  }

  if (!res.ok) {
    return { ok: false, status: res.status, error: `HTTP ${res.status}`, data };
  }
  return { ok: true, status: res.status, headers: res.headers, data };
}

// -------------------------------------------------------------------------
// Runnable example: a self-contained demo against httpbin.org/post, which
// echoes the request back. Open this file in a browser or run with Node 18+.
// -------------------------------------------------------------------------
if (typeof window !== "undefined" || (typeof process !== "undefined" && process.versions && process.versions.node)) {
  const isMain = typeof require !== "undefined" && typeof module !== "undefined" && require.main === module;
  const isBrowserMain = typeof window !== "undefined" && typeof window.document !== "undefined";
  if (isMain || (isBrowserMain && window.location.search.includes("run=1"))) {
    (async () => {
      const out = document.getElementById ? document.getElementById("out") : null;
      const log = (line) => { if (out) out.textContent += line + "\n"; else console.log(line); };
      try {
        log("[fetch-forge] POSTing JSON to https://httpbin.org/post ...");
        const result = await postJson(
          "https://httpbin.org/post",
          {
            agent: "fetch-forge",
            did: "did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq",
            ts: new Date().toISOString(),
            note: "hello from a no-build fetch-only agent",
          },
          { timeoutMs: 10000 }
        );
        log("[fetch-forge] ok=" + result.ok + " status=" + result.status);
        log("[fetch-forge] echoed json keys: " + (result.ok ? Object.keys(result.data.json || {}).join(",") : "n/a"));
        if (!result.ok) log("[fetch-forge] error: " + result.error);
      } catch (e) {
        log("[fetch-forge] unexpected: " + e);
      }
    })();
  }
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { postJson };
}
if (typeof window !== "undefined") {
  window.fetchForge = Object.assign(window.fetchForge || {}, { postJson });
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
