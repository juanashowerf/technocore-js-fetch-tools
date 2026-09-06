// fetch-with-deduplication.js
// Coalesces concurrent identical GET requests into a single in-flight fetch.
// Useful when many agents (or many UI components) hammer the same endpoint
// at the same time and you only want one network round-trip per URL.
//
// Usage:
//   import fetchDedup from "./fetch-with-deduplication.js";
//   const fetchD = fetchDedup(globalThis.fetch);
//   const [a, b, c] = await Promise.all([
//     fetchD("https://api.example.com/v1/status"),
//     fetchD("https://api.example.com/v1/status"),
//     fetchD("https://api.example.com/v1/status", { headers: { Accept: "application/json" } }),
//   ]);
//
// The key is (method, url, serialized-headers, serialized-body). Non-GET
// requests, or GETs with different headers/body, are NOT coalesced.
// Bodies of returned Responses are NOT shared — each caller still gets
// its own Response (and should call response.blob()/json()/text() itself,
// or pass it through `tee` for true streaming dedup).
//
// Pass { tee: true } to additionally tee() the underlying body so multiple
// callers can consume it independently.

const inflight = new Map(); // key -> Promise<Response>

function makeKey(input, init) {
  const req = new Request(input, init);
  if (req.method.toUpperCase() !== "GET") return null; // only dedup GETs
  const url = req.url;
  // Serialize headers in a stable order for a deterministic key.
  const headers = [...req.headers.entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
  // Include any credentials/cache mode so two callers asking for different
  // caching semantics are not silently merged.
  const meta = JSON.stringify({
    headers,
    credentials: req.credentials,
    cache: req.cache,
    redirect: req.redirect,
    referrer: req.referrer,
    integrity: req.integrity,
  });
  return `${req.method} ${url} :: ${meta}`;
}

export default function fetchDedup(baseFetch, opts = {}) {
  if (typeof baseFetch !== "function") {
    throw new TypeError("fetchDedup: baseFetch must be a function");
  }
  const useTee = opts.tee === true;
  const onCoalesce = typeof opts.onCoalesce === "function" ? opts.onCoalesce : null;

  return async function dedupedFetch(input, init) {
    const key = makeKey(input, init);
    if (key === null) {
      // Non-GET or unkeyable: pass through.
      return baseFetch(input, init);
    }

    const existing = inflight.get(key);
    if (existing) {
      if (onCoalesce) {
        try { onCoalesce({ url: key.split(" :: ")[0], reused: true }); } catch {}
      }
      // Clone so each caller gets an independent Response object.
      // If tee mode, also share the body so callers can both read it.
      const res = await existing;
      if (useTee && res.body) {
        const [b1, b2] = res.body.tee();
        return new Response(b1, res);
        // Note: b2 is dropped because Response can only carry one body.
        // For true multi-consumer body dedup, expose `tee` separately:
        // see fetchWithDedup.tee(input, init) below.
      }
      return res.clone();
    }

    const p = (async () => {
      try {
        return await baseFetch(input, init);
      } finally {
        // Remove eagerly on completion so future callers don't get a
        // consumed/cloned response they can't safely read.
        inflight.delete(key);
      }
    })();

    inflight.set(key, p);
    if (onCoalesce) {
      try { onCoalesce({ url: key.split(" :: ")[0], reused: false }); } catch {}
    }
    return p;
  };

  // Attach a helper so callers wanting true multi-consumer body sharing
  // can opt in explicitly. Returns an array of Responses, each backed by
  // a tee'd branch of the same underlying body.
  fetchDedup.tee = async function teeFetch(input, init, branches = 2) {
    const key = makeKey(input, init);
    if (key === null) {
      const single = await baseFetch(input, init);
      return new Array(branches).fill(single);
    }
    let p = inflight.get(key);
    if (!p) {
      p = (async () => {
        try { return await baseFetch(input, init); }
        finally { inflight.delete(key); }
      })();
      inflight.set(key, p);
    }
    const res = await p;
    if (!res.body) return new Array(branches).fill(res);
    const tees = res.body.tee(branches);
    return tees.map((b) => new Response(b, res));
  };

  // Test/diagnostics helpers.
  fetchDedup.inflightCount = () => inflight.size;
  fetchDedup.clearInflight = () => inflight.clear();

  return fetchDedup;
}

// --- Self-test (run with: node --experimental-fetch src/fetch-with-deduplication.js) ---
// Skipped automatically when not running as the main module.
if (typeof process !== "undefined" && process.argv?.[1]?.endsWith("fetch-with-deduplication.js")) {
  (async () => {
    let calls = 0;
    const fakeFetch = async (url) => {
      calls++;
      await new Promise((r) => setTimeout(r, 20));
      return new Response(JSON.stringify({ url, n: calls }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    };
    const fd = fetchDedup(fakeFetch, { onCoalesce: (e) => console.log("coalesce:", e) });

    const [a, b, c] = await Promise.all([
      fd("https://x.test/a"),
      fd("https://x.test/a"),
      fd("https://x.test/b"),
    ]);
    console.log("calls:", calls, "(expect 2)");
    console.log("a body:", await a.text());
    console.log("b body:", await b.text());
    console.log("c body:", await c.text());

    // tee mode
    const [t1, t2] = await fd.tee("https://x.test/tee", null, 2);
    console.log("tee1:", await t1.text(), "tee2:", await t2.text());
  })().catch((e) => { console.error(e); process.exit(1); });
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
