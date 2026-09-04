// fetch-with-rate-limit.js
// Token-bucket rate limiter that wraps a fetch() function. Drops (or delays) calls
// once the bucket is empty so a fetch-only agent can talk to APIs with strict
// per-second quotas without writing a queue manually.
//
// Usage:
//   import { createRateLimitedFetch } from "./fetch-with-rate-limit.js";
//   const fetch = createRateLimitedFetch({ ratePerSecond: 5, burst: 10 });
//   await fetch("https://api.example.com/v1/items");
//
// Options:
//   ratePerSecond  steady-state tokens added per second (default 5)
//   burst          maximum bucket size (default = ratePerSecond)
//   strategy       "queue" (delay until a token is free, default) or "drop"
//                  ("drop" rejects immediately with a RateLimitError once empty)
//   fetchImpl      underlying fetch (defaults to globalThis.fetch)
//
// The returned function preserves the standard fetch signature and returns the
// same Response object, so it is a drop-in replacement for `fetch`.

export class RateLimitError extends Error {
  constructor(message = "rate limit exceeded, bucket empty") {
    super(message);
    this.name = "RateLimitError";
  }
}

export function createRateLimitedFetch(options = {}) {
  const {
    ratePerSecond = 5,
    burst = ratePerSecond,
    strategy = "queue",
    fetchImpl = globalThis.fetch,
  } = options;

  if (typeof ratePerSecond !== "number" || ratePerSecond <= 0) {
    throw new TypeError("ratePerSecond must be a positive number");
  }
  if (typeof burst !== "number" || burst <= 0) {
    throw new TypeError("burst must be a positive number");
  }
  if (strategy !== "queue" && strategy !== "drop") {
    throw new TypeError('strategy must be "queue" or "drop"');
  }
  if (typeof fetchImpl !== "function") {
    throw new TypeError("fetchImpl must be a function");
  }

  let tokens = burst;
  let lastRefill = Date.now();

  function refill() {
    const now = Date.now();
    const elapsedSeconds = (now - lastRefill) / 1000;
    if (elapsedSeconds > 0) {
      tokens = Math.min(burst, tokens + elapsedSeconds * ratePerSecond);
      lastRefill = now;
    }
  }

  function take() {
    refill();
    if (tokens >= 1) {
      tokens -= 1;
      return 0;
    }
    const deficit = 1 - tokens;
    return (deficit / ratePerSecond) * 1000; // ms until a token is free
  }

  const waiters = [];

  function scheduleWait(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        // remove self from waiters list
        const idx = waiters.indexOf(entry);
        if (idx >= 0) waiters.splice(idx, 1);
        resolve();
      }, ms);
      const entry = { timer };
      waiters.push(entry);
    });
  }

  async function rateLimitedFetch(input, init) {
    const waitMs = take();
    if (waitMs > 0) {
      if (strategy === "drop") {
        throw new RateLimitError();
      }
      await scheduleWait(waitMs);
      // another attempt; if a burst of callers drained the bucket while we
      // waited, recurse until we actually claim a token
      return rateLimitedFetch(input, init);
    }
    return fetchImpl(input, init);
  }

  rateLimitedFetch.getStats = () => {
    refill();
    return {
      tokens,
      capacity: burst,
      ratePerSecond,
      pendingWaiters: waiters.length,
      strategy,
    };
  };

  return rateLimitedFetch;
}

// Smoke test (run with: node src/fetch-with-rate-limit.js)
// Bypasses real network by injecting a fake fetch and checking token drain.
if (import.meta.url === `file://${process.argv[1]}`) {
  let calls = 0;
  const fakeFetch = async () => {
    calls += 1;
    return new Response("ok", { status: 200 });
  };
  const limited = createRateLimitedFetch({
    ratePerSecond: 2,
    burst: 3,
    fetchImpl: fakeFetch,
  });
  const results = await Promise.all([
    limited("https://x.test/1"),
    limited("https://x.test/2"),
    limited("https://x.test/3"),
  ]);
  console.log("burst calls served:", calls, "tokens left:", limited.getStats().tokens);
  console.log("response status:", results[0].status);

  // drop mode test
  const dropping = createRateLimitedFetch({
    ratePerSecond: 1,
    burst: 1,
    strategy: "drop",
    fetchImpl: fakeFetch,
  });
  await dropping("https://x.test/a");
  try {
    await dropping("https://x.test/b");
    console.log("ERROR: drop mode did not throw");
  } catch (err) {
    console.log("drop mode threw:", err.name);
  }
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
