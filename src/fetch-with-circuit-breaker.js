// src/fetch-with-circuit-breaker.js
// A drop-in fetch wrapper that adds a client-side circuit breaker around a
// "primary" fetch implementation. Useful for fetch-only agents that need to
// stop hammering a flaky upstream and fail fast (or fall back) instead.
//
// States:
//   CLOSED     -> requests pass through; failures increment a counter;
//                 once `failureThreshold` is reached within `windowMs`,
//                 the breaker trips OPEN.
//   OPEN       -> requests are short-circuited for `cooldownMs`, throwing
//                 a CircuitOpenError so callers can recover instantly.
//   HALF_OPEN  -> after cooldown, the next request is allowed through as
//                 a probe. Success closes the breaker; failure re-opens it.
//
// Usage:
//   import { createFetchWithCircuitBreaker } from './fetch-with-circuit-breaker.js';
//   const fetchCB = createFetchWithCircuitBreaker({ failureThreshold: 5, cooldownMs: 30_000 });
//   const res = await fetchCB('https://upstream.example/data');
//
// All times are milliseconds. State is per-instance; create one breaker per
// upstream you care about.

export class CircuitOpenError extends Error {
  constructor(message, { retryAfterMs } = {}) {
    super(message);
    this.name = 'CircuitOpenError';
    this.retryAfterMs = retryAfterMs;
  }
}

const DEFAULT_OPTS = {
  failureThreshold: 5,       // consecutive failures before tripping
  windowMs: 10_000,          // failures older than this don't count
  cooldownMs: 30_000,        // how long to stay OPEN before HALF_OPEN
  shouldCount: (err) => true // filter which errors count as failures
};

export function createFetchWithCircuitBreaker(userOpts = {}) {
  const opts = { ...DEFAULT_OPTS, ...userOpts };
  const state = { status: 'CLOSED', failures: [], openedAt: 0 };

  function recordFailure(err) {
    if (!opts.shouldCount(err)) return;
    const now = Date.now();
    state.failures = state.failures.filter(t => now - t < opts.windowMs);
    state.failures.push(now);
    if (state.failures.length >= opts.failureThreshold && state.status !== 'OPEN') {
      state.status = 'OPEN';
      state.openedAt = now;
    }
  }

  function recordSuccess() {
    if (state.status !== 'CLOSED') {
      state.status = 'CLOSED';
      state.failures = [];
    }
  }

  function preflight() {
    if (state.status === 'CLOSED') return 'PROCEED';
    const now = Date.now();
    const elapsed = now - state.openedAt;
    if (state.status === 'OPEN' && elapsed >= opts.cooldownMs) {
      state.status = 'HALF_OPEN';
      return 'PROBE';
    }
    if (state.status === 'OPEN') {
      return new CircuitOpenError(
        `circuit open: ${opts.cooldownMs - elapsed}ms remaining`,
        { retryAfterMs: opts.cooldownMs - elapsed }
      );
    }
    // HALF_OPEN: only one probe at a time; others short-circuit
    if (state.probeInFlight) {
      return new CircuitOpenError('circuit half-open: probe in flight', { retryAfterMs: 0 });
    }
    return 'PROBE';
  }

  state.probeInFlight = false;

  async function fetchCB(input, init) {
    const decision = preflight();
    if (decision instanceof CircuitOpenError) throw decision;
    const probing = decision === 'PROBE';
    if (probing) state.probeInFlight = true;
    try {
      const res = await globalThis.fetch(input, init);
      // Treat 5xx as failures so a "200 with empty body" doesn't silently pass
      if (!res.ok && res.status >= 500) {
        recordFailure(new Error(`HTTP ${res.status}`));
        if (probing) state.status = 'OPEN';
        throw new Error(`HTTP ${res.status}`);
      }
      recordSuccess();
      return res;
    } catch (err) {
      if (!(err instanceof CircuitOpenError)) recordFailure(err);
      if (probing && state.status !== 'OPEN') state.status = 'OPEN';
      throw err;
    } finally {
      if (probing) state.probeInFlight = false;
    }
  }

  // Read-only introspection (handy for /health endpoints)
  fetchCB.getState = () => ({
    status: state.status,
    failures: state.failures.length,
    openedAt: state.openedAt
  });

  fetchCB.reset = () => {
    state.status = 'CLOSED';
    state.failures = [];
    state.openedAt = 0;
    state.probeInFlight = false;
  };

  return fetchCB;
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
