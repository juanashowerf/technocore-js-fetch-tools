// fetch-with-circuit-breaker.js
// A zero-dependency circuit breaker wrapper around the standard fetch() API,
// designed for fetch-only browser/JS agents that need resilience against
// cascading failures when calling flaky or overloaded HTTP services.
//
// Concepts:
//   - CLOSED:     requests pass through normally; failures count toward the threshold.
//   - OPEN:       requests short-circuit immediately with a synthetic Response (no network call).
//   - HALF_OPEN:  one probe request is allowed through to test recovery; success closes the circuit,
//                 failure re-opens it for another cooldown.
//
// Usage:
//   import { createCircuitBreakerFetch } from './fetch-with-circuit-breaker.js';
//   const fetchCB = createCircuitBreakerFetch({ windowMs: 30_000, failureThreshold: 5, cooldownMs: 15_000 });
//   const res = await fetchCB('https://api.example.com/data');
//
// The returned Response object is the real one from fetch() when the circuit is closed or the
// probe succeeds. When the circuit is open, a synthetic Response is returned with status 503 and
// a JSON body explaining the situation. Callers can branch on res.ok or res.status === 503.

const DEFAULT_OPTIONS = {
  // How many failures within `windowMs` will trip the breaker OPEN.
  failureThreshold: 5,
  // Sliding window for counting failures, in milliseconds.
  windowMs: 30_000,
  // How long the breaker stays OPEN before allowing a HALF_OPEN probe.
  cooldownMs: 15_000,
  // Which HTTP statuses count as "failure" for breaker accounting.
  // Network errors (TypeError thrown by fetch) always count as failures.
  failureStatuses: (status) => status >= 500 || status === 429,
  // Optional: a clock function for tests; defaults to Date.now.
  now: () => Date.now(),
  // Optional: which fetch implementation to use (defaults to globalThis.fetch).
  fetchImpl: typeof fetch !== 'undefined' ? fetch : undefined,
};

export function createCircuitBreakerFetch(userOptions = {}) {
  const opts = { ...DEFAULT_OPTIONS, ...userOptions };
  if (typeof opts.fetchImpl !== 'function') {
    throw new TypeError('createCircuitBreakerFetch: no fetch implementation available');
  }

  // State: { state: 'CLOSED'|'OPEN'|'HALF_OPEN', openedAt: number, failures: number[] }
  const state = { state: 'CLOSED', openedAt: 0, failures: [] };

  function pruneFailures(now) {
    const cutoff = now - opts.windowMs;
    while (state.failures.length && state.failures[0] < cutoff) {
      state.failures.shift();
    }
  }

  function recordFailure(now) {
    state.failures.push(now);
    pruneFailures(now);
    if (state.failures.length >= opts.failureThreshold) {
      state.state = 'OPEN';
      state.openedAt = now;
    }
  }

  function shouldAllowRequest(now) {
    if (state.state === 'CLOSED') return true;
    if (state.state === 'OPEN') {
      if (now - state.openedAt >= opts.cooldownMs) {
        state.state = 'HALF_OPEN';
        return true;
      }
      return false;
    }
    // HALF_OPEN: allow exactly one probe; subsequent concurrent requests short-circuit.
    // For simplicity we let one through per HALF_OPEN window by flipping to OPEN again
    // until the next cooldown elapses. If you need true concurrency control, layer
    // fetch-with-deduplication.js on top.
    if (state.state === 'HALF_OPEN') {
      state.state = 'OPEN';
      state.openedAt = now;
      return true;
    }
    return false;
  }

  function syntheticUnavailable(reason) {
    return new Response(
      JSON.stringify({ error: 'circuit_open', message: reason }),
      { status: 503, headers: { 'content-type': 'application/json', 'x-circuit-state': 'open' } }
    );
  }

  async function circuitFetch(input, init) {
    const now = opts.now();
    if (!shouldAllowRequest(now)) {
      return syntheticUnavailable('Circuit breaker is OPEN; request short-circuited.');
    }
    const wasHalfOpen = state.state === 'HALF_OPEN';
    let res;
    try {
      res = await opts.fetchImpl(input, init);
    } catch (err) {
      // Network-level error: count as failure and rethrow so callers can decide.
      if (state.state !== 'CLOSED') state.state = 'CLOSED';
      recordFailure(now);
      throw err;
    }
    if (opts.failureStatuses(res.status)) {
      recordFailure(now);
      if (wasHalfOpen) {
        // Probe failed: re-open and start cooldown again.
        state.state = 'OPEN';
        state.openedAt = now;
      }
    } else {
      // Success: close the circuit and clear failure history.
      state.state = 'CLOSED';
      state.failures = [];
    }
    return res;
  }

  // Introspection helpers so a calling agent can observe breaker health.
  circuitFetch.getState = () => ({ ...state, failures: state.failures.slice() });
  circuitFetch.reset = () => { state.state = 'CLOSED'; state.openedAt = 0; state.failures = []; };

  return circuitFetch;
}

// Example wiring for a fetch-only agent:
//   const fetchCB = createCircuitBreakerFetch({ failureThreshold: 3, cooldownMs: 10_000 });
//   export const safeFetch = fetchCB;
//   export const breakerState = () => fetchCB.getState();

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
