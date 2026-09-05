// fetch-with-circuit-breaker.js
// A no-build, dependency-free circuit breaker wrapper around fetch().
// Use it to stop hammering a sick downstream service once failures pile up,
// then probe it again after a cool-down window. Designed to compose with the
// other fetch-* helpers in this repo.
//
// Usage:
//   import { createCircuitBreaker } from './fetch-with-circuit-breaker.js';
//   const breaker = createCircuitBreaker({ failureThreshold: 5, resetTimeoutMs: 30_000 });
//   const res = await breaker.fetch('https://api.example.com/things');
//
// Options:
//   failureThreshold   number of consecutive failures before opening (default 5)
//   resetTimeoutMs      ms to wait in OPEN before moving to HALF_OPEN (default 30000)
//   successThreshold    successes in HALF_OPEN required to close again (default 1)
//   isFailure(err, res) optional predicate; default treats non-2xx and thrown errors as failures
//   now()               injectable clock for tests
//   name                label used in thrown errors (default 'circuit')
//
// States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED (or back to OPEN on failure).

export function createCircuitBreaker(opts = {}) {
  const failureThreshold = opts.failureThreshold ?? 5;
  const resetTimeoutMs = opts.resetTimeoutMs ?? 30_000;
  const successThreshold = opts.successThreshold ?? 1;
  const isFailure = opts.isFailure || defaultIsFailure;
  const now = opts.now || (() => Date.now());
  const name = opts.name || 'circuit';

  let state = 'CLOSED';
  let consecutiveFailures = 0;
  let halfOpenSuccesses = 0;
  let openedAt = 0;
  const subscribers = new Set();

  function snapshot() {
    return { state, consecutiveFailures, halfOpenSuccesses, openedAt };
  }

  function subscribe(fn) {
    subscribers.add(fn);
    return () => subscribers.delete(fn);
  }

  function emit(event) {
    for (const fn of subscribers) {
      try { fn(event, snapshot()); } catch { /* ignore observer errors */ }
    }
  }

  function open() {
    state = 'OPEN';
    openedAt = now();
    halfOpenSuccesses = 0;
    emit({ type: 'open' });
  }

  function close() {
    state = 'CLOSED';
    consecutiveFailures = 0;
    halfOpenSuccesses = 0;
    openedAt = 0;
    emit({ type: 'close' });
  }

  // Decide whether a call is allowed right now. Returns a short, structured
  // reason when it is not so callers can branch (e.g. fall back to cache).
  function allow() {
    if (state === 'CLOSED' || state === 'HALF_OPEN') return { ok: true };
    // OPEN: check if the cool-down has elapsed.
    if (now() - openedAt >= resetTimeoutMs) {
      state = 'HALF_OPEN';
      halfOpenSuccesses = 0;
      emit({ type: 'half-open' });
      return { ok: true };
    }
    const retryInMs = openedAt + resetTimeoutMs - now();
    return { ok: false, reason: 'open', retryInMs };
  }

  function recordSuccess() {
    if (state === 'HALF_OPEN') {
      halfOpenSuccesses += 1;
      if (halfOpenSuccesses >= successThreshold) close();
      return;
    }
    // CLOSED: reset failure streak on any success.
    consecutiveFailures = 0;
  }

  function recordFailure() {
    if (state === 'HALF_OPEN') {
      // Probe failed: trip the breaker again and reset the cool-down clock.
      open();
      return;
    }
    consecutiveFailures += 1;
    if (consecutiveFailures >= failureThreshold) open();
  }

  async function fetch(input, init) {
    const gate = allow();
    if (!gate.ok) {
      const err = new Error(`${name}: circuit is ${state}`);
      err.code = 'CIRCUIT_OPEN';
      err.retryInMs = gate.retryInMs;
      err.snapshot = snapshot();
      throw err;
    }
    let res;
    try {
      res = await globalThis.fetch(input, init);
    } catch (e) {
      recordFailure();
      throw e;
    }
    if (isFailure(null, res)) {
      recordFailure();
      return res; // surface the response so the caller can inspect it
    }
    recordSuccess();
    return res;
  }

  function defaultIsFailure(_err, res) {
    if (!res) return true;
    return res.status < 200 || res.status >= 300;
  }

  // Manual controls (useful for ops dashboards or tests).
  function forceOpen() { open(); }
  function forceClose() { close(); }
  function reset() { close(); }

  return {
    fetch,
    subscribe,
    snapshot,
    forceOpen,
    forceClose,
    reset,
    get state() { return state; },
  };
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
