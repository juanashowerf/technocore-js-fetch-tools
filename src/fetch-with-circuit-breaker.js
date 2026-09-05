// fetch-with-circuit-breaker.js
// Wrap the global fetch() with a circuit breaker so that a failing/overloaded
// remote service stops being hammered. Once the failure threshold is crossed
// within a sliding window, the circuit OPENS and calls short-circuit with a
// fast error until the cooldown elapses, at which point a single trial request
// is allowed (HALF_OPEN) to decide whether to CLOSE again or stay OPEN.
//
// No deps. Works in browsers and modern Node (>=18 global fetch).
//
// Usage:
//   import circuitFetch from './fetch-with-circuit-breaker.js';
//   const fetchCB = circuitFetch({ failureThreshold: 5, cooldownMs: 30_000 });
//   const res = await fetchCB('https://api.example.com/x');
//
// You can also share a breaker across multiple URLs by passing `name`:
//   const f = circuitFetch({ name: 'upstream-A', failureThreshold: 3 });

const breakers = new Map(); // name -> state

function defaultIsFailure(res) {
  // Treat 5xx and 429 as failures; everything else as success.
  return res.status >= 500 || res.status === 429;
}

function createBreaker(name, opts) {
  return {
    name,
    state: 'CLOSED',         // CLOSED | OPEN | HALF_OPEN
    failures: 0,             // consecutive failures in current window
    openedAt: 0,             // ms timestamp when we last opened
    halfOpenInFlight: false, // ensure only one trial request when HALF_OPEN
    opts,
  };
}

function getBreaker(name, opts) {
  let b = breakers.get(name);
  if (!b) {
    b = createBreaker(name, opts);
    breakers.set(name, b);
  } else {
    // Live-update tunable options on an existing breaker.
    b.opts = { ...b.opts, ...opts };
  }
  return b;
}

class CircuitOpenError extends Error {
  constructor(breakerName, retryAfterMs) {
    super(`Circuit "${breakerName}" is OPEN; retry after ${retryAfterMs}ms`);
    this.name = 'CircuitOpenError';
    this.retryAfterMs = retryAfterMs;
    this.breakerName = breakerName;
  }
}

/**
 * Build a fetch wrapper with a circuit breaker.
 *
 * @param {object}  [options]
 * @param {string}  [options.name='default']        Breaker instance name (share across callers).
 * @param {number}  [options.failureThreshold=5]    Consecutive failures before opening.
 * @param {number}  [options.cooldownMs=30000]      Time to wait before HALF_OPEN trial.
 * @param {function(Response):boolean} [options.isFailure]
 *        Predicate classifying a Response as a failure. Defaults to 5xx/429.
 * @param {function(any):boolean} [options.isError]
 *        Predicate classifying a thrown error as a failure. Defaults to true.
 * @param {function(Error)} [options.onOpen]
 * @param {function(Error)} [options.onClose]
 * @param {function(Error)} [options.onReject]       Called when a request is rejected by the open circuit.
 * @returns {function(input: RequestInfo, init?: RequestInit): Promise<Response>}
 */
export default function circuitFetch(options = {}) {
  const {
    name = 'default',
    failureThreshold = 5,
    cooldownMs = 30_000,
    isFailure = defaultIsFailure,
    isError = () => true,
    onOpen,
    onClose,
    onReject,
  } = options;

  const opts = { name, failureThreshold, cooldownMs, isFailure, isError, onOpen, onClose, onReject };
  const breaker = getBreaker(name, opts);

  const wrapped = async (input, init) => {
    const now = Date.now();

    // OPEN: reject fast until cooldown elapses.
    if (breaker.state === 'OPEN') {
      const remaining = breaker.openedAt + cooldownMs - now;
      if (remaining > 0) {
        const err = new CircuitOpenError(name, remaining);
        if (onReject) onReject(err);
        throw err;
      }
      // Cooldown elapsed -> transition to HALF_OPEN and allow one trial.
      breaker.state = 'HALF_OPEN';
      breaker.halfOpenInFlight = true;
    } else if (breaker.state === 'HALF_OPEN') {
      // Another caller is already probing; reject the rest.
      const err = new CircuitOpenError(name, cooldownMs);
        if (onReject) onReject(err);
        throw err;
    }

    try {
      const res = await fetch(input, init);

      if (isFailure(res)) {
        recordFailure(breaker);
        return res; // still hand the response back; breaker state is updated.
      }

      recordSuccess(breaker);
      return res;
    } catch (err) {
      if (isError(err)) recordFailure(breaker);
      throw err;
    }
  };

  // Expose introspection + manual controls (handy for tests/health endpoints).
  wrapped.breaker = breaker;
  wrapped.reset = () => {
    breaker.state = 'CLOSED';
    breaker.failures = 0;
    breaker.openedAt = 0;
    breaker.halfOpenInFlight = false;
  };
  wrapped.state = () => breaker.state;
  return wrapped;
}

function recordFailure(b) {
  if (b.state === 'HALF_OPEN') {
    // Trial failed -> reopen.
    b.state = 'OPEN';
    b.openedAt = Date.now();
    b.halfOpenInFlight = false;
    if (b.opts.onOpen) b.opts.onOpen(new Error(`half-open trial failed for ${b.name}`));
    return;
  }
  b.failures += 1;
  if (b.failures >= b.opts.failureThreshold) {
    b.state = 'OPEN';
    b.openedAt = Date.now();
    if (b.opts.onOpen) b.opts.onOpen(new Error(`circuit ${b.name} opened after ${b.failures} failures`));
  }
}

function recordSuccess(b) {
  if (b.state === 'HALF_OPEN') {
    b.state = 'CLOSED';
    b.failures = 0;
    b.halfOpenInFlight = false;
    if (b.opts.onClose) b.opts.onClose(new Error(`circuit ${b.name} closed after successful trial`));
    return;
  }
  b.failures = 0;
}

export { CircuitOpenError, circuitFetch };

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
