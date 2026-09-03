// fetch-with-abort-signal.js
// ----------------------------------------------------------------------------
// Demonstrates how to make a cancellable fetch using AbortController / AbortSignal.
// A fetch-only agent can wire this up so the user (or another agent) can cancel
// an in-flight request, which is critical for long-running fetches, slow peers,
// or hanging streams. No build step, no dependencies; runs in any modern
// browser or Node 18+.
//
// Two patterns are shown:
//   1) Manual cancellation via AbortController.abort().
//   2) Timeout-based cancellation by wiring setTimeout -> controller.abort().
//
// The example installs its UI handlers only when run in a browser; in Node it
// just exercises the timeout pattern on a known endpoint so the script is
// genuinely self-contained and testable.
// ----------------------------------------------------------------------------

(async () => {
  'use strict';

  const log = (...a) => console.log('[fetch-abort]', ...a);

  // ----- Pattern 1: manual cancellation -----------------------------------
  async function fetchWithManualAbort(url) {
    const controller = new AbortController();

    // Kick off the request.
    const promise = fetch(url, { signal: controller.signal });

    // Cancel after 50ms for demo purposes (don't do this in production).
    setTimeout(() => {
      log('aborting manually after 50ms');
      controller.abort();
    }, 50);

    try {
      const res = await promise;
      log('unexpected success:', res.status);
      return res;
    } catch (err) {
      if (err.name === 'AbortError') {
        log('caught AbortError as expected (manual abort)');
      } else {
        log('unexpected error (manual abort path):', err);
      }
    }
  }

  // ----- Pattern 2: timeout-based cancellation -----------------------------
  // Wrap any fetch call with a max-time budget. Useful as a general guard.
  async function fetchWithTimeout(url, opts = {}, timeoutMs = 3000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(new Error('timeout')), timeoutMs);

    // If the caller already passed a signal, link them so either side cancels.
    if (opts.signal) {
      opts.signal.addEventListener('abort', () => controller.abort(opts.signal.reason));
    }

    try {
      const res = await fetch(url, { ...opts, signal: controller.signal });
      return res;
    } finally {
      clearTimeout(timer);
    }
  }

  // ----- Optional: browser UI hookup ---------------------------------------
  // When this file is dropped into a page, expose a button that aborts an
  // in-flight request so a human (or another agent) can cancel mid-flight.
  function installBrowserUi() {
    if (typeof document === 'undefined') return;
    const startBtn = document.getElementById('start-fetch');
    const cancelBtn = document.getElementById('cancel-fetch');
    const out = document.getElementById('out');
    if (!startBtn || !cancelBtn || !out) return;

    let controller = null;
    startBtn.addEventListener('click', async () => {
      controller = new AbortController();
      out.textContent = 'fetching...';
      try {
        const res = await fetch('/slow-endpoint', { signal: controller.signal });
        out.textContent = 'status: ' + res.status;
      } catch (err) {
        out.textContent = err.name === 'AbortError' ? 'aborted' : ('error: ' + err.message);
      }
    });
    cancelBtn.addEventListener('click', () => {
      if (controller) {
        controller.abort();
        out.textContent = 'cancel requested';
      }
    });
  }

  installBrowserUi();

  // ----- Self-test (Node + browser) ---------------------------------------
  // Use httpbin's /delay endpoint, which waits N seconds before responding.
  // We ask for a 2s delay but abort after ~80ms via the timeout helper.
  const url = 'https://httpbin.org/delay/2';

  await fetchWithManualAbort(url);

  try {
    const res = await fetchWithTimeout(url, {}, 80);
    log('timeout path unexpectedly resolved:', res.status);
  } catch (err) {
    if (err.name === 'AbortError') {
      log('caught AbortError as expected (timeout)');
    } else {
      log('unexpected error (timeout path):', err.message);
    }
  }
})();

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
