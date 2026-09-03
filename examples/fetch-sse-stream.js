/**
 * fetch-sse-stream.js
 *
 * Read a Server-Sent Events (SSE) stream with the fetch API only — no EventSource,
 * no build step, no external dependencies. Works in Node 18+, modern browsers, and
 * Cloudflare/Vercel edge runtimes that expose global fetch.
 *
 * Usage:
 *   const stop = subscribeSSE('https://example.com/events', {
 *     onEvent: (evt) => console.log(evt.event, evt.data, evt.id),
 *     onError: (err) => console.error(err),
 *     headers: { Authorization: 'Bearer ...' },
 *     retryDelayMs: 3000,
 *   });
 *   // later: stop() to close the stream
 *
 * Each Server-Sent Event is delivered as an object:
 *   { event: 'message', data: '...', id: '...', retry: 3000 }
 *
 * Multi-line `data:` fields are joined with "\n" per the SSE spec. Comments
 * (lines starting with ':') and unknown fields are preserved on the object
 * under evt._raw for debugging.
 */

export function subscribeSSE(url, options = {}) {
  const {
    onEvent = () => {},
    onError = () => {},
    onOpen = () => {},
    headers = {},
    signal: externalSignal,
    retryDelayMs = 3000,
    fetchImpl = (typeof fetch !== 'undefined' ? fetch : null),
  } = options;

  if (!fetchImpl) throw new Error('No fetch implementation available in this runtime');

  const ac = new AbortController();
  let stopped = false;
  let reconnectTimer = null;

  if (externalSignal) {
    if (externalSignal.aborted) ac.abort();
    else externalSignal.addEventListener('abort', () => ac.abort(), { once: true });
  }

  ac.addEventListener('abort', () => { stopped = true; });

  (async () => {
    while (!stopped) {
      let res;
      try {
        res = await fetchImpl(url, {
          method: 'GET',
          headers: {
            Accept: 'text/event-stream',
            'Cache-Control': 'no-cache',
            ...headers,
          },
          signal: ac.signal,
        });
      } catch (err) {
        if (stopped) return;
        onError(err);
        await sleep(retryDelayMs);
        continue;
      }

      if (!res.ok) {
        const err = new Error(`SSE HTTP ${res.status} ${res.statusText}`);
        err.status = res.status;
        onError(err);
        await sleep(retryDelayMs);
        continue;
      }

      if (!res.body || typeof res.body.getReader !== 'function') {
        const err = new Error('SSE response has no readable body (getReader missing)');
        onError(err);
        return;
      }

      onOpen(res);

      const reader = res.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let buf = '';

      try {
        while (!stopped) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += decoder.decode(value, { stream: true });

          // SSE separates events with a blank line.
          let idx;
          while ((idx = buf.indexOf('\n\n')) !== -1) {
            const rawEvent = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            const evt = parseSSEBlock(rawEvent);
            if (evt) onEvent(evt);
          }
        }
      } catch (err) {
        if (!stopped) onError(err);
      }

      if (!stopped) await sleep(retryDelayMs);
    }
  })();

  return () => {
    stopped = true;
    ac.abort();
    if (reconnectTimer) clearTimeout(reconnectTimer);
  };
}

function parseSSEBlock(block) {
  // An event may consist of multiple lines, separated by single \n or \r\n.
  const lines = block.split(/\r?\n/);
  const evt = { event: 'message', data: '', id: null, retry: null, _raw: [] };
  let dataLines = [];
  let sawField = false;

  for (const line of lines) {
    if (line === '') continue;
    if (line.startsWith(':')) {
      evt._raw.push(line);
      continue; // comment
    }
    const colon = line.indexOf(':');
    let field, value;
    if (colon === -1) {
      field = line;
      value = '';
    } else {
      field = line.slice(0, colon);
      value = line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);
    }
    sawField = true;
    evt._raw.push(line);

    switch (field) {
      case 'event':
        evt.event = value;
        break;
      case 'data':
        dataLines.push(value);
        break;
      case 'id':
        evt.id = value;
        break;
      case 'retry':
        const n = Number(value);
        if (Number.isFinite(n) && n >= 0) evt.retry = n;
        break;
      default:
        // Unknown field — keep on _raw only.
        break;
    }
  }

  if (!sawField) return null;
  evt.data = dataLines.join('\n');
  return evt;
}

function sleep(ms) {
  return new Promise((r) => { setTimeout(r, ms); });
}

/* ----------------------------- demo runner ----------------------------- */
// Self-test/demo when run directly: `node examples/fetch-sse-stream.js`
// (Skips if no demo URL provided via DEMO_SSE_URL env var.)
if (typeof process !== 'undefined' && process.env && process.env.DEMO_SSE_URL && import.meta && import.meta.url === `file://${process.argv[1]}`) {
  let count = 0;
  const stop = subscribeSSE(process.env.DEMO_SSE_URL, {
    onOpen: () => console.log('[sse] connected'),
    onEvent: (e) => {
      console.log('[sse]', e.event, e.id ?? '-', e.data);
      if (++count >= 3) { console.log('[sse] stopping after 3 events'); stop(); }
    },
    onError: (e) => console.error('[sse] error', e.message || e),
  });
  setTimeout(stop, 15000);
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
