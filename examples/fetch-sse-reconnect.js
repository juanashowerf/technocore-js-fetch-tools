// examples/fetch-reconnect-sse.js
// Self-contained example: an SSE (text/event-stream) consumer with
// automatic exponential-backoff reconnection, a single in-flight AbortController
// that is aborted and replaced on every reconnect attempt, an idle/dead-line
// watchdog, and a tiny parser for the SSE wire format (data:, blank-line
// dispatch, multi-line data joined by LF, [DONE] sentinel, id:/event: fields,
// comment lines starting with ":").
//
// Designed so a fetch-only browser agent (no npm, no build step) can consume
// token streams from OpenAI-compatible, Anthropic-compatible, or any SSE
// endpoint without depending on EventSource — which lacks AbortControl,
// custom headers, and POST bodies.
//
// Usage:
//   const conn = connectSSE({
//     url: 'https://api.example.com/v1/stream',
//     headers: { Authorization: 'Bearer ...' },
//     onEvent: (ev) => console.log(ev.event, ev.data, ev.id),
//     onOpen:  (res) => console.log('open', res.status),
//     onError: (err, attempt) => console.warn('err, retry in', attempt),
//     // optional:
//     method: 'POST', body: JSON.stringify({...}),
//     maxBackoffMs: 15000, idleTimeoutMs: 60000,
//   });
//   // later: conn.close();

export function connectSSE({
  url,
  headers = {},
  method = 'GET',
  body = null,
  onOpen = () => {},
  onEvent = () => {},
  onError = () => {},
  maxBackoffMs = 15000,
  idleTimeoutMs = 60000,
} = {}) {
  let attempt = 0;
  let controller = null;
  let reader = null;
  let closed = false;
  let buf = '';
  let watchdog = null;

  const state = { currentEvent: 'message', currentId: null, dataLines: [] };

  function resetParser() {
    buf = '';
    state.currentEvent = 'message';
    state.currentId = null;
    state.dataLines = [];
  }

  function dispatchEvent() {
    if (state.dataLines.length === 0 && state.currentEvent === 'message') return;
    const data = state.dataLines.join('\n');
    const ev = {
      event: state.currentEvent,
      id: state.currentId,
      data,
      isDone: data === '[DONE]',
    };
    onEvent(ev);
    if (ev.isDone) {
      try { controller && controller.abort(); } catch (_) {}
    }
    state.currentEvent = 'message';
    state.currentId = null;
    state.dataLines = [];
  }

  function feed(chunk) {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      let line = buf.slice(0, idx);
      if (line.endsWith('\r')) line = line.slice(0, -1);
      buf = buf.slice(idx + 1);
      if (line === '') {
        dispatchEvent();
        continue;
      }
      if (line.startsWith(':')) continue; // comment / heartbeat
      const colon = line.indexOf(':');
      const field = colon === -1 ? line : line.slice(0, colon);
      let value = colon === -1 ? '' : line.slice(colon + 1);
      if (value.startsWith(' ')) value = value.slice(1);
      switch (field) {
        case 'event':
          state.currentEvent = value;
          break;
        case 'data':
          state.dataLines.push(value);
          break;
        case 'id':
          if (!value.includes('\u0000')) state.currentId = value;
          break;
        case 'retry':
          // server-requested retry interval (ms) — we ignore and use our own backoff.
          break;
      }
    }
  }

  function armWatchdog() {
    clearTimeout(watchdog);
    watchdog = setTimeout(() => {
      try { controller && controller.abort(); } catch (_) {}
    }, idleTimeoutMs);
  }

  async function loop() {
    while (!closed) {
      controller = new AbortController();
      armWatchdog();
      attempt += 1;
      try {
        const res = await fetch(url, {
          method,
          headers,
          body,
          signal: controller.signal,
        });
        if (!res.ok) {
          throw new Error('SSE HTTP ' + res.status + ' ' + res.statusText);
        }
        const ct = res.headers.get('content-type') || '';
        if (!ct.includes('text/event-stream')) {
          throw new Error('SSE bad content-type: ' + ct);
        }
        onOpen(res, attempt);
        reader = res.body.getReader();
        const decoder = new TextDecoder('utf-8');
        resetParser();
        attempt = 0; // success resets backoff
        while (!closed) {
          const { value, done } = await reader.read();
          if (done) break;
          armWatchdog();
          feed(decoder.decode(value, { stream: true }));
        }
      } catch (err) {
        if (closed) break;
        onError(err, attempt);
      } finally {
        clearTimeout(watchdog);
        try { reader && reader.releaseLock(); } catch (_) {}
        reader = null;
      }
      if (closed) break;
      // exponential backoff with full jitter, capped.
      const base = Math.min(maxBackoffMs, 500 * Math.pow(2, attempt - 1));
      const wait = Math.floor(Math.random() * base) + 250;
      await new Promise(r => setTimeout(r, wait));
    }
  }

  const promise = loop();
  promise.catch(() => { /* surfaced via onError */ });

  return {
    close() {
      closed = true;
      clearTimeout(watchdog);
      try { reader && reader.cancel(); } catch (_) {}
      try { controller && controller.abort(); } catch (_) {}
    },
  };
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
