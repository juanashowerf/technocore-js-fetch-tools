/**
 * fetch-post-form-and-multipart.js
 *
 * A fetch-only agent must be able to POST both application/x-www-form-urlencoded
 * (HTML form submits) and multipart/form-data (file uploads) without any build
 * step, third-party library, or FormData polyfill. This example shows both
 * shapes using only WHATWG Fetch + the standard Web platform APIs available in
 * Node 18+, modern browsers, Deno, Bun, and Workers.
 *
 * Run:
 *   node examples/fetch-post-form-and-multipart.js
 *
 * The script talks to httpbin.org which echoes the request back so you can see
 * exactly what the server received. No state is stored, no secrets are
 * transmitted.
 */

const ENDPOINT = 'https://httpbin.org/post';

// -----------------------------------------------------------------------------
// 1. application/x-www-form-urlencoded — the default for classic <form> submits.
//    Body is a single string of key=value pairs joined with '&'. Values MUST be
//    percent-encoded using encodeURIComponent (spaces -> %20, & -> %26, etc.).
// -----------------------------------------------------------------------------
export async function postUrlEncoded(fields, { signal, headers } = {}) {
  const body = Object.entries(fields)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    // Note: do NOT set Content-Type manually with an explicit charset unless you
    // match it in the header below; httpbin echoes whatever we send.
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      Accept: 'application/json',
      ...headers,
    },
    body,
    signal,
  });

  if (!res.ok) {
    throw new Error(`urlencoded POST failed: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// -----------------------------------------------------------------------------
// 2. multipart/form-data — required whenever you upload files, or want to mix
//    text fields and binary blobs in one request. The browser/Node runtime
//    builds the boundary and the part headers for you when you hand it a
//    FormData instance, so you should NOT set Content-Type yourself: setting it
//    would lock in the wrong boundary and the server would fail to parse.
//
//    FormData accepts: strings, Blobs, ArrayBuffers, TypedArrays, and other
//    FormData entries. File-like entries become proper file parts with filename
//    and content-type; strings become plain text parts.
// -----------------------------------------------------------------------------
export async function postMultipart(parts, { signal } = {}) {
  const fd = new FormData();
  for (const part of parts) {
    if (part.kind === 'file') {
      // Wrap the raw bytes in a Blob so the runtime can attach a filename and
      // content-type. A Blob is available in every fetch-capable environment.
      const blob = new Blob([part.bytes], { type: part.type || 'application/octet-stream' });
      fd.append(part.name, blob, part.filename);
    } else {
      // 'kind: "field"' — simple key/value text part.
      fd.append(part.name, String(part.value));
    }
  }

  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      // Intentionally NOT setting Content-Type. The fetch implementation will
      // replace any value we provide with one that includes the generated
      // boundary, so we let it do its job.
    },
    body: fd,
    signal,
  });

  if (!res.ok) {
    throw new Error(`multipart POST failed: ${res.status} ${res.statusText}`);
  }
  return res.json();
}

// -----------------------------------------------------------------------------
// Demo: only runs when this file is executed directly (e.g. `node examples/...`).
// As a module export it stays side-effect free so other snippets can import it.
// -----------------------------------------------------------------------------
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    const ac = new AbortController();
    const timeout = setTimeout(() => ac.abort(new Error('demo timeout')), 15_000);

    try {
      console.log('--- urlencoded ---');
      const urlEcho = await postUrlEncoded(
        { name: 'fetch-forge', role: 'agent', tag: 'hello world & bye' },
        { signal: ac.signal },
      );
      console.log('server saw form:', urlEcho.form);
      console.log('content-type the server parsed:', urlEcho.headers['Content-Type']);

      console.log('\n--- multipart ---');
      const helloBytes = new TextEncoder().encode('Hello from a fetch-only agent!\n');
      const mpEcho = await postMultipart(
        [
          { kind: 'field', name: 'description', value: 'minimal multipart demo' },
          {
            kind: 'file',
            name: 'greeting',
            filename: 'greeting.txt',
            type: 'text/plain',
            bytes: helloBytes,
          },
        ],
        { signal: ac.signal },
      );
      console.log('server saw fields:', Object.keys(mpEcho.form));
      console.log('server saw files:', Object.keys(mpEcho.files));
      console.log('content-type the server parsed:', mpEcho.headers['Content-Type']);
    } catch (err) {
      console.error('demo failed:', err);
      process.exitCode = 1;
    } finally {
      clearTimeout(timeout);
    }
  })();
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
