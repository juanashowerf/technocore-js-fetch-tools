// examples/fetch-upload-with-progress.js
// -----------------------------------------------------------------------------
// Tracks upload progress for a fetch() POST using a ReadableStream body.
// Works in modern browsers and Node 18+ (global fetch + TransformStream).
//
// Why: fetch() does not expose upload progress natively. By piping the body
// through a TransformStream we can observe each chunk as it leaves, computing
// bytesSent / totalBytes for a progress bar or ETA. This is essential for a
// fetch-only agent that needs to send large payloads (logs, screenshots, model
// weights) without pulling in axios, got, or any dependency.
//
// Usage:
//   node examples/fetch-upload-with-progress.js <url> <path-to-file>
//
// Notes:
//   - The server must accept the streamed body (chunked transfer encoding).
//   - Content-Length is set from the file size so progress is meaningful.
//   - AbortSignal is wired up: pass --cancel or hit Ctrl-C to stop mid-upload.
// -----------------------------------------------------------------------------

import { createReadStream, statSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const [,, url, filePath] = process.argv;
if (!url || !filePath) {
  console.error('usage: node fetch-upload-with-progress.js <url> <filePath>');
  process.exit(2);
}

const totalBytes = statSync(filePath).size;
const controller = new AbortController();
process.on('SIGINT',  () => controller.abort(new Error('SIGINT')));
process.on('SIGTERM', () => controller.abort(new Error('SIGTERM')));

// Progress accounting ------------------------------------------------------
let bytesSent = 0;
let lastReport = 0;
const startedAt = performance.now();

const ticker = setInterval(() => {
  if (bytesSent >= totalBytes) return;
  const pct   = (bytesSent / totalBytes * 100).toFixed(1);
  const speed = (bytesSent / ((performance.now() - startedAt) / 1000) / 1024).toFixed(1);
  process.stdout.write(`\ruploading... ${pct}%  ${speed} KiB/s  ${bytesSent}/${totalBytes}B`);
  lastReport = bytesSent;
}, 200);

// TransformStream that tees the source bytes through a counter -------------
const { readable, writable } = new TransformStream({
  transform(chunk, controller) {
    bytesSent += chunk.byteLength;
    controller.enqueue(chunk);
  }
});

// Pipe the file into the writable side of the progress stream --------------
const nodeStream = createReadStream(filePath);
nodeStream.on('error', err => controller.abort(err));
nodeStream.on('data',  buf => writable.getWriter().write(buf).catch(() => {}));
nodeStream.on('end',  () => writable.getWriter().close().catch(() => {}));

// Fire the request ---------------------------------------------------------
let res;
try {
  res = await fetch(url, {
    method: 'POST',
    headers: {
      'content-type': 'application/octet-stream',
      'content-length': String(totalBytes),
    },
    body: readable,
    duplex: 'half',                // required when streaming a request body
    signal: controller.signal,
  });
} catch (err) {
  clearInterval(ticker);
  console.error(`\nupload failed: ${err.name}: ${err.message}`);
  process.exit(1);
}

clearInterval(ticker);
const elapsed = ((performance.now() - startedAt) / 1000).toFixed(2);
const avg     = (totalBytes / elapsed / 1024).toFixed(1);
process.stdout.write(`\nupload finished: ${res.status} ${res.statusText}  in ${elapsed}s  (${avg} KiB/s avg)\n`);

// Drain the response so the connection can be reused ------------------------
const body = await res.text();
console.log('response body length:', body.length);

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
