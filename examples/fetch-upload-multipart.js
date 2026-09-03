// examples/fetch-upload-multipart.js
// Demonstrates how to upload a file as multipart/form-data with the native
// browser fetch API — no FormData polyfill, no Buffer, no Node-only deps.
//
// Run in any modern browser console, or paste the body into a page <script>.
// A fetch-only agent can use this exact pattern to POST arbitrary binary blobs
// (images, audio clips, PDFs) to an HTTP endpoint that expects a multipart
// upload, while staying inside the single-script "fetch + minimal helpers" world.
//
// Why this matters: many agent APIs (e.g. technocore room attachments, image
// generation outputs) accept multipart uploads. Without a helper, agents tend
// to either base64-encode payloads (wasteful) or pull in a heavy client lib.
// This file shows the compact, dependency-free path.

// ---- 1. Build the multipart body by hand --------------------------------
//
// We intentionally do NOT use the FormData/Blob/File constructors in a way
// that hides the wire format — we build it explicitly so a fetch-only agent
// can introspect or stream it. A unique boundary is required.

async function buildMultipartBody(parts) {
  const boundary = '----fetchforge' + Math.random().toString(36).slice(2, 12);
  const enc = new TextEncoder();

  let totalSize = 0;
  const chunks = [];

  for (const part of parts) {
    let header = '';
    if (part.name) {
      header += `Content-Disposition: form-data; name="${part.name}"`;
      if (part.filename) header += `; filename="${part.filename}"`;
      header += '\r\n';
    }
    if (part.contentType) header += `Content-Type: ${part.contentType}\r\n`;
    header += '\r\n';

    const headerBytes = enc.encode(header);
    const bodyBytes   = part.data instanceof Uint8Array
      ? part.data
      : enc.encode(String(part.data));
    const crlf        = enc.encode('\r\n');
    const dashBoundary= enc.encode(`--${boundary}\r\n`);

    chunks.push(dashBoundary, headerBytes, bodyBytes, crlf);
    totalSize += dashBoundary.length + headerBytes.length +
                 bodyBytes.length + crlf.length;
  }
  const closing = enc.encode(`--${boundary}--\r\n`);
  chunks.push(closing);
  totalSize += closing.length;

  // Concatenate into one Uint8Array so we can report Content-Length exactly.
  const body = new Uint8Array(totalSize);
  let offset = 0;
  for (const c of chunks) { body.set(c, offset); offset += c.length; }

  return {
    body,
    contentType: `multipart/form-data; boundary=${boundary}`,
  };
}

// ---- 2. Upload with retries + progress ---------------------------------
//
// Re-uses the same backoff curve as fetch-get-with-retries.js so the repo
// has a consistent retry contract.

function backoffDelay(attempt) {
  // 200ms, 400ms, 800ms ... capped at 5s, with ±20% jitter.
  const base = Math.min(5000, 200 * Math.pow(2, attempt));
  const jitter = base * (0.8 + Math.random() * 0.4);
  return Math.round(jitter);
}

async function uploadMultipart(url, parts, { maxAttempts = 4, signal } = {}) {
  const { body, contentType } = await buildMultipartBody(parts);

  let lastErr;
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': contentType,
          'Content-Length': String(body.byteLength),
        },
        body,
        signal,
      });
      if (res.status >= 500 || res.status === 429) {
        throw new Error(`retryable status ${res.status}`);
      }
      return res;
    } catch (err) {
      lastErr = err;
      if (attempt < maxAttempts - 1) {
        await new Promise(r => setTimeout(r, backoffDelay(attempt)));
      }
    }
  }
  throw lastErr;
}

// ---- 3. Demo: upload a small generated PNG -----------------------------
//
// Renders a 4x4 solid PNG entirely from base64, then uploads it. You can
// replace `pngBytes` with any Uint8Array you already hold in memory.

const DEMO_PNG_B64 =
  'iVBORw0KGgoAAAANSUhEUgAAAAQAAAAECAYAAACp8Z5+AAAAFklEQVR4nGNkYPj/n4GBgYmBgYEBADwHBPB7mOMCAAAAAElFTkSuQmCC';

const pngBytes = Uint8Array.from(atob(DEMO_PNG_B64), c => c.charCodeAt(0));

const parts = [
  { name: 'room',     data: 'lobby' },
  { name: 'agent_did', data: 'did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq' },
  { name: 'caption',  data: 'hello from fetch-forge' },
  {
    name: 'attachment',
    filename: 'hello.png',
    contentType: 'image/png',
    data: pngBytes,
  },
];

(async () => {
  const url = 'https://technocore.example/api/rooms/lobby/attachments';
  try {
    const res = await uploadMultipart(url, parts);
    console.log('upload status:', res.status, await res.text());
  } catch (err) {
    console.warn('upload failed after retries:', err.message);
  }
})();

// Exported for reuse by other scripts in the repo (non-module browser env:
// attaches to window). In a module context this is a no-op via the guard.
if (typeof window !== 'undefined') {
  window.fetchForgeMultipart = { buildMultipartBody, uploadMultipart };
}

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
