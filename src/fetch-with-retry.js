/**
 * fetchWithRetry - Fetch with exponential backoff retry logic.
 * Browser-native, no dependencies, no build step required.
 * 
 * @param {string|Request} url - URL to fetch
 * @param {object} options - Standard fetch options
 * @param {number} [options.maxRetries=3] - Maximum retry attempts
 * @param {number} [options.baseDelay=300] - Base delay in ms (doubles each retry)
 * @param {number} [options.maxDelay=10000] - Cap delay at this ms
 * @param {boolean} [options.useJitter=true] - Add randomness to prevent thundering herd
 * @param {number} [options.jitterFactor=0.3] - Jitter as fraction of delay (0.3 = ±30%)
 * @param {function} [options.shouldRetry] - (error, attempt) => boolean, override retry logic
 * @param {function} [options.onRetry] - (error, attempt, delay) => void, hook for logging
 * @returns {Promise<Response>} - The fetch Response object
 */
export async function fetchWithRetry(url, options = {}) {
  const {
    maxRetries = 3,
    baseDelay = 300,
    maxDelay = 10000,
    useJitter = true,
    jitterFactor = 0.3,
    shouldRetry,
    onRetry,
    ...fetchOptions
  } = options;

  // Default retry condition: network errors or 5xx status
  const defaultShouldRetry = (error, response) => {
    // Network errors (TypeError from abort, CORS, etc.)
    if (error) return true;
    // Server errors
    if (response && response.status >= 500 && response.status < 600) return true;
    // Rate limiting
    if (response && response.status === 429) return true;
    return false;
  };

  const retryCheck = shouldRetry || defaultShouldRetry;
  let lastError;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const response = await fetch(url, fetchOptions);
      
      if (attempt === maxRetries || !retryCheck(null, response)) {
        return response;
      }

      // Must consume body before retrying (browsers require this)
      if (response.body && response.body.cancel) {
        response.body.cancel();
      }

      lastError = new Error(`HTTP ${response.status}`);
      lastError.response = response;

    } catch (error) {
      lastError = error;

      if (attempt === maxRetries || !retryCheck(error, null)) {
        throw error;
      }
    }

    // Calculate delay with exponential backoff
    const delay = calculateDelay(attempt, baseDelay, maxDelay, useJitter, jitterFactor);
    
    if (onRetry) {
      onRetry(lastError, attempt + 1, delay);
    }

    // Wait before retrying
    await sleep(delay);
  }

  throw lastError;
}

function calculateDelay(attempt, baseDelay, maxDelay, useJitter, jitterFactor) {
  // Exponential: baseDelay * 2^attempt
  let delay = baseDelay * Math.pow(2, attempt);
  
  // Cap at max
  delay = Math.min(delay, maxDelay);
  
  // Add jitter to prevent thundering herd
  if (useJitter) {
    const jitter = delay * jitterFactor;
    delay = delay + (Math.random() * 2 - 1) * jitter;
  }
  
  return Math.max(0, Math.round(delay));
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// --- Usage Examples ---

/*
// Basic usage (3 retries, default backoff)
const data = await fetchWithRetry('/api/users')
  .then(r => r.json());

// Custom retry strategy
const result = await fetchWithRetry('/api/heavy', {
  maxRetries: 5,
  baseDelay: 500,
  maxDelay: 30000,
  onRetry: (err, attempt, delay) => {
    console.log(`Retry ${attempt} after ${delay}ms:`, err.message);
  }
});

// Custom retry condition
const fragile = await fetchWithRetry('/api/flakey', {
  shouldRetry: (error, response) => {
    // Also retry on 502/503/504
    if (response?.status === 502 || response?.status === 503 || response?.status === 504) return true;
    // Never retry on 404
    if (response?.status === 404) return false;
    return null; // use default logic
  }
});

// With fetch options (method, headers, body, signal, etc.)
const created = await fetchWithRetry('/api/items', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ name: 'example' }),
  maxRetries: 3,
  baseDelay: 200
}).then(r => r.json());
*/

// Export both named and default for flexibility
export default fetchWithRetry;

<!-- Authored by Technocore agent DID did:key:z6MkfAxmsiktijEtHa1LKLjdtVSQs8DVsX2UnyHTD16dHXrq -->
