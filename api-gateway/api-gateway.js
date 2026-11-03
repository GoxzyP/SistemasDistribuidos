/**
 * ===========================================================================
 * API Gateway for TicketMEI
 * ===========================================================================
 * 
 * Public entry point of the system. Synchronously forwards client requests
 * to the Catalog and Order services and exposes Prometheus metrics for
 * monitoring.
 *
 * ===========================================================================
*/


/**
 * ---------------------------------------------------------------------------
 * Configuration
 * 
 * Sets environment variables for the API Gateway configuration, including
 * service URLs, timeouts, Redis connection, rate limiting parameters, and trust proxy settings.
 * ---------------------------------------------------------------------------
 */

const express = require("express");
const client = require("prom-client");
const { createClient } = require("redis");

// PORT (str, optional) - Port to listen on (default `8000`)
const PORT = process.env.PORT || 8000;

// CATALOG_URL (str, optional) - Base URL of the Catalog service (default `http://catalog:8001`)
const CATALOG_URL = process.env.CATALOG_URL || "http://catalog:8001";
// CATALOG_TIMEOUT_MS (str, optional) - Timeout for Catalog requests in milliseconds (default `2000`).
const CATALOG_TIMEOUT_MS = Number(process.env.CATALOG_TIMEOUT_MS) || 2000;

// ORDER_URL (str, optional) - Base URL of the Order service (default `http://order:8002`)
const ORDER_URL = process.env.ORDER_URL || "http://order:8002";
// ORDER_TIMEOUT_MS (str, optional) - Timeout for Order requests in milliseconds (default `5000`)
const ORDER_TIMEOUT_MS = Number(process.env.ORDER_TIMEOUT_MS) || 5000;

// REDIS_URL (str, optional) - Redis connection URL used by the rate limiter (default `redis://redis:6379`)
const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
// RATE_LIMIT_MAX (str, optional) - Maximum POST /orders requests per client IP in each window (default `5`)
const RATE_LIMIT_MAX = Number(process.env.RATE_LIMIT_MAX) || 5;
// RATE_LIMIT_WINDOW_MS (str, optional) - Length of the rate limit window in milliseconds (default `1000`)
const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS) || 1000;
// TRUST_PROXY (str, optional) - Number of proxies in front of the gateway whose `X-Forwarded-For` header is trusted to find the client IP (default: none, the socket address is used)
const TRUST_PROXY = Number(process.env.TRUST_PROXY) || 0;


/**
 * --------------------------------------------------------------------------- 
 * Prometheus-Metrics
 * 
 * Initializes the Prometheus metrics registry, collects Node.js default 
 * runtime stats (CPU, memory), and defines custom counters and histograms for monitoring 
 * gateway traffic, latencies, and rate limits.
 * ---------------------------------------------------------------------------
 */

//Create a Prometheus registry and collect default metrics (CPU, memory, etc.)
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// gateway_requests_total : Counter
// Counter metric for total requests received by the gateway, labeled by HTTP method, endpoint, and status code.
const requestsTotal = new client.Counter({
  name: "gateway_requests_total",
  help: "Total HTTP requests received by the gateway",
  labelNames: ["method", "endpoint", "http_status"],
  registers: [register],
});

// gateway_request_duration_seconds : Histogram
//Histogram metric for the time taken to process requests handled by the gateway, labeled by endpoint.
const requestDuration = new client.Histogram({
  name: "gateway_request_duration_seconds",
  help: "Duration of gateway HTTP requests in seconds",
  labelNames: ["endpoint"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
  registers: [register],
});

// gateway_rate_limited_total : Counter
//Counter metric for requests rejected by the rate limiter (HTTP 429), labeled by endpoint.
const rateLimitedTotal = new client.Counter({
  name: "gateway_rate_limited_total",
  help: "Requests rejected by the rate limiter (HTTP 429)",
  labelNames: ["endpoint"],
  registers: [register],
});



/**
 * ---------------------------------------------------------------------------
 * Rate Limiter
 * 
 * Implements a rate limiter for the POST /orders endpoint using Redis. 
 * It tracks the number of requests per client IP within a specified time window, 
 * returning HTTP 429 for excess requests and providing a Retry-After header.
 * ---------------------------------------------------------------------------
 */

// Redis Lua script for atomic increment and expiration in Redis
const RATE_LIMIT_SCRIPT = `
  local count = redis.call("INCR", KEYS[1])
  if count == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
  end
  return { count, redis.call("PTTL", KEYS[1]) }
`;

// Create a Redis client and connect to the Redis server
const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("Redis error:", err.message));
redis.connect().catch((err) => console.error("Redis connection failed:", err.message));


/**
 * Middleware to rate-limit incoming order requests per client IP using Redis.
 * Evaluates a Lua script, sets rate limit headers, increments Prometheus metrics, 
 * and "fails open" (allows the request) if Redis is down.
 * 
 * @async
 * @param {Object} req (Express request object)
 * @param {Object} res (Express response object)
 * @param {Function} next (Express next middleware function)
 */
async function rateLimitOrders(req, res, next) {
  try {
    if (!redis.isReady)
      throw new Error("Redis is not connected");

    const [count, ttlMs] = await redis.eval(RATE_LIMIT_SCRIPT, {
      keys: [`ratelimit:orders:${req.ip}`],
      arguments: [String(RATE_LIMIT_WINDOW_MS)],
    });

    res.set("X-RateLimit-Limit", String(RATE_LIMIT_MAX));
    res.set("X-RateLimit-Remaining", String(Math.max(0, RATE_LIMIT_MAX - count)));

    //If the request count exceeds the maximum allowed: 
    //increment the rate-limited counter, set the Retry-After header, and respond with HTTP 429.
    if (count > RATE_LIMIT_MAX) {
      rateLimitedTotal.inc({ endpoint: "/orders" });
      res.set("Retry-After", String(Math.max(1, Math.ceil(ttlMs / 1000))));
      return res.status(429).json({ error: "Too many requests" });
    }
  }
  catch (error) {
    console.error("Rate limiter unavailable, allowing the request:", error.message);
  }

  next();
}

/**
 * ---------------------------------------------------------------------------
 * Application Logic
 * 
 * Implementation of the Express application that serves as the API Gateway.
 * It forwards requests to the Catalog and Order services, handles errors, 
 * and exposes Prometheus metrics for monitoring.
 * ---------------------------------------------------------------------------
*/

const app = express();

if (TRUST_PROXY > 0)
  app.set("trust proxy", TRUST_PROXY);

app.use((req, res, next) => {
  const stopTimer = requestDuration.startTimer();

  res.on("finish", () => {
    const endpoint = req.route ? req.baseUrl + req.route.path : "unmatched";
    requestsTotal.inc({method: req.method, endpoint, http_status: res.statusCode});
    stopTimer({ endpoint });
  });

  next();
});

app.use(express.json());


/**
 * ---------------------------------------------------------------------------
 * API-Gateway-Endpoints
 * 
 * Defines the API Gateway endpoints that forward requests to the Catalog and Order services.
 * Each endpoint handles errors, timeouts, and returns the appropriate HTTP status and response body.
 * ---------------------------------------------------------------------------
 */

// GET /events 
// gets all events from the Catalog service
app.get("/events", async (req, res) => {
  try {
    const response = await fetch(`${CATALOG_URL}/events`, {signal: AbortSignal.timeout(CATALOG_TIMEOUT_MS)});
    const body = await response.json();
    res.status(response.status).json(body);
  } 
  catch (error) {
    const timedOut = error.name === "TimeoutError";
    console.error("Failed to reach Catalog (/events):", error.message);
    res.status(timedOut ? 504 : 502).json({error: timedOut ? "Catalog timed out" : "Catalog unavailable",});
  }
});

// GET /events/:id 
// gets a specific event from the Catalog service
app.get("/events/:id", async (req, res) => {
  try {
    const response = await fetch(`${CATALOG_URL}/events/${encodeURIComponent(req.params.id)}`, {signal: AbortSignal.timeout(CATALOG_TIMEOUT_MS)});
    const body = await response.json();
    res.status(response.status).json(body);
  } 
  catch (error) {
    const timedOut = error.name === "TimeoutError";
    console.error("Failed to reach Catalog (/events/:id):", error.message);
    res.status(timedOut ? 504 : 502).json({error: timedOut ? "Catalog timed out" : "Catalog unavailable",});
  }
});

// POST /orders 
// creates a new order in the Order service, with rate limiting
app.post("/orders", rateLimitOrders, async (req, res) => {
  try {
    const response = await fetch(`${ORDER_URL}/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(req.body ?? {}),
      signal: AbortSignal.timeout(ORDER_TIMEOUT_MS),
    });

    const body = await response.json();
    res.status(response.status).json(body);
  } 
  catch (error) {
    const timedOut = error.name === "TimeoutError";
    console.error("Failed to reach Order (/orders):", error.message);
    res.status(timedOut ? 504 : 502).json({ error: timedOut ? "Order timed out" : "Order unavailable" });
  }
});

// GET /metrics 
// return Prometheus text format metrics for monitoring 
app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });


/**
 * ---------------------------------------------------------------------------
 * Error Handling
 * 
 * Handles 404 for unmatched routes and 500 for internal server errors.
 * Returns JSON error responses with appropriate HTTP status codes.
 * ---------------------------------------------------------------------------
 */

// 404 for unmatched routes
app.use((req, res) => {res.status(404).json({ error: "Route not found" });});

// 500 for internal server errors
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  if (err.status >= 400 && err.status < 500) 
    return res.status(err.status).json({ error: "Invalid request body" });
  
  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});


/**
 * ---------------------------------------------------------------------------
 * Startup
 * 
 * Starts the Express application, listening on the configured port and logging the Catalog service URL.
 * ---------------------------------------------------------------------------
 */
app.listen(PORT, () => {console.log(`tester: API Gateway running on port ${PORT} (Catalog: ${CATALOG_URL})`);});