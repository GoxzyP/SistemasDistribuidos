// ===========================================================================
// API Gateway for TicketMEI
// ===========================================================================
//
// Public entry point of the system. Synchronously forwards client requests
// to the Catalog and Order services and exposes Prometheus metrics for
// monitoring.
//
// Endpoints
// ---------
// GET /events
//     Forward to Catalog `GET /events`.
// GET /events/:id
//     Forward to Catalog `GET /events/:id`. The Catalog response (200 or 404)
//     is returned as is.
// POST /orders
//     Forward to Order `POST /orders` (body: customer_id, customer_email,
//     event_id, quantity). The Order response (202, 400, 404, ...) is returned as is.
//     Rate limited per client IP with Redis: the excess gets HTTP 429 and a
//     `Retry-After` header.
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Metrics
// -------
// gateway_requests_total : Counter
//     Total HTTP requests. Labels: `method`, `endpoint`, `http_status`.
// gateway_request_duration_seconds : Histogram
//     Request duration in seconds. Labels: `endpoint`.
// gateway_rate_limited_total : Counter
//     Requests rejected by the rate limiter (HTTP 429). Labels: `endpoint`.
//
// Environment Variables
// ---------------------
// PORT : str, optional
//     Port to listen on (default `8000`).
// CATALOG_URL : str, optional
//     Base URL of the Catalog service (default `http://catalog:8001`).
// CATALOG_TIMEOUT_MS : str, optional
//     Timeout for Catalog requests in milliseconds (default `2000`).
// ORDER_URL : str, optional
//     Base URL of the Order service (default `http://order:8002`).
// ORDER_TIMEOUT_MS : str, optional
//     Timeout for Order requests in milliseconds (default `5000`).
// REDIS_URL : str, optional
//     Redis connection URL used by the rate limiter (default `redis://redis:6379`).
// RATE_LIMIT_MAX : str, optional
//     Maximum POST /orders requests per client IP in each window (default `5`).
// RATE_LIMIT_WINDOW_MS : str, optional
//     Length of the rate limit window in milliseconds (default `1000`).
// TRUST_PROXY : str, optional
//     Number of proxies in front of the gateway whose `X-Forwarded-For` header
//     is trusted to find the client IP (default: none, the socket address is used).
//
// ===========================================================================

const express = require("express");
const client = require("prom-client");
const { createClient } = require("redis");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 8000;

const CATALOG_URL = process.env.CATALOG_URL || "http://catalog:8001";
const CATALOG_TIMEOUT_MS = Number(process.env.CATALOG_TIMEOUT_MS) || 2000;

const ORDER_URL = process.env.ORDER_URL || "http://order:8002";
const ORDER_TIMEOUT_MS = Number(process.env.ORDER_TIMEOUT_MS) || 5000;

const REDIS_URL = process.env.REDIS_URL || "redis://redis:6379";
const RATE_LIMIT_MAX = Number(process.env.RATE_LIMIT_MAX) || 5;
const RATE_LIMIT_WINDOW_MS = Number(process.env.RATE_LIMIT_WINDOW_MS) || 1000;
const TRUST_PROXY = Number(process.env.TRUST_PROXY) || 0;

// ---------------------------------------------------------------------------
// Prometheus Metrics
// ---------------------------------------------------------------------------
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const requestsTotal = new client.Counter({
  name: "gateway_requests_total",
  help: "Total HTTP requests received by the gateway",
  labelNames: ["method", "endpoint", "http_status"],
  registers: [register],
});

const requestDuration = new client.Histogram({
  name: "gateway_request_duration_seconds",
  help: "Duration of gateway HTTP requests in seconds",
  labelNames: ["endpoint"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
  registers: [register],
});

const rateLimitedTotal = new client.Counter({
  name: "gateway_rate_limited_total",
  help: "Requests rejected by the rate limiter (HTTP 429)",
  labelNames: ["endpoint"],
  registers: [register],
});

// ---------------------------------------------------------------------------
// Rate limiter (Redis)
// ---------------------------------------------------------------------------
// Fixed window per client IP. The counters live in Redis, so the limit is shared
// by every gateway instance. The Lua script runs atomically inside Redis: it
// increments the counter, starts the window on the first request and returns
// the counter together with the time left in the window.
const RATE_LIMIT_SCRIPT = `
  local count = redis.call("INCR", KEYS[1])
  if count == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
  end
  return { count, redis.call("PTTL", KEYS[1]) }
`;

const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("Redis error:", err.message));
redis.connect().catch((err) => console.error("Redis connection failed:", err.message));

// If Redis is down the limiter "fails open": requests are allowed and the
// problem is logged, so a Redis outage does not take the whole gateway down.
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

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------
const app = express();

// Only trust X-Forwarded-For when the gateway really sits behind a proxy;
// otherwise a client could fake its IP and bypass the rate limit.
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

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------
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

app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

app.use((req, res) => {res.status(404).json({ error: "Route not found" });});

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  if (err.status >= 400 && err.status < 500) 
    return res.status(err.status).json({ error: "Invalid request body" });
  
  console.error(err);
  res.status(500).json({ error: "Internal server error" });
});

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------
app.listen(PORT, () => {console.log(`API Gateway running on port ${PORT} (Catalog: ${CATALOG_URL})`);});