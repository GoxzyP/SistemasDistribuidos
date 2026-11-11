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
//     event_id, quantity). The Order response (201, 400, 404, 503, ...) is returned as is.
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
// PORT : int, optional
//     Port to listen on (default `8000`).
// CATALOG_URL : str, optional
//     Base URL of the Catalog service (default `http://catalog:8001`).
// CATALOG_TIMEOUT_MS : int, optional
//     Timeout for Catalog requests in milliseconds (default `2000`).
// ORDER_URL : str, optional
//     Base URL of the Order service (default `http://order:8002`).
// ORDER_TIMEOUT_MS : int, optional
//     Timeout for Order requests in milliseconds (default `5000`).
// REDIS_URL : str, optional
//     Redis connection URL used by the rate limiter (default `redis://redis:6379`).
// RATE_LIMIT_MAX : int, optional
//     Maximum POST /orders requests per client IP in each window (default `5`).
// RATE_LIMIT_WINDOW_MS : int, optional
//     Length of the rate limit window in milliseconds (default `1000`).
// TRUST_PROXY : int, optional
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
// Lua script of the fixed-window rate limiter, run atomically by Redis.
//
// Parameters
// ----------
// KEYS[1] : str
//     Counter key of one client, `ratelimit:orders:<ip>`.
// ARGV[1] : str
//     Window length in milliseconds.
//
// Returns
// -------
// [int, int]
//     Number of requests of this client in the current window, and the
//     milliseconds left until the window ends.
//
// Notes
// -----
// The first request of a window creates the key and sets its expiry, so the
// counter resets by itself. Running INCR and PEXPIRE in one script keeps them
// atomic even with several gateway replicas.
const RATE_LIMIT_SCRIPT = `
  local count = redis.call("INCR", KEYS[1])
  if count == 1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[1])
  end
  return { count, redis.call("PTTL", KEYS[1]) }
`;

// Redis client used by the rate limiter. It reconnects by itself; errors are
// only printed so the gateway keeps running without Redis.
const redis = createClient({ url: REDIS_URL });
redis.on("error", (err) => console.error("Redis error:", err.message));
redis.connect().catch((err) => console.error("Redis connection failed:", err.message));

// Middleware that limits POST /orders per client IP (RNF3).
//
// Parameters
// ----------
// req : express.Request
//     Incoming request; `req.ip` identifies the client.
// res : express.Response
//     Outgoing response; gets the `X-RateLimit-*` headers.
// next : function
//     Passes control to the forwarding handler when the request is allowed.
//
// Returns
// -------
// 429 : {"error": "Too many requests"}
//     When the client sent more than `RATE_LIMIT_MAX` requests in the current
//     window, with a `Retry-After` header (seconds).
//
// Notes
// -----
// Fail-open: if Redis is down the request is allowed, so a rate limiter
// failure does not take the whole shop offline.
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

// Behind a proxy, trust its `X-Forwarded-For` header so `req.ip` is the real
// client IP instead of the proxy address.
if (TRUST_PROXY > 0)
  app.set("trust proxy", TRUST_PROXY);

// Record the Prometheus metrics of every HTTP request.
//
// Parameters
// ----------
// req : express.Request
//     Incoming request.
// res : express.Response
//     Outgoing response; the metrics are recorded when it finishes.
// next : function
//     Passes control to the next handler.
//
// Notes
// -----
// The `endpoint` label uses the route pattern (e.g. `/events/:id`) instead of
// the real URL, so each id does not create a new time series. Requests that
// match no route are labelled `unmatched`.
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
// GET /events
//
// Forward to Catalog `GET /events`.
//
// Returns
// -------
// 200 : Event[]
//     Response of the Catalog, returned as is.
// 502 : {"error": "Catalog unavailable"}
//     The Catalog could not be reached.
// 504 : {"error": "Catalog timed out"}
//     No answer within `CATALOG_TIMEOUT_MS`.
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
//
// Forward to Catalog `GET /events/:id`.
//
// Parameters
// ----------
// id : str
//     Event id from the URL.
//
// Returns
// -------
// 200 | 404
//     Response of the Catalog, returned as is.
// 502 | 504
//     Catalog unavailable or timed out (as in GET /events).
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
//
// Forward a purchase to Order `POST /orders`, after the rate limiter.
//
// Parameters
// ----------
// body : object
//     `customer_id`, `customer_email`, `event_id` and `quantity`, sent to the
//     Order service unchanged.
//
// Returns
// -------
// 201 | 400 | 404 | 502 | 503 | 504
//     Response of the Order service, returned as is.
// 429 : {"error": "Too many requests"}
//     Rejected by the rate limiter.
// 502 : {"error": "Order unavailable"}
//     The Order service could not be reached.
// 504 : {"error": "Order timed out"}
//     No answer within `ORDER_TIMEOUT_MS`.
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
//
// Returns
// -------
// 200 : text/plain
//     All the metrics of this service in Prometheus text format (scraped by
//     Prometheus).
app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

// Fallback for any route that does not exist.
//
// Returns
// -------
// 404 : {"error": "Route not found"}
app.use((req, res) => {res.status(404).json({ error: "Route not found" });});

// Express error handler (last middleware).
//
// Parameters
// ----------
// err : Error
//     Error thrown by a previous handler. `err.status` is set by Express for
//     client errors, e.g. a body that is not valid JSON.
// req, res, next : express objects
//     `next` is unused but must be declared: Express recognises an error
//     handler by its four parameters.
//
// Returns
// -------
// 4xx : {"error": "Invalid request body"}
//     When `err.status` is a client error.
// 500 : {"error": "Internal server error"}
//     Any other error (also printed to the console).
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
// Start the HTTP server on `PORT`.
app.listen(PORT, () => {console.log(`API Gateway running on port ${PORT} (Catalog: ${CATALOG_URL})`);});