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
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Metrics
// -------
// gateway_requests_total : Counter
//     Total HTTP requests. Labels: `method`, `endpoint`, `http_status`.
// gateway_request_duration_seconds : Histogram
//     Request duration in seconds. Labels: `endpoint`.
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
//
// ===========================================================================

const express = require("express");
const client = require("prom-client");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 8000;

const CATALOG_URL = process.env.CATALOG_URL || "http://catalog:8001";
const CATALOG_TIMEOUT_MS = Number(process.env.CATALOG_TIMEOUT_MS) || 2000;

const ORDER_URL = process.env.ORDER_URL || "http://order:8002";
const ORDER_TIMEOUT_MS = Number(process.env.ORDER_TIMEOUT_MS) || 5000;

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

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------
const app = express();

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

app.post("/orders", async (req, res) => {
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