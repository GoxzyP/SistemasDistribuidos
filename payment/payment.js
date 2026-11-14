// ===========================================================================
// Payment service for TicketMEI
// ===========================================================================
//
// Simulates an external payment provider. Every charge is kept in the
// service's own in-memory store (payment_db, like the Order service) and the
// service can be told to fail or to slow down on purpose (fault injection),
// so that the Circuit Breaker in the Order service can be tested.
//
// Endpoints
// ---------
// POST /payments
//     Charge an order. Body (JSON): { "order_id": "<id>", "amount": 49.9 }
//     Responses: 201 (charged, status SUCCESS), 200 (order already charged,
//                same payment returned), 400 (invalid body), 409 (order
//                already charged with another amount), 500 (simulated failure,
//                status FAILED).
// POST /payments/configure-faults
//     Change the fault injection. Body (JSON, both fields optional):
//       { "error_rate": 0.5, "latency_ms": 10000 }
//     error_rate is a probability between 0 and 1 (0.5 = 50% of HTTP 500).
//     Send { "error_rate": 0, "latency_ms": 0 } to restore normal behaviour.
// GET /payments/stats
//     Counters of the payment_db (payments by status and the amount charged),
//     used by the k6 Saga test to check the data consistency.
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Metrics
// -------
// payment_requests_total : Counter
//     Responses of POST /payments. Label: `http_status`.
// payment_processing_duration_seconds : Histogram
//     Duration of POST /payments (injected latency included).
//
// Environment Variables
// ---------------------
// PORT : int, optional
//     Port to listen on (default `8004`).
// FAULT_ERROR_RATE : float, optional
//     Probability (0-1) of a simulated HTTP 500 at startup (default `0`).
// FAULT_LATENCY_MS : int, optional
//     Latency added to every payment at startup, in milliseconds (default `0`).
//
// ===========================================================================

const crypto = require("crypto");
const express = require("express");
const client = require("prom-client");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = Number(process.env.PORT) || 8004;

// Highest latency accepted by POST /payments/configure-faults (60 s).
const MAX_LATENCY_MS = 60000;

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

// In-memory store of the service (payment_db), like the Order service.
//
// Keys are `order_id` (str) and values are Payment objects:
//
// id : str
//     Unique payment id (UUID).
// order_id : str
//     Order that was charged. One payment per order.
// amount : float
//     Amount charged, in euros, rounded to 2 decimal places.
// status : {"SUCCESS", "FAILED"}
//     Result of the last attempt.
// processed_at : str
//     ISO 8601 timestamp of the last attempt.
//
// Notes
// -----
// Using `order_id` as the key makes POST /payments idempotent. The data is
// lost when the process restarts.
const payments = new Map();

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

// Current fault injection settings, changed by POST /payments/configure-faults.
//
// error_rate : float
//     Probability (0-1) that a payment fails with HTTP 500.
// latency_ms : int
//     Time each payment waits before being processed, in milliseconds.
//
// Notes
// -----
// Kept in memory, so it only affects the replica that received the
// configure-faults request (the Deployment runs a single replica).
const faults = {
  error_rate: Number(process.env.FAULT_ERROR_RATE) || 0,
  latency_ms: Number(process.env.FAULT_LATENCY_MS) || 0,
};

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const requestsTotal = new client.Counter({
  name: "payment_requests_total",
  help: "Responses of POST /payments, by HTTP status",
  labelNames: ["http_status"],
  registers: [register],
});

const processingDuration = new client.Histogram({
  name: "payment_processing_duration_seconds",
  help: "Duration of POST /payments in seconds (injected latency included)",
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 30],
  registers: [register],
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Wait for some time without blocking the event loop.
//
// Parameters
// ----------
// ms : int
//     Time to wait, in milliseconds.
//
// Returns
// -------
// Promise<void>
//     Resolves after `ms` milliseconds.
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Validate the body of POST /payments.
//
// Parameters
// ----------
// body : object
//     Request body. Expected fields: `order_id` (non-empty str or int) and
//     `amount` (number greater than 0).
//
// Returns
// -------
// string[]
//     Validation error messages; empty when the body is valid.
function validatePaymentRequest(body) {
  const errors = [];

  const isText = (value) => typeof value === "string" && value.trim() !== "";

  if (!isText(body.order_id) && !Number.isInteger(body.order_id))
    errors.push("order_id is required");

  if (typeof body.amount !== "number" || !Number.isFinite(body.amount) || body.amount <= 0)
    errors.push("amount must be a number greater than 0");

  return errors;
}

// Validate the body of POST /payments/configure-faults.
//
// Parameters
// ----------
// body : object
//     Request body. Optional fields: `error_rate` (number between 0 and 1) and
//     `latency_ms` (integer between 0 and `MAX_LATENCY_MS`). At least one of
//     them must be present.
//
// Returns
// -------
// string[]
//     Validation error messages; empty when the body is valid.
function validateFaults(body) {
  const errors = [];

  if (body.error_rate !== undefined && (typeof body.error_rate !== "number" || body.error_rate < 0 || body.error_rate > 1))
    errors.push("error_rate must be a number between 0 and 1");

  if (body.latency_ms !== undefined && (!Number.isInteger(body.latency_ms) || body.latency_ms < 0 || body.latency_ms > MAX_LATENCY_MS))
    errors.push(`latency_ms must be an integer between 0 and ${MAX_LATENCY_MS}`);

  if (body.error_rate === undefined && body.latency_ms === undefined)
    errors.push("send error_rate and/or latency_ms");

  return errors;
}

// Middleware that records the metrics of POST /payments.
//
// Parameters
// ----------
// req : express.Request
//     Incoming request.
// res : express.Response
//     Outgoing response; the metrics are recorded when it finishes.
// next : function
//     Passes control to the payment handler.
//
// Notes
// -----
// Increments `payment_requests_total{http_status}` and observes
// `payment_processing_duration_seconds`. It is applied only to this route, so
// the Prometheus scrapes of /metrics do not affect the numbers.
function trackPayment(req, res, next) {
  const stopTimer = processingDuration.startTimer();

  res.on("finish", () => {
    requestsTotal.inc({ http_status: String(res.statusCode) });
    stopTimer();
  });

  next();
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

const app = express();

app.use(express.json());

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// POST /payments
//
// Charge an order (simulated external payment provider).
//
// Parameters
// ----------
// order_id : str or int
//     Order to charge (request body).
// amount : float
//     Amount in euros, greater than 0 (request body).
//
// Returns
// -------
// 201 : Payment
//     Payment processed, `status` SUCCESS.
// 200 : Payment
//     The order was already charged; the same payment is returned and nothing
//     is charged again (idempotency).
// 400 : {"error", "details"}
//     Invalid body.
// 409 : {"error", "payment"}
//     The order was already charged with a different amount.
// 500 : {"error", "payment"}
//     Simulated failure (fault injection); the payment is stored as FAILED.
//
// Notes
// -----
// The injected latency is applied first. A FAILED payment can be retried with
// the same `order_id` and keeps its `id`; a SUCCESS is never overwritten.
app.post("/payments", trackPayment, async (req, res) => {
  const body = req.body || {};
  const errors = validatePaymentRequest(body);

  if (errors.length > 0)
    return res.status(400).json({ error: "Invalid payment", details: errors });

  const orderId = String(body.order_id);
  const amount = Math.round(body.amount * 100) / 100;

  // Snapshot of the faults at the start of the request.
  const { error_rate, latency_ms } = faults;

  if (latency_ms > 0)
    await sleep(latency_ms);

  // Idempotency: an order that was already charged is never charged twice.
  const existing = payments.get(orderId);

  if (existing && existing.status === "SUCCESS") {
    if (existing.amount !== amount)
      return res.status(409).json({ error: "Order already charged with a different amount", payment: existing });

    return res.status(200).json(existing);
  }

  // Create the payment, or retry one that FAILED before (same id).
  const failed = Math.random() < error_rate;

  const payment = {
    id: existing ? existing.id : crypto.randomUUID(),
    order_id: orderId,
    amount,
    status: failed ? "FAILED" : "SUCCESS",
    processed_at: new Date().toISOString(),
  };

  payments.set(orderId, payment);

  if (failed) {
    console.warn(`Payment for order ${orderId} FAILED (simulated)`);
    return res.status(500).json({ error: "Payment processing failed (simulated)", payment });
  }

  res.status(201).json(payment);
});

// POST /payments/configure-faults
//
// Change the fault injection used to test the Circuit Breaker in Order.
//
// Parameters
// ----------
// error_rate : float, optional
//     Probability (0-1) of HTTP 500, e.g. 0.8 = 80% (request body).
// latency_ms : int, optional
//     Latency added to each payment, in milliseconds (request body).
//
// Returns
// -------
// 200 : {"error_rate", "latency_ms"}
//     The settings now in use.
// 400 : {"error", "details"}
//     Invalid values.
//
// Examples
// --------
// Restore normal behaviour: { "error_rate": 0, "latency_ms": 0 }
app.post("/payments/configure-faults", (req, res) => {
  const body = req.body || {};
  
  const errors = validateFaults(body);

  if (errors.length > 0)
    return res.status(400).json({ error: "Invalid fault configuration", details: errors });

  if (body.error_rate !== undefined) faults.error_rate = body.error_rate;
  if (body.latency_ms !== undefined) faults.latency_ms = body.latency_ms;

  console.log(`Faults updated: error_rate=${faults.error_rate}, latency_ms=${faults.latency_ms}`);

  res.json(faults);
});

// GET /payments/stats
//
// Counters of the payment_db, to check the data consistency after a load test
// (compare them with `GET /orders/stats`).
//
// Returns
// -------
// 200 : object
//     total : int
//         Payments stored (one per order).
//     by_status : {"SUCCESS", "FAILED"} -> int
//         Payments by their current status.
//     success_amount : float
//         Sum of `amount` of the SUCCESS payments, in euros.
app.get("/payments/stats", (req, res) => {
  const stats = { total: payments.size, by_status: { SUCCESS: 0, FAILED: 0 }, success_amount: 0 };

  for (const payment of payments.values()) {
    stats.by_status[payment.status]++;

    if (payment.status === "SUCCESS")
      stats.success_amount += payment.amount;
  }

  stats.success_amount = Math.round(stats.success_amount * 100) / 100;
  res.json(stats);
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
// Error handling
// ---------------------------------------------------------------------------

// Fallback for any route that does not exist.
//
// Returns
// -------
// 404 : {"error": "Route not found"}
app.use((req, res) => { res.status(404).json({ error: "Route not found" }); });

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
app.listen(PORT, () => { console.log(`Payment running on port ${PORT}`); });
