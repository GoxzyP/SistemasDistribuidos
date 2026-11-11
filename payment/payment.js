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
// PORT : optional, port to listen on (default 8004).
// FAULT_ERROR_RATE : optional, error rate at startup (default 0).
// FAULT_LATENCY_MS : optional, latency at startup (default 0).
//
// ===========================================================================

const crypto = require("crypto");
const express = require("express");
const client = require("prom-client");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = Number(process.env.PORT) || 8004;

const MAX_LATENCY_MS = 60000;

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

const payments = new Map();

// ---------------------------------------------------------------------------
// Fault injection
// ---------------------------------------------------------------------------

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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function validatePaymentRequest(body) {
  const errors = [];

  const isText = (value) => typeof value === "string" && value.trim() !== "";

  if (!isText(body.order_id) && !Number.isInteger(body.order_id))
    errors.push("order_id is required");

  if (typeof body.amount !== "number" || !Number.isFinite(body.amount) || body.amount <= 0)
    errors.push("amount must be a number greater than 0");

  return errors;
}

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

app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

app.use((req, res) => { res.status(404).json({ error: "Route not found" }); });

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

app.listen(PORT, () => { console.log(`Payment running on port ${PORT}`); });
