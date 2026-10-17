// ===========================================================================
// Order service for TicketMEI
// ===========================================================================
//
// Receives ticket purchase requests, stores them in the PENDING state and
// publishes an "OrderCreated" message to RabbitMQ so that the Notification
// service can send the confirmation e-mail asynchronously.
//
// Endpoints
// ---------
// POST /orders
//     Create an order. Body (JSON):
//       { "customer_id": "c-1", "customer_email": "a@b.pt", "event_id": "evt-101", "quantity": 2 }
//     Responses: 202 (accepted, order PENDING), 400 (invalid body),
//                404 (unknown event), 502/504 (Catalog down/slow),
//                503 (message broker unavailable).
// GET /orders/:id
//     Return a single order, or 404 if it does not exist.
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Metrics
// -------
// order_requests_total : Counter
//     HTTP requests received by the Order service. Labels: `endpoint`, `http_status`.
// order_request_duration_seconds : Histogram
//     Duration of Order HTTP requests in seconds. Label: `endpoint`.
// orders_initiated_total : Counter
//     Orders handled by POST /orders. Labels: `status` (PENDING | FAILED).
//
// Environment Variables
// ---------------------
// PORT : optional, port to listen on (default 8002).
// CATALOG_URL : optional, Catalog base URL (default http://catalog:8001).
// CATALOG_TIMEOUT_MS : optional, Catalog request timeout (default 2000).
// RABBITMQ_URL : optional, AMQP URL (default amqp://guest:guest@rabbitmq:5672).
// ORDER_QUEUE : optional, queue name (default order_created).
// MAX_TICKETS_PER_ORDER : optional, ticket limit per order (default 4).
//
// ===========================================================================

const crypto = require("crypto");
const express = require("express");
const client = require("prom-client");
const amqp = require("amqplib");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 8002;

const CATALOG_URL = process.env.CATALOG_URL || "http://catalog:8001";
const CATALOG_TIMEOUT_MS = Number(process.env.CATALOG_TIMEOUT_MS) || 2000;

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672";
const ORDER_QUEUE = process.env.ORDER_QUEUE || "order_created";
const RABBITMQ_RETRY_MS = 3000;

const MAX_TICKETS_PER_ORDER = Number(process.env.MAX_TICKETS_PER_ORDER) || 4;

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

const orders = new Map();

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const requestsTotal = new client.Counter({
  name: "order_requests_total",
  help: "Total HTTP requests received by the Order service",
  labelNames: ["endpoint", "http_status"],
  registers: [register],
});

const requestDuration = new client.Histogram({
  name: "order_request_duration_seconds",
  help: "Duration of Order HTTP requests in seconds",
  labelNames: ["endpoint"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
  registers: [register],
});

const ordersInitiatedTotal = new client.Counter({
  name: "orders_initiated_total",
  help: "Orders handled by POST /orders, by resulting status",
  labelNames: ["status"],
  registers: [register],
});

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------

let channel = null;
let reconnectTimer = null;

function scheduleReconnect() {
  channel = null;

  if (reconnectTimer) 
    return;

  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectRabbit(); }, RABBITMQ_RETRY_MS);
}

async function connectRabbit() {
  let connection = null;

  try {
    connection = await amqp.connect(RABBITMQ_URL);
    connection.on("error", (err) => { console.error("RabbitMQ error:", err.message); });
    connection.on("close", () => { console.error("RabbitMQ connection closed, reconnecting..."); scheduleReconnect(); });

    const confirmChannel = await connection.createConfirmChannel();
    confirmChannel.on("error", (err) => { console.error("RabbitMQ channel error:", err.message); });
    confirmChannel.on("close", () => {
      if (channel === confirmChannel)
        channel = null;
      
      connection.close().catch(() => {});
    });

    await confirmChannel.assertQueue(ORDER_QUEUE, { durable: true });

    channel = confirmChannel;
    console.log(`Connected to RabbitMQ (queue: ${ORDER_QUEUE})`);

  } catch (err) {
    console.error(`RabbitMQ unavailable (${err.message}), retrying...`);

    if (connection) 
      connection.close().catch(() => {});

    scheduleReconnect();
  }
}

function publishToQueue(message) {
  return new Promise((resolve, reject) => {
    if (!channel) 
      return reject(new Error("RabbitMQ channel not ready"));
    
    channel.sendToQueue(
        ORDER_QUEUE,
        Buffer.from(JSON.stringify(message)),
        { persistent: true, contentType: "application/json" },
        (err) => (err ? reject(err) : resolve())
    );
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function validateOrderRequest(body) {
    const errors = [];
    const isText = (value) => typeof value === "string" && value.trim() !== "";

    const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    if (!isText(body.customer_id)) { errors.push("customer_id is required"); }
    if (!isText(body.event_id)) { errors.push("event_id is required"); }
    if (!isText(body.customer_email) || !EMAIL_REGEX.test(body.customer_email)) { errors.push("customer_email must be a valid e-mail address"); }

    if (!Number.isInteger(body.quantity) || body.quantity < 1 || body.quantity > MAX_TICKETS_PER_ORDER) 
        errors.push(`quantity must be an integer between 1 and ${MAX_TICKETS_PER_ORDER}`);
    
    return errors;
}

async function fetchEvent(eventId) {
    let response;

    try {
        response = await fetch(`${CATALOG_URL}/events/${encodeURIComponent(eventId)}`, { signal: AbortSignal.timeout(CATALOG_TIMEOUT_MS) });
    } 
    catch (err) {
        const timedOut = err.name === "TimeoutError";

        console.error("Failed to contact Catalog:", err.message);
        throw Object.assign(new Error(timedOut ? "Catalog timed out" : "Catalog unavailable"), { status: timedOut ? 504 : 502 });
    }

    if (response.status === 404) { throw Object.assign(new Error("Event not found"), { status: 404 }); }
    if (!response.ok) { throw Object.assign(new Error("Catalog returned an error"), { status: 502 }); }

    return response.json();
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

const app = express();

app.use((req, res, next) => {
  const stopTimer = requestDuration.startTimer();

  res.on("finish", () => {
    const endpoint = req.route ? req.baseUrl + req.route.path : "unmatched";
    requestsTotal.inc({ endpoint, http_status: res.statusCode });
    stopTimer({ endpoint });
  });

  next();
});

app.use(express.json());

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

app.post("/orders", async (req, res) => {
    const body = req.body || {};

    const errors = validateOrderRequest(body);

    if (errors.length > 0) 
        return res.status(400).json({ error: "Invalid order", details: errors });

    let event;

    try {
        event = await fetchEvent(body.event_id);
    } 
    catch (err) {
        return res.status(err.status).json({ error: err.message });
    }

    const order = {
        order_id: crypto.randomUUID(),
        customer_id: body.customer_id,
        event_id: event.id,
        created_at: new Date().toISOString(),
        total_amount: Math.round(event.price_eur * body.quantity * 100) / 100,
        quantity: body.quantity,
        status: "PENDING"
    };

    try {
        await publishToQueue({ type: "OrderCreated", order_id: order.order_id, email: body.customer_email });
        orders.set(order.order_id, order);
    } 
    catch (err) {
        console.error(`Failed to publish order ${order.order_id}:`, err.message);

        order.status = "FAILED";
        ordersInitiatedTotal.inc({ status: "FAILED" });

        return res.status(503).json({ error: "Message broker unavailable" });
    }

    ordersInitiatedTotal.inc({ status: "PENDING" });
    res.status(202).json(order);
});

app.get("/orders/:id", (req, res) => {
  const order = orders.get(req.params.id);

  if (!order) 
    return res.status(404).json({ error: "Order not found" });
  
  res.json(order);
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

app.listen(PORT, () => { console.log(`Order running on port ${PORT} (Catalog: ${CATALOG_URL})`); });
connectRabbit();
