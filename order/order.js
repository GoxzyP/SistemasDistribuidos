// ===========================================================================
// Order service for TicketMEI
// ===========================================================================
//
// Receives ticket purchase requests, charges them in the Payment service
// (synchronous HTTP call protected by retry with jitter + Circuit Breaker)
// and, when the payment succeeds, publishes an "OrderCreated" message to the
// RabbitMQ exchange `orders.exchange` so that the Notification service can
// send the confirmation e-mail asynchronously.
//
// Endpoints
// ---------
// POST /orders
//     Create and pay an order. Body (JSON):
//       { "customer_id": "c-1", "customer_email": "a@b.pt", "event_id": "evt-101", "quantity": 2 }
//     Responses: 201 (paid, order CONFIRMED), 400 (invalid body),
//                404 (unknown event), 502/504 (Catalog down/slow),
//                502/504 (Payment failed/timed out, order FAILED),
//                503 (Circuit Breaker OPEN, order FAILED).
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
//     Orders handled by POST /orders. Labels: `status` (CONFIRMED | FAILED).
// circuit_breaker_state : Gauge
//     State of the Payment Circuit Breaker: 0 = Closed, 1 = Open, 2 = Half-Open.
// circuit_breaker_fallbacks_total : Counter
//     Requests answered by the fallback because the circuit was open.
// payment_client_requests_total : Counter
//     Real HTTP requests sent to Payment. Label: `status`
//     (2xx | <HTTP code> | timeout | error). Not incremented while OPEN.
//
// Environment Variables
// ---------------------
// PORT : int, optional
//     Port to listen on (default `8002`).
// CATALOG_URL : str, optional
//     Catalog base URL (default `http://catalog:8001`).
// CATALOG_TIMEOUT_MS : int, optional
//     Catalog request timeout in milliseconds (default `2000`).
// PAYMENT_URL : str, optional
//     Payment base URL (default `http://payment:8004`).
// PAYMENT_TIMEOUT_MS : int, optional
//     Timeout of each Payment attempt in milliseconds; this is the latency
//     limit of the Circuit Breaker (default `1000`).
// PAYMENT_MAX_ATTEMPTS : int, optional
//     Attempts per order, 1 = no retry (default `3`).
// PAYMENT_RETRY_BASE_MS : int, optional
//     Base delay of the exponential backoff in milliseconds (default `100`).
// PAYMENT_RETRY_MAX_MS : int, optional
//     Maximum delay between attempts in milliseconds (default `500`).
// CB_WINDOW_SIZE : int, optional
//     Calls kept in the sliding window of the breaker (default `10`).
// CB_MIN_CALLS : int, optional
//     Calls needed before the circuit can open (default `5`).
// CB_FAILURE_RATE : float, optional
//     Failure rate (0-1) that opens the circuit (default `0.5`).
// CB_OPEN_MS : int, optional
//     Time OPEN before trying HALF_OPEN, in milliseconds (default `10000`).
// CB_HALF_OPEN_CALLS : int, optional
//     Trial calls allowed in HALF_OPEN (default `1`).
// RABBITMQ_URL : str, optional
//     AMQP connection URL (default `amqp://guest:guest@rabbitmq:5672`).
// ORDER_EXCHANGE : str, optional
//     Exchange where OrderCreated is published (default `orders.exchange`).
// ORDER_ROUTING_KEY : str, optional
//     Routing key of OrderCreated (default `order.created`).
// ORDER_QUEUE : str, optional
//     Queue bound to the exchange, consumed by Notification (default `order_created`).
// MAX_TICKETS_PER_ORDER : int, optional
//     Ticket limit per order (default `4`).
//
// ===========================================================================

const crypto = require("crypto");
const express = require("express");
const client = require("prom-client");
const amqp = require("amqplib");
const { CircuitBreaker, CircuitOpenError, CIRCUIT_STATE } = require("./circuit-breaker");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 8002;

const CATALOG_URL = process.env.CATALOG_URL || "http://catalog:8001";
const CATALOG_TIMEOUT_MS = Number(process.env.CATALOG_TIMEOUT_MS) || 2000;

const PAYMENT_URL = process.env.PAYMENT_URL || "http://payment:8004";
const PAYMENT_TIMEOUT_MS = Number(process.env.PAYMENT_TIMEOUT_MS) || 1000;
const PAYMENT_MAX_ATTEMPTS = Number(process.env.PAYMENT_MAX_ATTEMPTS) || 3;
const PAYMENT_RETRY_BASE_MS = Number(process.env.PAYMENT_RETRY_BASE_MS) || 100;
const PAYMENT_RETRY_MAX_MS = Number(process.env.PAYMENT_RETRY_MAX_MS) || 500;

const CB_WINDOW_SIZE = Number(process.env.CB_WINDOW_SIZE) || 10;
const CB_MIN_CALLS = Number(process.env.CB_MIN_CALLS) || 5;
const CB_FAILURE_RATE = Number(process.env.CB_FAILURE_RATE) || 0.5;
const CB_OPEN_MS = Number(process.env.CB_OPEN_MS) || 10000;
const CB_HALF_OPEN_CALLS = Number(process.env.CB_HALF_OPEN_CALLS) || 1;

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672";
const ORDER_EXCHANGE = process.env.ORDER_EXCHANGE || "orders.exchange";
const ORDER_ROUTING_KEY = process.env.ORDER_ROUTING_KEY || "order.created";
const ORDER_QUEUE = process.env.ORDER_QUEUE || "order_created";
const RABBITMQ_RETRY_MS = 3000;

const MAX_TICKETS_PER_ORDER = Number(process.env.MAX_TICKETS_PER_ORDER) || 4;

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

// In-memory store of the service (order_db). Keys are `order_id` (str) and
// values are Order objects:
//
// order_id : str
//     Unique order id (UUID).
// customer_id : str
//     Customer that made the purchase.
// event_id : str
//     Event of the tickets.
// quantity : int
//     Number of tickets (1 to `MAX_TICKETS_PER_ORDER`).
// total_amount : float
//     Price of the event times the quantity, in euros.
// status : {"PENDING", "CONFIRMED", "FAILED"}
//     PENDING while the payment runs, then CONFIRMED or FAILED.
// payment_id : str, optional
//     Id of the payment, only when CONFIRMED.
// created_at : str
//     ISO 8601 creation timestamp.
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
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 30],
  registers: [register],
});

const ordersInitiatedTotal = new client.Counter({
  name: "orders_initiated_total",
  help: "Orders handled by POST /orders, by resulting status",
  labelNames: ["status"],
  registers: [register],
});

const breakerStateGauge = new client.Gauge({
  name: "circuit_breaker_state",
  help: "Payment Circuit Breaker state (0 = Closed, 1 = Open, 2 = Half-Open)",
  registers: [register],
});

const breakerFallbacksTotal = new client.Counter({
  name: "circuit_breaker_fallbacks_total",
  help: "Requests answered by the fallback because the circuit was open",
  registers: [register],
});

const paymentClientRequestsTotal = new client.Counter({
  name: "payment_client_requests_total",
  help: "HTTP requests sent to the Payment service, by result",
  labelNames: ["status"],
  registers: [register],
});

// ---------------------------------------------------------------------------
// Payment client: Circuit Breaker + retry with jitter
// ---------------------------------------------------------------------------

// Value of the `circuit_breaker_state` gauge for each breaker state, as defined in the assignment (0 = Closed, 1 = Open, 2 = Half-Open).
const GAUGE_VALUE_BY_STATE = { [CIRCUIT_STATE.CLOSED]: 0, [CIRCUIT_STATE.OPEN]: 1, [CIRCUIT_STATE.HALF_OPEN]: 2 };

// Circuit Breaker that protects every call to the Payment service.
//
// Notes
// -----
// Its limits come from the `CB_*` environment variables. Only errors that show
// Payment is unhealthy (`retryable`: 5xx, timeout, no connection) count as
// failures. Every state change updates the gauge and is logged with a
// timestamp, to measure the CLOSED -> OPEN transition time in the k6 test.
const paymentBreaker = new CircuitBreaker({
  slidingWindowSize: CB_WINDOW_SIZE,
  minimumCallsBeforeOpening: CB_MIN_CALLS,
  failureRateThreshold: CB_FAILURE_RATE,
  openStateDurationMs: CB_OPEN_MS,
  maxTrialCalls: CB_HALF_OPEN_CALLS,

  // Only errors that show Payment is unhealthy (5xx, timeout, connection) count.
  countsAsFailure: (error) => error.retryable === true,
  onStateChange: (previousState, newState) => {
    breakerStateGauge.set(GAUGE_VALUE_BY_STATE[newState]);
    console.warn(`[circuit-breaker] ${previousState} -> ${newState} at ${new Date().toISOString()}`);
  },
});

breakerStateGauge.set(GAUGE_VALUE_BY_STATE[CIRCUIT_STATE.CLOSED]);

// Error of one call to the Payment service.
//
// Parameters
// ----------
// message : str
//     Error description.
// options : object
//     kind : {"timeout", "network", "http"}
//         What went wrong.
//     status : int or null, optional
//         HTTP status when `kind` is "http" (default null).
//     retryable : boolean
//         Whether trying again may work.
//
// Attributes
// ----------
// kind : str
//     Same as the parameter.
// status : int or null
//     Same as the parameter.
// retryable : boolean
//     Used by the retry loop (try again?) and by the breaker (is it a failure?).
class PaymentError extends Error {
  constructor(message, { kind, status = null, retryable }) {
    super(message);
    this.name = "PaymentError";
    this.kind = kind;            
    this.status = status;        
    this.retryable = retryable;
  }
}

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

// Send one HTTP request to Payment `POST /payments` (one attempt).
//
// Parameters
// ----------
// order : object
//     Order to charge; uses `order_id` and `total_amount`.
//
// Returns
// -------
// Promise<object>
//     The Payment created by the Payment service (status SUCCESS).
//
// Raises
// ------
// PaymentError
//     kind "timeout" when there is no answer within `PAYMENT_TIMEOUT_MS`,
//     kind "network" when Payment cannot be reached, kind "http" when it
//     answers with an error status. Only 5xx, timeout and network errors are
//     `retryable`.
//
// Notes
// -----
// This is the only place where `payment_client_requests_total{status}` is
// incremented (2xx, the HTTP code, timeout or error). While the circuit is OPEN
// this function is not called, so the counter does not change.
async function callPayment(order) {
  let response;

  try {
    response = await fetch(`${PAYMENT_URL}/payments`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ order_id: order.order_id, amount: order.total_amount }),
      signal: AbortSignal.timeout(PAYMENT_TIMEOUT_MS),
    });
  }
  catch (err) {
    if (err.name === "TimeoutError") {
      paymentClientRequestsTotal.inc({ status: "timeout" });
      throw new PaymentError("Payment timed out", { kind: "timeout", retryable: true });
    }

    paymentClientRequestsTotal.inc({ status: "error" });
    throw new PaymentError(`Payment unavailable (${err.message})`, { kind: "network", retryable: true });
  }

  if (response.ok) {
    paymentClientRequestsTotal.inc({ status: "2xx" });
    return response.json();
  }

  paymentClientRequestsTotal.inc({ status: String(response.status) });

  throw new PaymentError(`Payment returned HTTP ${response.status}`, {
    kind: "http",
    status: response.status,
    retryable: response.status >= 500,
  });
}

// Wait time before the next attempt: exponential backoff with full jitter.
//
// Parameters
// ----------
// attempt : int
//     Number of the attempt that just failed (1, 2, ...).
//
// Returns
// -------
// float
//     Random delay in milliseconds between 0 and
//     min(PAYMENT_RETRY_MAX_MS, PAYMENT_RETRY_BASE_MS * 2^(attempt - 1)).
//
// Notes
// -----
// The random part (jitter) spreads the retries of many clients in time, so
// they do not all hit Payment again at the same moment.
function retryDelay(attempt) {
  const ceiling = Math.min(PAYMENT_RETRY_MAX_MS, PAYMENT_RETRY_BASE_MS * 2 ** (attempt - 1));
  return Math.random() * ceiling;
}

// Charge an order with retry and jitter, every attempt inside the breaker.
//
// Parameters
// ----------
// order : object
//     Order to charge.
//
// Returns
// -------
// Promise<object>
//     The Payment, when one of the attempts succeeds.
//
// Raises
// ------
// CircuitOpenError
//     The circuit is (or became) OPEN; the retries stop at once and the caller
//     must run the fallback.
// PaymentError
//     Error that is not retryable, or the last error after
//     `PAYMENT_MAX_ATTEMPTS` attempts.
//
// Notes
// -----
// Because every attempt goes through the breaker, failed attempts count
// towards opening it: the circuit opens only after several failures.
async function chargeOrder(order) {
  for (let attempt = 1; ; attempt++) {
    try {
      return await paymentBreaker.execute(() => callPayment(order));
    }
    catch (err) {
      if (err instanceof CircuitOpenError)
        throw err;

      if (!err.retryable || attempt >= PAYMENT_MAX_ATTEMPTS)
        throw err;

      const delay = retryDelay(attempt);
      console.warn(`Payment attempt ${attempt} for order ${order.order_id} failed (${err.message}), retrying in ${Math.round(delay)} ms`);
      await sleep(delay);
    }
  }
}

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------

// Current RabbitMQ confirm channel (null while disconnected) and timer of the next reconnection attempt.
let channel = null;
let reconnectTimer = null;

// Schedule a new RabbitMQ connection attempt after `RABBITMQ_RETRY_MS`.
//
// Notes
// -----
// Several errors can arrive at the same time (connection and channel). The
// `reconnectTimer` guard makes sure only one attempt is scheduled.
//
// The channel is cleared at once so publishOrderCreated fails fast while
// disconnected.
function scheduleReconnect() {
  channel = null;

  if (reconnectTimer)
    return;

  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectRabbit(); }, RABBITMQ_RETRY_MS);
}

// Connect to RabbitMQ and declare `orders.exchange` and its queue.
//
// Returns
// -------
// Promise<void>
//     Resolves once the channel is ready, or once a retry has been scheduled.
//
// Notes
// -----
// Declares the durable `direct` exchange `ORDER_EXCHANGE`, the durable queue
// `ORDER_QUEUE` and the binding between them with `ORDER_ROUTING_KEY`, so the
// Notification service keeps consuming the same queue. A confirm channel is
// used so each publish waits for the broker to accept the message.
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

    // Exchange -> queue: the Notification service keeps consuming the same queue.
    await confirmChannel.assertExchange(ORDER_EXCHANGE, "direct", { durable: true });
    await confirmChannel.assertQueue(ORDER_QUEUE, { durable: true });
    await confirmChannel.bindQueue(ORDER_QUEUE, ORDER_EXCHANGE, ORDER_ROUTING_KEY);

    channel = confirmChannel;
    console.log(`Connected to RabbitMQ (exchange: ${ORDER_EXCHANGE} -> queue: ${ORDER_QUEUE})`);

  } catch (err) {
    console.error(`RabbitMQ unavailable (${err.message}), retrying...`);

    if (connection)
      connection.close().catch(() => {});

    scheduleReconnect();
  }
}

// Publish a message to `orders.exchange` and wait for the broker confirmation.
//
// Parameters
// ----------
// message : object
//     Message body, sent as JSON (e.g. `{ type: "OrderCreated", order_id, email }`).
//
// Returns
// -------
// Promise<void>
//     Resolves when RabbitMQ confirms it stored the message.
//
// Raises
// ------
// Error
//     If there is no channel (RabbitMQ down) or the broker rejects the message.
function publishOrderCreated(message) {
  return new Promise((resolve, reject) => {
    if (!channel)
      return reject(new Error("RabbitMQ channel not ready"));

    channel.publish(
        ORDER_EXCHANGE,
        ORDER_ROUTING_KEY,
        Buffer.from(JSON.stringify(message)),
        { persistent: true, contentType: "application/json" },
        (err) => (err ? reject(err) : resolve())
    );
  });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Validate the body of POST /orders.
//
// Parameters
// ----------
// body : object
//     Request body with `customer_id`, `customer_email`, `event_id` and
//     `quantity`.
//
// Returns
// -------
// string[]
//     Validation error messages; empty when the body is valid.
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

// Get an event from the Catalog service.
//
// Parameters
// ----------
// eventId : str
//     Id of the event (e.g. "evt-101").
//
// Returns
// -------
// Promise<object>
//     The event, with `id` and `price_eur`.
//
// Raises
// ------
// Error
//     With `status` 404 (event not found), 502 (Catalog unavailable or
//     error) or 504 (no answer within `CATALOG_TIMEOUT_MS`).
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
    requestsTotal.inc({ endpoint, http_status: res.statusCode });
    stopTimer({ endpoint });
  });

  next();
});

app.use(express.json());

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// POST /orders
//
// Create an order and pay it through the Circuit Breaker.
//
// Parameters
// ----------
// customer_id : str
//     Customer id (request body).
// customer_email : str
//     E-mail for the confirmation (request body).
// event_id : str
//     Event to buy (request body).
// quantity : int
//     Number of tickets, 1 to `MAX_TICKETS_PER_ORDER` (request body).
//
// Returns
// -------
// 201 : Order
//     Paid, order CONFIRMED; OrderCreated was published to `orders.exchange`.
// 400 : {"error", "details"}
//     Invalid body.
// 404 | 502 | 504 : {"error"}
//     Event not found, or Catalog unavailable / timed out.
// 502 : {"error": "Payment failed", "order"}
//     Payment kept failing with the circuit CLOSED; order FAILED.
// 503 : {"error": "Payment system unavailable, try again later"}
//     Fallback: the circuit is OPEN, Payment was not called; order FAILED.
// 504 : {"error": "Payment timed out", "order"}
//     Payment kept timing out with the circuit CLOSED; order FAILED.
//
// Notes
// -----
// The order is stored as PENDING before the payment. If publishing to
// RabbitMQ fails after the payment, the order stays CONFIRMED (the ticket is
// paid) and the error is only logged.
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

    orders.set(order.order_id, order);

    let payment;

    try {
        payment = await chargeOrder(order);
    }
    catch (err) {
        order.status = "FAILED";
        ordersInitiatedTotal.inc({ status: "FAILED" });

        // Fallback: circuit OPEN, Payment was not called (fail-fast).
        if (err instanceof CircuitOpenError) {
            breakerFallbacksTotal.inc();
            return res.status(503).json({ error: "Payment system unavailable, try again later" });
        }

        console.error(`Payment failed for order ${order.order_id}:`, err.message);

        const timedOut = err.kind === "timeout";
        return res.status(timedOut ? 504 : 502).json({ error: timedOut ? "Payment timed out" : "Payment failed", order });
    }

    order.status = "CONFIRMED";
    order.payment_id = payment.id;
    ordersInitiatedTotal.inc({ status: "CONFIRMED" });

    try {
        await publishOrderCreated({ type: "OrderCreated", order_id: order.order_id, email: body.customer_email });
    }
    catch (err) {
        console.error(`Order ${order.order_id} CONFIRMED but OrderCreated was not published:`, err.message);
    }

    res.status(201).json(order);
});

// GET /orders/:id
//
// Parameters
// ----------
// id : str
//     Order id from the URL.
//
// Returns
// -------
// 200 : Order
//     The order and its current status.
// 404 : {"error": "Order not found"}
//     If no order has that id.
app.get("/orders/:id", (req, res) => {
  const order = orders.get(req.params.id);

  if (!order)
    return res.status(404).json({ error: "Order not found" });

  res.json(order);
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

// Start the HTTP server on `PORT` and connect to RabbitMQ.
app.listen(PORT, () => { console.log(`Order running on port ${PORT} (Catalog: ${CATALOG_URL}, Payment: ${PAYMENT_URL})`); });
connectRabbit();
