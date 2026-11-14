// ===========================================================================
// Order service for TicketMEI
// ===========================================================================
//
// Orchestrator of the checkout Saga. It receives ticket purchase requests and
// runs three sequential steps:
//   1. reserve the tickets in the Inventory service (POST /inventory/reserve);
//   2. charge the customer in the Payment service (POST /payments, a
//      synchronous HTTP call protected by retry with jitter + Circuit Breaker);
//   3. confirm the order (status CONFIRMED) and publish an "OrderCreated"
//      message to the RabbitMQ exchange `orders.exchange` so that the
//      Notification service can send the confirmation e-mail asynchronously.
// If the payment fails after the tickets were reserved, the compensating
// transaction POST /inventory/release gives the tickets back and the order
// ends FAILED. If there are no tickets left, the Saga stops at step 1 and
// there is nothing to compensate.
//
// Endpoints
// ---------
// POST /orders
//     Create and pay an order. Body (JSON):
//       { "customer_id": "c-1", "customer_email": "a@b.pt", "event_id": "evt-101", "quantity": 2 }
//     Responses: 201 (Saga completed, order CONFIRMED), 400 (invalid body),
//                404 (unknown event), 409 (no tickets left, order FAILED),
//                502/504 (Catalog or Inventory down/slow),
//                502/504 (Payment failed/timed out, tickets released, order FAILED),
//                503 (Circuit Breaker OPEN, tickets released, order FAILED).
// GET /orders/stats
//     Counters of the order_db (orders by status, confirmed tickets by event,
//     compensations), used by the k6 test to check the data consistency.
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
// saga_transactions_initiated_total : Counter
//     Checkout Sagas started (one per order that reaches the Inventory step).
// saga_transactions_success_total : Counter
//     Sagas completed (order CONFIRMED).
// saga_transactions_aborted_total : Counter
//     Sagas that ended with the order FAILED (initiated = success + aborted).
// saga_compensations_executed_total : Counter
//     Compensating transactions (POST /inventory/release) run after a step
//     failed. Label: `outcome` (success | failed).
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
// INVENTORY_URL : str, optional
//     Inventory base URL (default `http://inventory:8005`).
// INVENTORY_TIMEOUT_MS : int, optional
//     Timeout of each Inventory request in milliseconds (default `2000`).
// COMPENSATION_MAX_ATTEMPTS : int, optional
//     Attempts to release the tickets when the Saga is compensated; they use
//     the same backoff with jitter as the Payment retries (default `5`).
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

const INVENTORY_URL = process.env.INVENTORY_URL || "http://inventory:8005";
const INVENTORY_TIMEOUT_MS = Number(process.env.INVENTORY_TIMEOUT_MS) || 2000;
const COMPENSATION_MAX_ATTEMPTS = Number(process.env.COMPENSATION_MAX_ATTEMPTS) || 5;

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
//     PENDING while the Saga runs, then CONFIRMED or FAILED.
// payment_id : str, optional
//     Id of the payment, only when CONFIRMED.
// failure_reason : str, optional
//     Why the Saga was aborted, only when FAILED: OUT_OF_STOCK, UNKNOWN_EVENT,
//     INVENTORY_UNAVAILABLE, PAYMENT_FAILED or PAYMENT_UNAVAILABLE.
// compensation : {"DONE", "FAILED", "NOT_NEEDED"}, optional
//     Result of the release of the reserved tickets, only when a compensation
//     was tried. NOT_NEEDED when Inventory had no reservation for the order.
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

const sagaInitiatedTotal = new client.Counter({
  name: "saga_transactions_initiated_total",
  help: "Checkout Sagas started",
  registers: [register],
});

const sagaSuccessTotal = new client.Counter({
  name: "saga_transactions_success_total",
  help: "Checkout Sagas completed successfully (order CONFIRMED)",
  registers: [register],
});

const sagaAbortedTotal = new client.Counter({
  name: "saga_transactions_aborted_total",
  help: "Checkout Sagas aborted (order FAILED)",
  registers: [register],
});

const sagaCompensationsTotal = new client.Counter({
  name: "saga_compensations_executed_total",
  help: "Compensating transactions (release of the reserved tickets), by outcome",
  labelNames: ["outcome"],
  registers: [register],
});

// Show both outcomes with 0 from the start, so dashboards do not show "no data".
sagaCompensationsTotal.inc({ outcome: "success" }, 0);
sagaCompensationsTotal.inc({ outcome: "failed" }, 0);

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
// Inventory client and Saga compensation
// ---------------------------------------------------------------------------

// Error of one call to the Inventory service.
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
//     code : str or null, optional
//         Error code sent by Inventory, e.g. `OUT_OF_STOCK` (default null).
//
// Attributes
// ----------
// kind, status, code : same as the parameters.
class InventoryError extends Error {
  constructor(message, { kind, status = null, code = null }) {
    super(message);
    this.name = "InventoryError";
    this.kind = kind;
    this.status = status;
    this.code = code;
  }
}

// Send a POST request to the Inventory service (one attempt).
//
// Parameters
// ----------
// path : str
//     Path to call, e.g. `/inventory/reserve`.
// body : object
//     Body sent as JSON.
//
// Returns
// -------
// Promise<object>
//     The JSON answer of Inventory (2xx).
//
// Raises
// ------
// InventoryError
//     kind "timeout" when there is no answer within `INVENTORY_TIMEOUT_MS`,
//     kind "network" when Inventory cannot be reached, kind "http" when it
//     answers with an error status (`status` and `code` are filled in).
async function postInventory(path, body) {
  let response;

  try {
    response = await fetch(`${INVENTORY_URL}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(INVENTORY_TIMEOUT_MS),
    });
  }
  catch (err) {
    if (err.name === "TimeoutError")
      throw new InventoryError("Inventory timed out", { kind: "timeout" });

    throw new InventoryError(`Inventory unavailable (${err.message})`, { kind: "network" });
  }

  const data = await response.json().catch(() => ({}));

  if (!response.ok)
    throw new InventoryError(data.error || `Inventory returned HTTP ${response.status}`, { kind: "http", status: response.status, code: data.code || null });

  return data;
}

// Saga step 1: reserve the tickets of an order in Inventory.
//
// Parameters
// ----------
// order : object
//     Order to reserve; uses `order_id`, `event_id` and `quantity`.
//
// Returns
// -------
// Promise<object>
//     The reservation.
//
// Raises
// ------
// InventoryError
//     With `status` 409 and `code` OUT_OF_STOCK when there are not enough
//     tickets, `status` 404 when Inventory does not know the event, or
//     timeout / network / other HTTP errors.
function reserveStock(order) {
  return postInventory("/inventory/reserve", { event_id: order.event_id, order_id: order.order_id, quantity: order.quantity });
}

// Compensating transaction: give the reserved tickets of an order back.
//
// Parameters
// ----------
// order : object
//     Order whose reservation is released; `order.compensation` is set to the
//     result.
//
// Returns
// -------
// Promise<boolean>
//     True when the tickets are released (or there was nothing to release),
//     false when all the attempts failed.
//
// Notes
// -----
// POST /inventory/release is idempotent, so it is safe to try again. Each try
// waits an exponential backoff with jitter, like the Payment retries. Every
// compensation that ran is counted in
// `saga_compensations_executed_total{outcome}`. If Inventory has no
// reservation for the order (404), nothing was reserved: it is not counted.
async function compensateReservation(order) {
  for (let attempt = 1; ; attempt++) {
    try {
      await postInventory("/inventory/release", { order_id: order.order_id });
      sagaCompensationsTotal.inc({ outcome: "success" });
      order.compensation = "DONE";
      return true;
    }
    catch (err) {
      if (err.status === 404) {
        order.compensation = "NOT_NEEDED";
        return true;
      }

      if (attempt >= COMPENSATION_MAX_ATTEMPTS) {
        console.error(`Compensation FAILED for order ${order.order_id} after ${attempt} attempts: ${err.message}. Tickets stay reserved.`);
        sagaCompensationsTotal.inc({ outcome: "failed" });
        order.compensation = "FAILED";
        return false;
      }

      const delay = retryDelay(attempt);
      console.warn(`Release attempt ${attempt} for order ${order.order_id} failed (${err.message}), retrying in ${Math.round(delay)} ms`);
      await sleep(delay);
    }
  }
}

// End a Saga with the order FAILED.
//
// Parameters
// ----------
// order : object
//     Order of the Saga.
// reason : str
//     Value of `order.failure_reason`.
//
// Notes
// -----
// Called after the compensation (if any) has finished, so `status` only
// becomes FAILED once the tickets were given back.
function abortSaga(order, reason) {
  order.status = "FAILED";
  order.failure_reason = reason;

  ordersInitiatedTotal.inc({ status: "FAILED" });
  sagaAbortedTotal.inc();
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
// Create an order and run the checkout Saga (reserve, pay, confirm).
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
//     Saga completed, order CONFIRMED; OrderCreated was published to
//     `orders.exchange`.
// 400 : {"error", "details"}
//     Invalid body.
// 404 | 502 | 504 : {"error"}
//     Event not found, or Catalog unavailable / timed out.
// 409 : {"error": "Not enough tickets available", "order"}
//     Step 1 failed: no tickets left. Nothing to compensate; order FAILED.
// 502 | 504 : {"error", "order"}
//     Inventory unavailable / timed out (step 1), or Payment kept failing /
//     timing out with the circuit CLOSED (step 2). The tickets are released
//     and the order is FAILED.
// 503 : {"error": "Payment system unavailable, try again later"}
//     Fallback: the circuit is OPEN, Payment was not called; the tickets are
//     released and the order is FAILED.
//
// Notes
// -----
// The order is stored as PENDING before the Saga starts. When step 2 fails,
// the compensation (POST /inventory/release) runs BEFORE the answer is sent,
// so the latency of a compensated Saga includes it. If publishing to RabbitMQ
// fails after the payment, the order stays CONFIRMED (the ticket is paid) and
// the error is only logged.
//
// Known limitation: if the Order service gives up on a payment (timeout) that
// Payment finishes later, that charge is not refunded (Payment has no refund
// endpoint); the stock stays consistent, the money does not.
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
    sagaInitiatedTotal.inc();

    // Step 1: reserve the tickets in Inventory.
    try {
        await reserveStock(order);
    }
    catch (err) {
        // No tickets left, or unknown event: nothing was reserved, nothing to compensate.
        if (err.status === 409 && err.code === "OUT_OF_STOCK") {
            abortSaga(order, "OUT_OF_STOCK");
            return res.status(409).json({ error: "Not enough tickets available", order });
        }

        if (err.status === 404) {
            abortSaga(order, "UNKNOWN_EVENT");
            return res.status(404).json({ error: "Event not found in Inventory" });
        }

        // Timeout or error: Inventory may have reserved the tickets without answering, so release them to be safe.
        console.error(`Inventory reserve failed for order ${order.order_id}:`, err.message);

        await compensateReservation(order);
        abortSaga(order, "INVENTORY_UNAVAILABLE");

        const timedOut = err.kind === "timeout";
        return res.status(timedOut ? 504 : 502).json({ error: timedOut ? "Inventory timed out" : "Inventory unavailable", order });
    }

    // Step 2: charge the customer (retry with jitter + Circuit Breaker).
    let payment;

    try {
        payment = await chargeOrder(order);
    }
    catch (err) {
        // Compensating transaction: give the reserved tickets back.
        await compensateReservation(order);

        // Fallback: circuit OPEN, Payment was not called (fail-fast).
        if (err instanceof CircuitOpenError) {
            abortSaga(order, "PAYMENT_UNAVAILABLE");
            breakerFallbacksTotal.inc();
            return res.status(503).json({ error: "Payment system unavailable, try again later" });
        }

        abortSaga(order, "PAYMENT_FAILED");
        console.error(`Payment failed for order ${order.order_id}:`, err.message);

        const timedOut = err.kind === "timeout";
        return res.status(timedOut ? 504 : 502).json({ error: timedOut ? "Payment timed out" : "Payment failed", order });
    }

    // Step 3: confirm the order.
    order.status = "CONFIRMED";
    order.payment_id = payment.id;
    ordersInitiatedTotal.inc({ status: "CONFIRMED" });
    sagaSuccessTotal.inc();

    try {
        await publishOrderCreated({ type: "OrderCreated", order_id: order.order_id, email: body.customer_email });
    }
    catch (err) {
        console.error(`Order ${order.order_id} CONFIRMED but OrderCreated was not published:`, err.message);
    }

    res.status(201).json(order);
});

// GET /orders/stats
//
// Counters of the order_db, to check the data consistency after a load test
// (compare them with `GET /payments/stats` and `GET /inventory/:event_id`).
//
// Returns
// -------
// 200 : object
//     total : int
//         Orders stored.
//     by_status : {"PENDING", "CONFIRMED", "FAILED"} -> int
//         Orders by status.
//     confirmed_tickets_by_event : object
//         `{ event_id: tickets }` of the CONFIRMED orders.
//     confirmed_amount : float
//         Sum of `total_amount` of the CONFIRMED orders, in euros.
//     compensation : {"DONE", "FAILED", "NOT_NEEDED"} -> int
//         Orders by result of the compensation.
//
// Notes
// -----
// Declared before `GET /orders/:id`, otherwise "stats" would be taken as an id.
app.get("/orders/stats", (req, res) => {
  const stats = {
    total: orders.size,
    by_status: { PENDING: 0, CONFIRMED: 0, FAILED: 0 },
    confirmed_tickets_by_event: {},
    confirmed_amount: 0,
    compensation: { DONE: 0, FAILED: 0, NOT_NEEDED: 0 },
  };

  for (const order of orders.values()) {
    stats.by_status[order.status]++;

    if (order.status === "CONFIRMED") {
      stats.confirmed_tickets_by_event[order.event_id] = (stats.confirmed_tickets_by_event[order.event_id] || 0) + order.quantity;
      stats.confirmed_amount += order.total_amount;
    }

    if (order.compensation)
      stats.compensation[order.compensation]++;
  }

  stats.confirmed_amount = Math.round(stats.confirmed_amount * 100) / 100;
  res.json(stats);
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
