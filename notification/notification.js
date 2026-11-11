// ===========================================================================
// Notification service for TicketMEI
// ===========================================================================
//
// Consumes "OrderCreated" messages from RabbitMQ, simulates sending a
// confirmation e-mail (100 ms delay) and records the delivery in a log.
//
// Endpoints
// ---------
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Message consumed (JSON, published by the Order service)
// -------------------------------------------------------
//     { "type": "OrderCreated", "order_id": "...", "email": "a@b.pt", ... }
//
// Data model (one log entry per processed message)
// ------------------------------------------------
//     { order_id, email, status: SENT | FAILED, timestamp }
//
// Metrics
// -------
// notifications_processed_total : Counter
//     Messages processed. Labels: `status` (SENT | FAILED).
// notification_processing_duration_seconds : Histogram
//     Time spent processing each message, in seconds.
//
// Environment Variables
// ---------------------
// PORT : int, optional
//     Port for /metrics (default `8003`).
// RABBITMQ_URL : str, optional
//     AMQP connection URL (default `amqp://guest:guest@rabbitmq:5672`).
// ORDER_QUEUE : str, optional
//     Queue to consume (default `order_created`).
// EMAIL_DELAY_MS : int, optional
//     Simulated e-mail sending time in milliseconds (default `100`).
// PREFETCH : int, optional
//     Messages processed at the same time (default `1`).
// LOG_FILE : str, optional
//     Delivery log path (default `./logs/notification_logs.json`).
//
// ===========================================================================

const fs = require("fs");
const path = require("path");
const express = require("express");
const client = require("prom-client");
const amqp = require("amqplib");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = process.env.PORT || 8003;

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672";
const ORDER_QUEUE = process.env.ORDER_QUEUE || "order_created";
const PREFETCH = Number(process.env.PREFETCH) || 1;
const RABBITMQ_RETRY_MS = 3000;

const EMAIL_DELAY_MS = Number(process.env.EMAIL_DELAY_MS ?? 100);

const LOG_FILE = process.env.LOG_FILE || path.join(__dirname, "logs", "notification_logs.json");

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const notificationsProcessedTotal = new client.Counter({
  name: "notifications_processed_total",
  help: "Notifications processed, by resulting status",
  labelNames: ["status"],
  registers: [register],
});

const notificationDuration = new client.Histogram({
  name: "notification_processing_duration_seconds",
  help: "Time spent processing each notification, in seconds",
  buckets: [0.05, 0.09, 0.1, 0.11, 0.125, 0.15, 0.25, 0.5, 1],
  registers: [register],
});

// ---------------------------------------------------------------------------
// Delivery log
// ---------------------------------------------------------------------------

// Create the log folder at startup if it does not exist.
fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });

// Record one processed message in the console and in `LOG_FILE`.
//
// Parameters
// ----------
// entry : object
//     Log entry: `order_id` (str or null), `email` (str or null),
//     `status` ("SENT" | "FAILED") and `timestamp` (ISO 8601 str).
//
// Notes
// -----
// The file is written asynchronously, one JSON object per line. A write error
// is only printed, it does not stop the message processing.
function logDelivery(entry) {
  console.log(`[notification] ${JSON.stringify(entry)}`);
  fs.appendFile(LOG_FILE, JSON.stringify(entry) + "\n", (err) => { if (err) console.error("Could not write to the log file:", err.message); });
}

// ---------------------------------------------------------------------------
// Message processing
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

// Simulate sending the confirmation e-mail.
//
// Returns
// -------
// Promise<void>
//     Resolves after `EMAIL_DELAY_MS` milliseconds.
//
// Notes
// -----
// No real e-mail is sent; the arguments given by the caller are ignored.
async function sendEmail() { await sleep(EMAIL_DELAY_MS); }

// Process one "OrderCreated" message taken from the queue.
//
// Parameters
// ----------
// channel : amqplib.Channel
//     Channel the message came from, used to acknowledge it.
// msg : amqplib.Message
//     Message whose content is the JSON published by the Order service.
//
// Returns
// -------
// Promise<void>
//     Resolves once the message is logged and acknowledged.
//
// Notes
// -----
// The message is `ack`ed when the e-mail is "sent". When it is invalid (bad
// JSON, no `order_id` or `email`) it is `nack`ed without requeue, so a broken
// message is not delivered again in a loop. Both cases are logged and counted
// in `notifications_processed_total{status}`.
async function handleMessage(channel, msg) {
    const stopTimer = notificationDuration.startTimer();

    let order = null;
    let status = "SENT";

    try {
        order = JSON.parse(msg.content.toString());

        if (!order.order_id || !order.email) 
            throw new Error("message is missing order_id or email");

        await sendEmail(order.email, order);
    } 
    catch (err) {
        console.error(`Notification failed: ${err.message}`);
        status = "FAILED";
    }

    logDelivery({ order_id: order?.order_id ?? null, email: order?.email ?? null, status, timestamp: new Date().toISOString() });

    notificationsProcessedTotal.inc({ status });
    stopTimer();

    status === "SENT" ? channel.ack(msg) : channel.nack(msg, false, false);
}

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------

// Timer of the next reconnection attempt (null when none is scheduled).
let reconnectTimer = null;

// Schedule a new RabbitMQ connection attempt after `RABBITMQ_RETRY_MS`.
//
// Notes
// -----
// Several errors can arrive at the same time (connection and channel). The
// `reconnectTimer` guard makes sure only one attempt is scheduled.
function scheduleReconnect() {
  if (reconnectTimer) 
    return;

  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectRabbit(); }, RABBITMQ_RETRY_MS);
}

// Connect to RabbitMQ and start consuming `ORDER_QUEUE`.
//
// Returns
// -------
// Promise<void>
//     Resolves once consuming has started, or once a retry has been scheduled.
//
// Notes
// -----
// The queue is declared durable (same settings as the Order service) and
// `prefetch(PREFETCH)` limits how many messages are processed at once. Any
// failure, now or later (closed connection), schedules a new attempt.
async function connectRabbit() {
  let connection = null;

  try {
    connection = await amqp.connect(RABBITMQ_URL);
    connection.on("error", (err) => console.error("RabbitMQ error:", err.message));
    connection.on("close", () => { console.error("RabbitMQ connection closed, reconnecting..."); scheduleReconnect(); });

    const channel = await connection.createChannel();
    channel.on("error", (err) => console.error("RabbitMQ channel error:", err.message));
    channel.on("close", () => connection.close().catch(() => {}));

    await channel.assertQueue(ORDER_QUEUE, { durable: true });
    await channel.prefetch(PREFETCH);
    await channel.consume(ORDER_QUEUE, (msg) => { if (msg) handleMessage(channel, msg).catch((err) => console.error(err)); });

    console.log(`Connected to RabbitMQ, consuming "${ORDER_QUEUE}"`);
  } 
  catch (err) {
    console.error(`RabbitMQ unavailable (${err.message}), retrying...`);
    
    if (connection) 
      connection.close().catch(() => {});

    scheduleReconnect();
  }
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

const app = express();

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// GET /metrics
//
// Returns
// -------
// 200 : text/plain
//     All the metrics of this service in Prometheus text format (scraped by
//     Prometheus).
app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

// Start the /metrics server on `PORT` and connect to RabbitMQ.
app.listen(PORT, () => { console.log(`Notification running on port ${PORT} (queue: ${ORDER_QUEUE})`); });
connectRabbit();
