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
// PORT : optional, port for /metrics (default 8003).
// RABBITMQ_URL : optional, AMQP URL (default amqp://guest:guest@rabbitmq:5672).
// ORDER_QUEUE : optional, queue to consume (default order_created).
// EMAIL_DELAY_MS : optional, simulated e-mail sending time (default 100).
// PREFETCH : optional, messages processed at the same time (default 1).
// LOG_FILE : optional, delivery log path (default ./logs/notification_logs.json).
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

fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });

function logDelivery(entry) {
  console.log(`[notification] ${JSON.stringify(entry)}`);
  fs.appendFile(LOG_FILE, JSON.stringify(entry) + "\n", (err) => { if (err) console.error("Could not write to the log file:", err.message); });
}

// ---------------------------------------------------------------------------
// Message processing
// ---------------------------------------------------------------------------

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function sendEmail() { await sleep(EMAIL_DELAY_MS); }

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

let reconnectTimer = null;

function scheduleReconnect() {
  if (reconnectTimer) 
    return;

  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectRabbit(); }, RABBITMQ_RETRY_MS);
}

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

app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

app.listen(PORT, () => { console.log(`Notification running on port ${PORT} (queue: ${ORDER_QUEUE})`); });
connectRabbit();
