/**
 * ===========================================================================
 * Notification service for TicketMEI
 * ==========================================================================
 * 
 * Consumes "OrderCreated" messages from RabbitMQ, simulates sending a
 * confirmation e-mail (100 ms delay) and records the delivery in a log.
 * 
 * Message consumed (JSON, published by the Order service)
 * -------------------------------------------------------
 * { "type": "OrderCreated", "order_id": "...", "email": "a@b.pt", ... }
 * 
 * Data model (one log entry per processed message)
 * ------------------------------------------------
 *     { order_id, email, status: SENT | FAILED, timestamp }
 * 
 *  ===========================================================================
 */

/**
 * ---------------------------------------------------------------------------
 * Configuration  
 * 
 * Sets environment variables for the Notification service, including the port
 * to listen on, RabbitMQ connection details, and the path to the delivery log.
 * ---------------------------------------------------------------------------
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const client = require("prom-client");
const amqp = require("amqplib");

// PORT : optional, port for /metrics (default 8003).
const PORT = process.env.PORT || 8003;

// RABBITMQ_URL : optional, AMQP URL (default amqp://guest:guest@rabbitmq:5672).
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672";
// ORDER_QUEUE : optional, queue to consume (default order_created).
const ORDER_QUEUE = process.env.ORDER_QUEUE || "order_created";
// PREFETCH : optional, messages processed at the same time (default 1).
const PREFETCH = Number(process.env.PREFETCH) || 1;
// RABBITMQ_RETRY_MS : optional, retry delay for RabbitMQ connections (default 3000).
const RABBITMQ_RETRY_MS = 3000;

// EMAIL_DELAY_MS : optional, simulated e-mail sending time (default 100).
const EMAIL_DELAY_MS = Number(process.env.EMAIL_DELAY_MS ?? 100);
// LOG_FILE : optional, delivery log path (default ./logs/notification_logs.json).
const LOG_FILE = process.env.LOG_FILE || path.join(__dirname, "logs", "notification_logs.json");

/**
 * ---------------------------------------------------------------------------
 * Prometheus metrics
 * 
 * Initializes the Prometheus metrics registry, collects Node.js default
 * runtime stats (CPU, memory), and defines custom counters and histograms for
 * monitoring notification processing.
 * ---------------------------------------------------------------------------
 */

const register = new client.Registry();
client.collectDefaultMetrics({ register });

// notifications_processed_total : Counter
// Counter metric to track the total number of notifications processed,
// labeled by resulting status (SENT or FAILED).
const notificationsProcessedTotal = new client.Counter({
  name: "notifications_processed_total",
  help: "Notifications processed, by resulting status",
  labelNames: ["status"],
  registers: [register],
});

// notification_processing_duration_seconds : Histogram
// Histogram metric to track the duration of notification processing in seconds.
const notificationDuration = new client.Histogram({
  name: "notification_processing_duration_seconds",
  help: "Time spent processing each notification, in seconds",
  buckets: [0.05, 0.09, 0.1, 0.11, 0.125, 0.15, 0.25, 0.5, 1],
  registers: [register],
});

/**
 * ---------------------------------------------------------------------------
 * Delivery log
 * 
 * Creates/Writes to a log file to record the delivery status of each notification, including
 * the order ID, email address, status (SENT or FAILED), and timestamp.
 * ---------------------------------------------------------------------------
 */

fs.mkdirSync(path.dirname(LOG_FILE), { recursive: true });

/**
 * Logs a delivery notification entry to the console and appends it 
 * asynchronously to the persistent log file.
 * 
 * @param {Object} entry - The delivery event data object to record.
 * @param {string} [entry.order_id] - The unique identifier of the order.
 * @param {string} [entry.status] - The current status of the notification/delivery.
 * @returns {void}
 */
function logDelivery(entry) {
  console.log(`[notification] ${JSON.stringify(entry)}`);
  fs.appendFile(LOG_FILE, JSON.stringify(entry) + "\n", (err) => { if (err) console.error("Could not write to the log file:", err.message); });
}

/**
 * ---------------------------------------------------------------------------
 * Message processing
 * 
 * Defines the logic for handling messages consumed from RabbitMQ, 
 * simulating email sending and logging the results.
 * ---------------------------------------------------------------------------
 */

// Simulates sending an email by waiting for a specified delay (EMAIL_DELAY_MS).
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
async function sendEmail() { await sleep(EMAIL_DELAY_MS); }

/**
 * Handles a message consumed from RabbitMQ, simulating email delivery, 
 * recording performance metrics, logging the delivery status, and 
 * acknowledging or rejecting the message on the channel.
 * 
 * @async
 * @param {Object} channel (The RabbitMQ channel instance (from amqplib) used for ack/nack.)
 * @param {Object} msg (The raw RabbitMQ message object consumed from the queue.)
 * @returns {Promise<void>}
 */
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

/**
 * ---------------------------------------------------------------------------
 * RabbitMQ connection
 * 
 * Establishes a connection to RabbitMQ, sets up a channel, asserts the queue,
 * and starts consuming messages. Implements automatic reconnection on failure.
 * ---------------------------------------------------------------------------
 */

let reconnectTimer = null;

/**
 * Schedules a delayed reconnection attempt to RabbitMQ if a reconnection 
 * timer is not already pending. Prevents multiple duplicate timers from stacking up.
 * 
 * @returns {void}
 */
function scheduleReconnect() {
  if (reconnectTimer) 
    return;

  reconnectTimer = setTimeout(() => { reconnectTimer = null; connectRabbit(); }, RABBITMQ_RETRY_MS);
}

/**
 * 
 * Attempts to connect to RabbitMQ, create a channel, assert the queue, and start consuming messages.
 * If the connection fails, it logs the error and schedules a reconnection attempt.
 * 
 * @async
 * @returns {Promise<void>}
 */
async function connectRabbit() {
  let connection = null;

  try {
    connection = await amqp.connect(RABBITMQ_URL);
    connection.on("error", (err) => console.error("RabbitMQ error:", err.message));
    connection.on("close", () => { console.error("RabbitMQ connection closed, reconnecting..."); scheduleReconnect(); });

    //Open a communication channel (a virtual connection inside the TCP connection)
    const channel = await connection.createChannel();
    channel.on("error", (err) => console.error("RabbitMQ channel error:", err.message));
    channel.on("close", () => connection.close().catch(() => {}));

    //Ensure the target queue exists and is durable (survives RabbitMQ restarts)
    await channel.assertQueue(ORDER_QUEUE, { durable: true });
    //Limit how many unacknowledged messages this consumer can take at once (load leveling)
    await channel.prefetch(PREFETCH);
    //Start listening to the queue and processing incoming messages asynchronously
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


/**
 * ---------------------------------------------------------------------------
 * Application Logic
 * 
 * Implements the Express application that serves as the Notification service.  
 * ---------------------------------------------------------------------------
 */
const app = express();

/**
 * ---------------------------------------------------------------------------
 * Endpoints
 * 
 * Defines the HTTP endpoints for the Notification service, including a route to
 * expose Prometheus metrics for monitoring.
 * ---------------------------------------------------------------------------
 */

// GET /metrics
// Returns Prometheus metrics in text format for monitoring.
app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });


/**
 * ---------------------------------------------------------------------------
 * Startup
 * 
 * Starts the Express application and listens on the specified port, logging the queue being consumed.
 * ---------------------------------------------------------------------------
 */
app.listen(PORT, () => { console.log(`Notification running on port ${PORT} (queue: ${ORDER_QUEUE})`); });
connectRabbit();
