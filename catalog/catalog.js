/**
 * ===========================================================================
 * Catalog service for TicketMEI
 * ===========================================================================
 * 
 * Serves static event metadata (concerts, venues, base prices) over HTTP
 * and exposes Prometheus metrics for monitoring.
 * ===========================================================================
 */


/**
 * ---------------------------------------------------------------------------
 * Configuration
 * 
 * Sets environment variables for the Catalog service, including the port 
 * to listen on and the path to the events JSON file.
 * ---------------------------------------------------------------------------
 */

const fs = require("fs");
const path = require("path");
const express = require("express");
const client = require("prom-client");

// PORT (str, optional) : Port to listen on (default `8001`).
const PORT = process.env.PORT || 8001;
// EVENTS_FILE (str, optional) : Path to the events JSON file (default `data/events.json`).
const EVENTS_FILE = process.env.EVENTS_FILE || path.join(__dirname, "data", "events.json");

/**
 * ---------------------------------------------------------------------------
 * Data
 * 
 * Loads event data from the specified JSON file into memory for serving over
 * HTTP.
 * --------------------------------------------------------------------------
 */

const events = JSON.parse(fs.readFileSync(EVENTS_FILE, "utf-8"));


/**
 * --------------------------------------------------------------------------- 
 * Prometheus-Metrics
 * 
 * Initializes the Prometheus metrics registry, collects Node.js default 
 * runtime stats (CPU, memory), and defines custom counters and histograms for monitoring 
 * gateway traffic and latencies.
 * ---------------------------------------------------------------------------
 */
const register = new client.Registry();
client.collectDefaultMetrics({ register });

// catalog_requests_total : Counter
// Counter metric to track the total number of HTTP requests received by the Catalog service,
// labeled by endpoint and HTTP status code.
const requestsTotal = new client.Counter({
  name: "catalog_requests_total",
  help: "Total HTTP requests received by the Catalog service",
  labelNames: ["endpoint", "http_status"],
  registers: [register],
});

// catalog_request_duration_seconds : Histogram
// Histogram metric to track the duration of HTTP requests in seconds, labeled by endpoint.
const requestDuration = new client.Histogram({
  name: "catalog_request_duration_seconds",
  help: "Duration of Catalog HTTP requests in seconds",
  labelNames: ["endpoint"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
  registers: [register],
});


/**
 * ---------------------------------------------------------------------------
 * Application Logic
 * 
 * Implementation of the Express application that serves as the Catalog service.
 * It serves static event data, handles errors, and exposes Prometheus metrics 
 * for monitoring.
 * ---------------------------------------------------------------------------
*/

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

/**
 * ---------------------------------------------------------------------------
 * Catalog-Endpoints
 * 
 * Defines the HTTP endpoints for the Catalog service, including routes to
 * retrieve all events, retrieve a single event by ID, and expose Prometheus metrics.
 * ---------------------------------------------------------------------------
 */

// GET /events
// Returns a JSON array of all events.
app.get("/events", (req, res) => { res.json(events); });

// GET /events/:id
// Returns a single event by ID, or a 404 error if the event does not exist.
app.get("/events/:id", (req, res) => {
  const event = events.find((e) => e.id === req.params.id);

  if (!event)
    return res.status(404).json({ error: "Event not found" });

  res.json(event);
});

// GET /metrics
// Returns Prometheus metrics in text format for monitoring.
app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });


/**
 * ---------------------------------------------------------------------------
 * Error Handling
 * 
 * Defines middleware for handling 404 (route not found) and 500 (internal server error) responses.
 * ---------------------------------------------------------------------------
 */

// 404 for unmatched routes
app.use((req, res) => { res.status(404).json({ error: "Route not found" }); });

// 500 for internal server errors
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => { console.error(err); res.status(500).json({ error: "Internal server error" }); });


/**
 * ---------------------------------------------------------------------------
 * Startup
 * 
 * Starts the Express application and listens on the specified port, logging the number of events loaded.
 * ---------------------------------------------------------------------------
 */
app.listen(PORT, () => { console.log(`Catalog running on port ${PORT} (${events.length} events loaded!!)`); });