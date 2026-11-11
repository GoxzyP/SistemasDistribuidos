// ===========================================================================
// Catalog service for TicketMEI
// ===========================================================================
//
// Serves static event metadata (concerts, venues, base prices) over HTTP
// and exposes Prometheus metrics for monitoring.
//
// Endpoints
// ---------
// GET /events
//     Return the list of all events.
// GET /events/:id
//     Return a single event, or 404 if it does not exist.
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Metrics
// -------
// catalog_requests_total : Counter
//     Total HTTP requests. Labels: `endpoint`, `http_status`.
// catalog_request_duration_seconds : Histogram
//     Request duration in seconds. Labels: `endpoint`.
//
// Environment Variables
// ---------------------
// PORT : str, optional
//     Port to listen on (default `8001`).
// EVENTS_FILE : str, optional
//     Path to the events JSON file (default `data/events.json`).
//
// ===========================================================================

const fs = require("fs");
const path = require("path");
const express = require("express");
const client = require("prom-client");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const PORT = process.env.PORT || 8001;
const EVENTS_FILE = process.env.EVENTS_FILE || path.join(__dirname, "data", "events.json");

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------
// List of events, read once at startup from `EVENTS_FILE`.
//
// Each event is an object with at least an `id` (e.g. "evt-101") and a
// `price_eur`, used by the Order service to compute the order total.
//
// Raises
// ------
// Error
//     If the file does not exist or is not valid JSON; the service does not
//     start in that case.
const events = JSON.parse(fs.readFileSync(EVENTS_FILE, "utf-8"));

// ---------------------------------------------------------------------------
// Prometheus Metrics
// ---------------------------------------------------------------------------
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const requestsTotal = new client.Counter({
  name: "catalog_requests_total",
  help: "Total HTTP requests received by the Catalog service",
  labelNames: ["endpoint", "http_status"],
  registers: [register],
});

const requestDuration = new client.Histogram({
  name: "catalog_request_duration_seconds",
  help: "Duration of Catalog HTTP requests in seconds",
  labelNames: ["endpoint"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2],
  registers: [register],
});

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

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

// GET /events
//
// Returns
// -------
// 200 : Event[]
//     All the events loaded from `EVENTS_FILE`.
app.get("/events", (req, res) => { res.json(events); });

// GET /events/:id
//
// Parameters
// ----------
// id : str
//     Event id from the URL (e.g. "evt-101").
//
// Returns
// -------
// 200 : Event
//     The event with that id.
// 404 : {"error": "Event not found"}
//     If no event has that id.
app.get("/events/:id", (req, res) => {
  const event = events.find((e) => e.id === req.params.id);

  if (!event)
    return res.status(404).json({ error: "Event not found" });

  res.json(event);
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
// Error Handling
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
app.use((err, req, res, next) => { console.error(err); res.status(500).json({ error: "Internal server error" }); });

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

// Start the HTTP server on `PORT`.
app.listen(PORT, () => { console.log(`Catalog running on port ${PORT} (${events.length} events loaded!!)`); });