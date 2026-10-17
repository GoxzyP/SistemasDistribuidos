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

app.get("/events", (req, res) => { res.json(events); });

app.get("/events/:id", (req, res) => {
  const event = events.find((e) => e.id === req.params.id);

  if (!event)
    return res.status(404).json({ error: "Event not found" });

  res.json(event);
});

app.get("/metrics", async (req, res) => { res.set("Content-Type", register.contentType); res.end(await register.metrics()); });

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

app.use((req, res) => { res.status(404).json({ error: "Route not found" }); });

// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => { console.error(err); res.status(500).json({ error: "Internal server error" }); });

// ---------------------------------------------------------------------------
// // Startup
// ---------------------------------------------------------------------------

app.listen(PORT, () => { console.log(`Catalog running on port ${PORT} (${events.length} events loaded)`); });