// ===========================================================================
// Inventory service for TicketMEI
// ===========================================================================
//
// Keeps the ticket stock of every event (inventory_db, in memory like the
// other services) and lets the Order service reserve tickets before the
// payment and release them again when the payment fails. These two calls are
// the first step and the compensating transaction of the checkout Saga.
//
// Endpoints
// ---------
// POST /inventory/reserve
//     Reserve tickets. Body (JSON):
//       { "event_id": "evt-101", "order_id": "<id>", "quantity": 2 }
//     Responses: 201 (reserved), 200 (this order already reserved the same
//                tickets, nothing changes), 400 (invalid body), 404 (unknown
//                event), 409 (not enough tickets, or the order id was used
//                for another reservation).
// POST /inventory/release
//     Give the tickets of a reservation back (Saga compensation). Body (JSON):
//       { "order_id": "<id>" }
//     Responses: 200 (released, or already released), 400 (invalid body),
//                404 (no reservation for that order).
// GET /inventory/:event_id
//     Return the stock of an event, or 404 if it does not exist.
// POST /inventory/stock
//     Create an event stock or reset an existing one. Body (JSON):
//       { "event_id": "evt-101", "total_tickets": 20 }
//     `available_tickets` becomes `total_tickets` and the reservations of that
//     event are forgotten. Used by the k6 test to prepare an event.
// GET /metrics
//     Return metrics in Prometheus text format.
//
// Metrics
// -------
// inventory_requests_total : Counter
//     HTTP requests received by the Inventory service. Labels: `endpoint`, `http_status`.
// inventory_request_duration_seconds : Histogram
//     Duration of Inventory HTTP requests in seconds. Label: `endpoint`.
// inventory_reservations_total : Counter
//     Results of POST /inventory/reserve. Label: `result`
//     (reserved | replayed | insufficient_stock | unknown_event | conflict).
// inventory_releases_total : Counter
//     Results of POST /inventory/release. Label: `result`
//     (released | replayed | unknown_reservation).
// inventory_available_tickets : Gauge
//     Tickets still available. Label: `event_id`.
//
// Environment Variables
// ---------------------
// PORT : int, optional
//     Port to listen on (default `8005`).
// INITIAL_STOCK : str, optional
//     JSON object `{ "<event_id>": <total_tickets>, ... }` with the stock at
//     startup (default: the events of the Catalog with their `total_tickets`).
//
// ===========================================================================

const express = require("express");
const client = require("prom-client");

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const PORT = Number(process.env.PORT) || 8005;

// Stock at startup: the events of the Catalog (catalog/data/events.json) with
// their `total_tickets`. Can be replaced with the INITIAL_STOCK variable.
const DEFAULT_INITIAL_STOCK = { "evt-101": 60000, "evt-102": 50000, "evt-103": 55000, "evt-104": 20000 };

// Read the stock at startup.
//
// Returns
// -------
// object
//     `{ event_id: total_tickets }` from INITIAL_STOCK, or the default stock.
//
// Raises
// ------
// Error
//     If INITIAL_STOCK is not a JSON object of non-negative integers.
function loadInitialStock() {
  if (!process.env.INITIAL_STOCK)
    return DEFAULT_INITIAL_STOCK;

  const stock = JSON.parse(process.env.INITIAL_STOCK);

  const valid = stock !== null && typeof stock === "object" && !Array.isArray(stock)
    && Object.values(stock).every((total) => Number.isInteger(total) && total >= 0);

  if (!valid)
    throw new Error("INITIAL_STOCK must be a JSON object { event_id: total_tickets }");

  return stock;
}

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

// In-memory stock of the service (inventory_db). Keys are `event_id` (str) and
// values are Stock objects:
//
// event_id : str
//     Event id.
// total_tickets : int
//     Number of tickets to sell.
// available_tickets : int
//     Tickets still available (0 to `total_tickets`).
const stocks = new Map();

// In-memory reservations. Keys are `order_id` (str) and values are
// Reservation objects:
//
// order_id : str
//     Order that reserved the tickets. One reservation per order.
// event_id : str
//     Event of the tickets.
// quantity : int
//     Number of tickets reserved.
// status : {"RESERVED", "RELEASED"}
//     RELEASED once the tickets were given back.
//
// Notes
// -----
// Keeping the reservation per `order_id` makes POST /inventory/reserve and
// POST /inventory/release idempotent, so the Order service can retry them
// without taking or giving back the tickets twice. The data is lost when the
// process restarts.
const reservations = new Map();

for (const [eventId, total] of Object.entries(loadInitialStock()))
  stocks.set(eventId, { event_id: eventId, total_tickets: total, available_tickets: total });

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const requestsTotal = new client.Counter({
  name: "inventory_requests_total",
  help: "Total HTTP requests received by the Inventory service",
  labelNames: ["endpoint", "http_status"],
  registers: [register],
});

const requestDuration = new client.Histogram({
  name: "inventory_request_duration_seconds",
  help: "Duration of Inventory HTTP requests in seconds",
  labelNames: ["endpoint"],
  buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
  registers: [register],
});

const reservationsTotal = new client.Counter({
  name: "inventory_reservations_total",
  help: "Results of POST /inventory/reserve",
  labelNames: ["result"],
  registers: [register],
});

const releasesTotal = new client.Counter({
  name: "inventory_releases_total",
  help: "Results of POST /inventory/release",
  labelNames: ["result"],
  registers: [register],
});

// The gauge is filled on every scrape from the current stock.
new client.Gauge({
  name: "inventory_available_tickets",
  help: "Tickets still available, by event",
  labelNames: ["event_id"],
  registers: [register],
  collect() {
    this.reset();

    for (const stock of stocks.values())
      this.set({ event_id: stock.event_id }, stock.available_tickets);
  },
});

// Show every result label with 0 from the start, so dashboards do not show
// "no data" before the first request.
for (const result of ["reserved", "replayed", "insufficient_stock", "unknown_event", "conflict"])
  reservationsTotal.inc({ result }, 0);

for (const result of ["released", "replayed", "unknown_reservation"])
  releasesTotal.inc({ result }, 0);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const isText = (value) => typeof value === "string" && value.trim() !== "";

// Validate the body of POST /inventory/reserve.
//
// Parameters
// ----------
// body : object
//     Request body with `event_id`, `order_id` and `quantity`.
//
// Returns
// -------
// string[]
//     Validation error messages; empty when the body is valid.
function validateReserve(body) {
  const errors = [];

  if (!isText(body.event_id)) errors.push("event_id is required");
  if (!isText(body.order_id)) errors.push("order_id is required");
  if (!Number.isInteger(body.quantity) || body.quantity < 1) errors.push("quantity must be an integer greater than 0");

  return errors;
}

// Validate the body of POST /inventory/stock.
//
// Parameters
// ----------
// body : object
//     Request body with `event_id` and `total_tickets`.
//
// Returns
// -------
// string[]
//     Validation error messages; empty when the body is valid.
function validateStock(body) {
  const errors = [];

  if (!isText(body.event_id)) errors.push("event_id is required");
  if (!Number.isInteger(body.total_tickets) || body.total_tickets < 0) errors.push("total_tickets must be an integer greater than or equal to 0");

  return errors;
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
// The `endpoint` label uses the route pattern (e.g. `/inventory/:event_id`)
// instead of the real URL, so each id does not create a new time series.
// Requests that match no route are labelled `unmatched`.
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

// POST /inventory/reserve
//
// Take tickets out of the stock for an order (Saga step 1).
//
// Parameters
// ----------
// event_id : str
//     Event of the tickets (request body).
// order_id : str
//     Order that reserves them (request body).
// quantity : int
//     Number of tickets, at least 1 (request body).
//
// Returns
// -------
// 201 : Reservation + available_tickets
//     Tickets reserved.
// 200 : Reservation + available_tickets
//     The order had already reserved these tickets (idempotency).
// 400 : {"error", "details"}
//     Invalid body.
// 404 : {"error", "code": "UNKNOWN_EVENT"}
//     The event has no stock.
// 409 : {"error", "code", "available_tickets"?}
//     `OUT_OF_STOCK` when there are fewer tickets than requested;
//     `RESERVATION_CONFLICT` when the order id was used for another reservation
//     or its tickets were already released.
//
// Notes
// -----
// The check and the decrement run in the same synchronous block (no `await`
// in between). Node.js runs one handler at a time, so two requests can never
// both take the last tickets: there is no overbooking.
app.post("/inventory/reserve", (req, res) => {
  const body = req.body || {};
  const errors = validateReserve(body);

  if (errors.length > 0)
    return res.status(400).json({ error: "Invalid reservation", details: errors });

  const stock = stocks.get(body.event_id);

  if (!stock) {
    reservationsTotal.inc({ result: "unknown_event" });
    return res.status(404).json({ error: "Event not found in Inventory", code: "UNKNOWN_EVENT" });
  }

  const existing = reservations.get(body.order_id);

  if (existing) {
    const sameReservation = existing.event_id === body.event_id && existing.quantity === body.quantity;

    if (!sameReservation || existing.status === "RELEASED") {
      reservationsTotal.inc({ result: "conflict" });
      return res.status(409).json({ error: "Order id already used for another reservation", code: "RESERVATION_CONFLICT" });
    }

    reservationsTotal.inc({ result: "replayed" });
    return res.status(200).json({ ...existing, available_tickets: stock.available_tickets });
  }

  if (stock.available_tickets < body.quantity) {
    reservationsTotal.inc({ result: "insufficient_stock" });
    return res.status(409).json({ error: "Not enough tickets available", code: "OUT_OF_STOCK", available_tickets: stock.available_tickets });
  }

  stock.available_tickets -= body.quantity;

  const reservation = { order_id: body.order_id, event_id: body.event_id, quantity: body.quantity, status: "RESERVED" };
  reservations.set(body.order_id, reservation);

  reservationsTotal.inc({ result: "reserved" });
  res.status(201).json({ ...reservation, available_tickets: stock.available_tickets });
});

// POST /inventory/release
//
// Give the tickets of a reservation back to the stock (Saga compensation).
//
// Parameters
// ----------
// order_id : str
//     Order whose reservation is released (request body).
//
// Returns
// -------
// 200 : Reservation + available_tickets
//     Tickets released, or already released before (idempotency).
// 400 : {"error", "details"}
//     Invalid body.
// 404 : {"error", "code": "RESERVATION_NOT_FOUND"}
//     That order has no reservation (nothing to undo).
//
// Notes
// -----
// The stock never goes above `total_tickets`.
app.post("/inventory/release", (req, res) => {
  const body = req.body || {};

  if (!isText(body.order_id))
    return res.status(400).json({ error: "Invalid release", details: ["order_id is required"] });

  const reservation = reservations.get(body.order_id);

  if (!reservation) {
    releasesTotal.inc({ result: "unknown_reservation" });
    return res.status(404).json({ error: "Reservation not found", code: "RESERVATION_NOT_FOUND" });
  }

  const stock = stocks.get(reservation.event_id);

  if (reservation.status === "RELEASED") {
    releasesTotal.inc({ result: "replayed" });
    return res.status(200).json({ ...reservation, available_tickets: stock.available_tickets });
  }

  stock.available_tickets = Math.min(stock.total_tickets, stock.available_tickets + reservation.quantity);
  reservation.status = "RELEASED";

  releasesTotal.inc({ result: "released" });
  res.status(200).json({ ...reservation, available_tickets: stock.available_tickets });
});

// GET /inventory/:event_id
//
// Parameters
// ----------
// event_id : str
//     Event id from the URL.
//
// Returns
// -------
// 200 : Stock
//     `event_id`, `total_tickets` and `available_tickets`.
// 404 : {"error": "Event not found in Inventory"}
//     If the event has no stock.
app.get("/inventory/:event_id", (req, res) => {
  const stock = stocks.get(req.params.event_id);

  if (!stock)
    return res.status(404).json({ error: "Event not found in Inventory" });

  res.json(stock);
});

// POST /inventory/stock
//
// Create the stock of an event or reset an existing one.
//
// Parameters
// ----------
// event_id : str
//     Event id (request body).
// total_tickets : int
//     Number of tickets to sell, 0 or more (request body).
//
// Returns
// -------
// 201 : Stock
//     The stock was created.
// 200 : Stock
//     The stock already existed and was reset.
// 400 : {"error", "details"}
//     Invalid body.
//
// Notes
// -----
// `available_tickets` is set to `total_tickets` and the reservations of the
// event are deleted, so a later release of an old order cannot add tickets.
app.post("/inventory/stock", (req, res) => {
  const body = req.body || {};
  const errors = validateStock(body);

  if (errors.length > 0)
    return res.status(400).json({ error: "Invalid stock", details: errors });

  const existed = stocks.has(body.event_id);

  for (const [orderId, reservation] of reservations)
    if (reservation.event_id === body.event_id)
      reservations.delete(orderId);

  const stock = { event_id: body.event_id, total_tickets: body.total_tickets, available_tickets: body.total_tickets };
  stocks.set(body.event_id, stock);

  console.log(`Stock of ${stock.event_id} set to ${stock.total_tickets} tickets`);

  res.status(existed ? 200 : 201).json(stock);
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

// Start the HTTP server on `PORT`.
app.listen(PORT, () => { console.log(`Inventory running on port ${PORT} (${stocks.size} events in stock)`); });
