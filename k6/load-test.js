// ===========================================================================
// Load test for TicketMEI (k6)
// ===========================================================================
//
// Mixed traffic over every service and endpoint, so that all the Prometheus
// metrics and Grafana dashboards get data. Most requests go through the API
// Gateway; routes the Gateway does not expose are called directly on the
// internal services.
//
// Usage
// -----
// docker compose --profile testing run --rm k6 run load-test.js
//
// Load profile
// ------------
// Ramp up to 500 virtual users in 3 minutes, keep 500 for 2 minutes, then
// ramp down in 30 seconds. Each virtual user picks a weighted random action
// and waits 100 ms between actions.
//
// Thresholds
// ----------
// http_req_duration
//     95% of the requests must finish in less than 200 ms.
// http_req_failed
//     Less than 1% of the requests may fail (unexpected status).
//
// ===========================================================================
import http from 'k6/http';
import { check, sleep } from 'k6';

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------
// Most traffic goes through the API Gateway (the public entry point). Routes
// that the Gateway does not expose are hit directly on the internal services
// (reachable from the ticketmei-net network) so that every microservice and
// every endpoint produces metrics.
const GATEWAY = 'http://api-gateway:8000';
const CATALOG = 'http://catalog:8001';
const ORDER = 'http://order:8002';

// Ids of the events in the Catalog (catalog/data/events.json).
const EVENT_IDS = ['evt-101', 'evt-102', 'evt-103', 'evt-104'];

// Build the k6 parameters of one request.
//
// Parameters
// ----------
// name : str
//     Tag that groups URLs with ids under one name (otherwise k6 would create
//     one metric series per order id).
// expected : int, optional
//     Status that is *supposed* to happen, so intentional 400/404 responses
//     do not count as failures in `http_req_failed`.
// json : boolean, optional
//     Add the `Content-Type: application/json` header.
//
// Returns
// -------
// object
//     Parameters for `http.get` / `http.post`.
function params(name, expected, json) {
  const p = { tags: { name } };
  if (expected) p.responseCallback = http.expectedStatuses(expected);
  if (json) p.headers = { 'Content-Type': 'application/json' };
  return p;
}

// Load profile (stages) and pass/fail criteria (thresholds) of the test.
export const options = {
  stages: [
    { duration: '3m', target: 500 }, // Ramp-up to 500 Virtual Users over 3 minutes
    { duration: '2m', target: 500 }, // Sustain 500 Virtual Users for 2 minutes
    { duration: '30s', target: 0 },  // Scale down
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'], // 95% of requests must complete below 200ms
    http_req_failed: ['rate<0.01'],   // Less than 1% failed requests
  },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
// Pick a random element of a list.
//
// Parameters
// ----------
// list : any[]
//     Non-empty list.
//
// Returns
// -------
// any
//     One of the elements, chosen at random.
const pick = (list) => list[Math.floor(Math.random() * list.length)];

// Build the JSON body of a valid POST /orders.
//
// Returns
// -------
// str
//     Order with the id and e-mail of the current virtual user (`__VU`), a
//     random event and 1 to 4 tickets.
function validOrder() {
  return JSON.stringify({
    customer_id: `c-${__VU}`,
    customer_email: `user${__VU}@example.com`,
    event_id: pick(EVENT_IDS),
    quantity: 1 + Math.floor(Math.random() * 4),
  });
}

// Id of the last order created by this virtual user, used by the
// "GET /orders/:id" action (null until the first order succeeds).
let lastOrderId = null;

// ---------------------------------------------------------------------------
// Scenarios (weights add up to 100)
// ---------------------------------------------------------------------------
// Actions a virtual user can do, each with a weight.
//
// Attributes
// ----------
// weight : int
//     Relative probability of the action (the weights add up to 100).
// run : function
//     Sends the request(s) and checks the expected status.
const actions = [
  // Gateway -> Catalog
  { weight: 35, run: () => {
    const res = http.get(`${GATEWAY}/events`, params('GET /events'));
    check(res, { 'GET /events: 200': (r) => r.status === 200 });
  }},

  // Gateway -> Order -> Catalog + RabbitMQ -> Notification
  { weight: 25, run: () => {
    const res = http.post(`${GATEWAY}/orders`, validOrder(), params('POST /orders', 201, true));
    check(res, { 'POST /orders: 201': (r) => r.status === 201 });
    if (res.status === 201) lastOrderId = res.json('order_id');
  }},

  // Catalog (direct): one event
  { weight: 10, run: () => {
    const res = http.get(`${CATALOG}/events/${pick(EVENT_IDS)}`, params('GET catalog /events/:id'));
    check(res, { 'GET /events/:id: 200': (r) => r.status === 200 });
  }},

  // Order (direct): fetch an existing order
  { weight: 10, run: () => {
    if (!lastOrderId) return;
    const res = http.get(`${ORDER}/orders/${lastOrderId}`, params('GET order /orders/:id'));
    check(res, { 'GET /orders/:id: 200': (r) => r.status === 200 });
  }},

  // Error paths: they exercise the 4xx counters and dashboards
  { weight: 8, run: () => {
    const res = http.post(`${GATEWAY}/orders`, JSON.stringify({ customer_id: 'c-x', quantity: 99 }), params('POST /orders (invalid)', 400, true));
    check(res, { 'POST /orders (invalid): 400': (r) => r.status === 400 });
  }},
  { weight: 5, run: () => {
    const body = JSON.stringify({ customer_id: 'c-x', customer_email: 'x@example.com', event_id: 'evt-999', quantity: 1 });
    const res = http.post(`${GATEWAY}/orders`, body, params('POST /orders (unknown event)', 404, true));
    check(res, { 'POST /orders (unknown event): 404': (r) => r.status === 404 });
  }},
  { weight: 4, run: () => {
    const res = http.get(`${CATALOG}/events/evt-999`, params('GET catalog /events/:id (unknown)', 404));
    check(res, { 'GET /events/:id (unknown): 404': (r) => r.status === 404 });
  }},
  { weight: 3, run: () => {
    const res = http.get(`${ORDER}/orders/does-not-exist`, params('GET order /orders/:id (unknown)', 404));
    check(res, { 'GET /orders/:id (unknown): 404': (r) => r.status === 404 });
  }},
];

// Sum of all the weights, used to draw an action.
const totalWeight = actions.reduce((sum, a) => sum + a.weight, 0);

// Draw one action at random, in proportion to its weight.
//
// Returns
// -------
// object
//     The chosen element of `actions`.
function chooseAction() {
  let roll = Math.random() * totalWeight;
  for (const a of actions) {
    if (roll < a.weight) return a;
    roll -= a.weight;
  }
  return actions[0];
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------
// Iteration of each virtual user: run one random action, then wait 100 ms.
export default function () {
  chooseAction().run();
  sleep(0.1);
}
