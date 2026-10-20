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

const EVENT_IDS = ['evt-101', 'evt-102', 'evt-103', 'evt-104'];

// Request parameters. `name` groups URLs that contain ids under a single tag
// (otherwise k6 would create one metric series per order id), and `expected`
// marks the statuses that are *supposed* to happen, so that intentional
// 400/404 responses do not count as failures in `http_req_failed`.
function params(name, expected, json) {
  const p = { tags: { name } };
  if (expected) p.responseCallback = http.expectedStatuses(expected);
  if (json) p.headers = { 'Content-Type': 'application/json' };
  return p;
}

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
const pick = (list) => list[Math.floor(Math.random() * list.length)];

function validOrder() {
  return JSON.stringify({
    customer_id: `c-${__VU}`,
    customer_email: `user${__VU}@example.com`,
    event_id: pick(EVENT_IDS),
    quantity: 1 + Math.floor(Math.random() * 4),
  });
}

let lastOrderId = null;

// ---------------------------------------------------------------------------
// Scenarios (weights add up to 100)
// ---------------------------------------------------------------------------
const actions = [
  // Gateway -> Catalog
  { weight: 35, run: () => {
    const res = http.get(`${GATEWAY}/events`, params('GET /events'));
    check(res, { 'GET /events: 200': (r) => r.status === 200 });
  }},

  // Gateway -> Order -> Catalog + RabbitMQ -> Notification
  { weight: 25, run: () => {
    const res = http.post(`${GATEWAY}/orders`, validOrder(), params('POST /orders', 202, true));
    check(res, { 'POST /orders: 202': (r) => r.status === 202 });
    if (res.status === 202) lastOrderId = res.json('order_id');
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

const totalWeight = actions.reduce((sum, a) => sum + a.weight, 0);

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
export default function () {
  chooseAction().run();
  sleep(0.1);
}
