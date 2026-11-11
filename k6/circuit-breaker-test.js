// ===========================================================================
// Circuit Breaker test for TicketMEI (k6)
// ===========================================================================
//
// Keeps a constant load of 50 POST /orders per second and, in the middle of
// the test, degrades the Payment service for 2 minutes (80% of HTTP 500 or
// 10 s of latency) and then restores it. Used to watch, in Prometheus /
// Grafana, the Circuit Breaker of the Order service go
// CLOSED -> OPEN -> HALF_OPEN -> CLOSED.
//
// Phases (default durations)
// --------------------------
// normal : 0 - 2 min
//     Payment healthy. Orders get 201, circuit CLOSED.
// degraded : 2 - 4 min
//     Payment degraded. After a few failures the circuit opens and the Order
//     service answers 503 at once (fail-fast), so the latency goes down.
//     Every `CB_OPEN_MS` (10 s) one trial call is let through (HALF_OPEN).
// recovery : 4 - 6 min
//     Payment restored. The next trial call succeeds, the circuit closes
//     and orders get 201 again.
//
// Usage
// -----
// docker compose (80% of HTTP 500, the default)
//     docker compose --profile testing run --rm k6 run circuit-breaker-test.js
// docker compose (10 s of latency)
//     docker compose --profile testing run --rm -e FAULT_MODE=latency k6 run circuit-breaker-test.js
// Kubernetes (with `kubectl port-forward svc/order 8002:8002` and
// `kubectl port-forward svc/payment 8004:8004` running in other terminals)
//     docker run --rm -i -v "${PWD}/k6:/scripts" -e ORDER_URL=http://host.docker.internal:8002 -e PAYMENT_URL=http://host.docker.internal:8004 grafana/k6:0.49.0 run /scripts/circuit-breaker-test.js
//
// Environment Variables
// ---------------------
// ORDER_URL : str, optional
//     Order service URL (default `http://order:8002`).
// PAYMENT_URL : str, optional
//     Payment service URL, used to inject and restore the faults
//     (default `http://payment:8004`).
// FAULT_MODE : {"errors", "latency"}, optional
//     "errors" = 80% of HTTP 500, "latency" = 10 s per payment
//     (default `errors`).
// RATE : int, optional
//     POST /orders per second (default `50`).
// NORMAL_S : int, optional
//     Duration of the normal phase in seconds (default `120`).
// DEGRADED_S : int, optional
//     Duration of the degradation in seconds (default `120`).
// RECOVERY_S : int, optional
//     Duration of the recovery phase in seconds (default `120`).
//
// Notes
// -----
// The requests go straight to the Order service, not through the API
// Gateway: the gateway rate limiter allows only 5 POST /orders per second per
// IP and would reject almost all of the 50 per second with HTTP 429.
//
// Every request is tagged with its `phase`, so the summary at the end shows
// the latency of each phase separately (`http_req_duration{phase:...}`).
//
// ===========================================================================

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const ORDER_URL = __ENV.ORDER_URL || 'http://order:8002';
const PAYMENT_URL = __ENV.PAYMENT_URL || 'http://payment:8004';

const FAULT_MODE = __ENV.FAULT_MODE || 'errors';
const RATE = Number(__ENV.RATE) || 50;

const NORMAL_S = Number(__ENV.NORMAL_S) || 120;
const DEGRADED_S = Number(__ENV.DEGRADED_S) || 120;
const RECOVERY_S = Number(__ENV.RECOVERY_S) || 120;
const TOTAL_S = NORMAL_S + DEGRADED_S + RECOVERY_S;

// Fault injected in the Payment service for each FAULT_MODE, and the body that restores the normal behaviour.
const FAULTS = {
  errors: { error_rate: 0.8, latency_ms: 0 },
  latency: { error_rate: 0, latency_ms: 10000 },
};
const NO_FAULTS = { error_rate: 0, latency_ms: 0 };

// Ids of the events in the Catalog (catalog/data/events.json).
const EVENT_IDS = ['evt-101', 'evt-102', 'evt-103', 'evt-104'];

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------

// Number of responses of POST /orders by meaning:
// confirmed (201), fallback with the circuit OPEN (503) and payment failed with the circuit CLOSED (502 / 504).
const ordersConfirmed = new Counter('orders_confirmed_201');
const ordersFallback = new Counter('orders_circuit_open_503');
const ordersPaymentFailed = new Counter('orders_payment_failed_502_504');

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// Three scenarios run together:
//
// orders
//     Constant arrival rate of `RATE` POST /orders per second during the
//     whole test, whatever the response time (k6 adds VUs when the answers
//     get slow, up to `maxVUs`).
// inject
//     One iteration at `NORMAL_S`: degrades the Payment service.
// restore
//     One iteration at `NORMAL_S + DEGRADED_S`: restores it.
//
// The thresholds on `http_req_duration{phase:...}` only make k6 print the
// latency of each phase in the summary (`max>=0` is always true).
export const options = {
  scenarios: {
    orders: {
      executor: 'constant-arrival-rate',
      exec: 'placeOrder',
      rate: RATE,
      timeUnit: '1s',
      duration: `${TOTAL_S}s`,
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
    inject: {
      executor: 'per-vu-iterations',
      exec: 'injectFaults',
      vus: 1,
      iterations: 1,
      startTime: `${NORMAL_S}s`,
    },
    restore: {
      executor: 'per-vu-iterations',
      exec: 'restoreFaults',
      vus: 1,
      iterations: 1,
      startTime: `${NORMAL_S + DEGRADED_S}s`,
    },
  },
  thresholds: {
    'http_req_duration{phase:normal}': ['max>=0'],
    'http_req_duration{phase:degraded}': ['max>=0'],
    'http_req_duration{phase:recovery}': ['max>=0'],
    'orders_confirmed_201': ['count>0'],
    'orders_circuit_open_503': ['count>0'],
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
// any one of the elements, chosen at random. 
const pick = (list) => list[Math.floor(Math.random() * list.length)];

// Name of the test phase at a given moment.
//
// Parameters
// ----------
// startedAt : int
//     Start of the test, in milliseconds since the epoch (from `setup`).
//
// Returns
// -------
// {"normal", "degraded", "recovery"}
//     Phase the test is in now.
function currentPhase(startedAt) {
  const elapsedS = (Date.now() - startedAt) / 1000;

  if (elapsedS < NORMAL_S)
    return 'normal';

  if (elapsedS < NORMAL_S + DEGRADED_S)
    return 'degraded';

  return 'recovery';
}

// Send a fault configuration to the Payment service.
//
// Parameters
// ----------
// faults : object
//     Body of POST /payments/configure-faults (`error_rate`, `latency_ms`).
//
// Returns
// -------
// boolean
//     True when the Payment service accepted the configuration (HTTP 200).
function configureFaults(faults) {
  const res = http.post(`${PAYMENT_URL}/payments/configure-faults`, JSON.stringify(faults), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /payments/configure-faults' },
  });

  return check(res, { 'configure-faults: 200': (r) => r.status === 200 });
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Run once before the scenarios.
//
// Returns
// -------
// object
//     `{ startedAt }`, the start time used to tag each request with its phase.
//
// Notes
// -----
// Also makes sure the Payment service starts without faults, in case a
// previous run was interrupted.
export function setup() {
  configureFaults(NO_FAULTS);
  console.log(`Test started: ${RATE} orders/s, fault "${FAULT_MODE}" from ${NORMAL_S}s to ${NORMAL_S + DEGRADED_S}s, total ${TOTAL_S}s`);
  return { startedAt: Date.now() };
}

// Run once after the scenarios (also when the test is stopped with Ctrl+C).
//
// Notes
// -----
// Restores the Payment service, so it is never left degraded.
export function teardown() {
  configureFaults(NO_FAULTS);
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

// One POST /orders (scenario `orders`).
//
// Parameters
// ----------
// data : object
//     Value returned by `setup`, with `startedAt`.
//
// Notes
// -----
// 201, 502, 503 and 504 are all expected answers of this test, so they do not
// count in `http_req_failed`; each one is counted in its own custom counter.
export function placeOrder(data) {
  const phase = currentPhase(data.startedAt);

  const body = JSON.stringify({
    customer_id: `c-${__VU}`,
    customer_email: `user${__VU}@example.com`,
    event_id: pick(EVENT_IDS),
    quantity: 1 + Math.floor(Math.random() * 4),
  });

  const res = http.post(`${ORDER_URL}/orders`, body, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /orders', phase },
    responseCallback: http.expectedStatuses(201, 502, 503, 504),
  });

  if (res.status === 201) ordersConfirmed.add(1, { phase });
  if (res.status === 503) ordersFallback.add(1, { phase });
  if (res.status === 502 || res.status === 504) ordersPaymentFailed.add(1, { phase });

  check(res, { 'POST /orders: 201, 502, 503 or 504': (r) => [201, 502, 503, 504].includes(r.status) });
}

// Degrade the Payment service (scenario `inject`).
//
// Notes
// -----
// The time is printed so it can be compared with the moment
// `circuit_breaker_state` becomes 1 in Prometheus (CLOSED -> OPEN time).
export function injectFaults() {
  configureFaults(FAULTS[FAULT_MODE]);
  console.log(`[${new Date().toISOString()}] Payment degraded: ${JSON.stringify(FAULTS[FAULT_MODE])}`);
}

// Restore the Payment service (scenario `restore`).
//
// Notes
// -----
// The time is printed so it can be compared with the moment
// `circuit_breaker_state` goes back to 0 (HALF_OPEN -> CLOSED).
export function restoreFaults() {
  configureFaults(NO_FAULTS);
  console.log(`[${new Date().toISOString()}] Payment restored`);
}
