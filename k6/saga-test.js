// ===========================================================================
// Saga test for TicketMEI (k6)
// ===========================================================================
//
// Prepares an event with only 20 tickets and fires 1000 VUs at the same time,
// each one buying 1 ticket with POST /orders. At the end it checks the data of
// the Inventory, Order and Payment services and prints the latency of the
// Sagas that completed, of the ones that were compensated and of the ones
// rejected for lack of stock.
//
// Modes (MODE)
// ------------
// stock (default)
//     Payment works. Exactly 20 purchases are CONFIRMED (201) and the other
//     980 are rejected by the Inventory step (409): the Saga stops at step 1,
//     so there is nothing to compensate.
// faults
//     Payment fails `FAULT_ERROR_RATE` of the time (HTTP 500). The purchases
//     that reserved a ticket and whose payment failed go through the
//     compensating transaction (POST /inventory/release): those are the
//     "compensated" Sagas, and their tickets go back to the stock.
//
// Usage
// -----
// docker compose (the Order, Inventory and Payment services must be up)
//     docker compose --profile testing run --rm k6 run saga-test.js
//     docker compose --profile testing run --rm -e MODE=faults k6 run saga-test.js
// Kubernetes: see k8s/README.md (port-forward of order, inventory and payment).
//
// Environment Variables
// ---------------------
// ORDER_URL : str, optional
//     Order service URL (default `http://order:8002`).
// INVENTORY_URL : str, optional
//     Inventory service URL (default `http://inventory:8005`).
// PAYMENT_URL : str, optional
//     Payment service URL (default `http://payment:8004`).
// EVENT_ID : str, optional
//     Event to buy; it must exist in the Catalog (default `evt-101`).
// TICKETS : int, optional
//     Tickets available in the event (default `20`).
// VUS : int, optional
//     Simultaneous buyers (default `1000`).
// MODE : {"stock", "faults"}, optional
//     See above (default `stock`).
// FAULT_ERROR_RATE : float, optional
//     Probability of HTTP 500 in Payment in the `faults` mode (default `0.5`).
//
// Notes
// -----
// The requests go straight to the Order service, not through the API
// Gateway: its rate limiter allows only 5 POST /orders per second per IP and
// would answer 429 to almost every VU.
//
// Start from a healthy system: the Payment Circuit Breaker of the Order
// service must be CLOSED (restart Order, or wait CB_OPEN_MS after a previous
// test), otherwise the purchases get 503 instead of 201.
//
// The consistency check compares the counters of the services before and
// after the test, so the services do not need to be restarted between runs.
// Every violation is counted in `consistency_violations`, which has a
// threshold of 0.
//
// ===========================================================================

import http from 'k6/http';
import { Counter, Trend } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const ORDER_URL = __ENV.ORDER_URL || 'http://order:8002';
const INVENTORY_URL = __ENV.INVENTORY_URL || 'http://inventory:8005';
const PAYMENT_URL = __ENV.PAYMENT_URL || 'http://payment:8004';

const EVENT_ID = __ENV.EVENT_ID || 'evt-101';
const TICKETS = Number(__ENV.TICKETS) || 20;
const VUS = Number(__ENV.VUS) || 1000;
const MODE = __ENV.MODE || 'stock';
const FAULT_ERROR_RATE = Number(__ENV.FAULT_ERROR_RATE) || 0.5;

const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };
const NO_FAULTS = { error_rate: 0, latency_ms: 0 };

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------

// Number of purchases by outcome:
// confirmed (201), out of stock (409), compensated (the payment failed after
// the tickets were reserved: 502 / 503 / 504 "Payment ...") and unexpected
// (anything else, e.g. Catalog or Inventory errors, or no answer).
const ordersConfirmed = new Counter('orders_confirmed');
const ordersOutOfStock = new Counter('orders_out_of_stock');
const ordersCompensated = new Counter('orders_compensated');
const ordersUnexpected = new Counter('orders_unexpected');

// Duration of POST /orders (ms) by outcome, to compare the Sagas.
const durationSuccess = new Trend('saga_duration_success_ms', true);
const durationCompensated = new Trend('saga_duration_compensated_ms', true);
const durationOutOfStock = new Trend('saga_duration_out_of_stock_ms', true);

// Number of data consistency checks that failed (teardown).
const consistencyViolations = new Counter('consistency_violations');

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

// All the VUs are started together and each runs a single iteration, so the
// VUS purchases are sent at the same moment.
//
// In the `stock` mode the numbers are exact. In the `faults` mode the
// purchases that fail the payment release their ticket, so the number of
// CONFIRMED orders is at most `TICKETS` and the compensations must exist.
const thresholds = {
  'consistency_violations': ['count==0'],
  'orders_unexpected': ['count==0'],
  'saga_duration_success_ms': ['max>=0'],
  'saga_duration_compensated_ms': ['max>=0'],
  'saga_duration_out_of_stock_ms': ['max>=0'],
};

if (MODE === 'stock') {
  thresholds['orders_confirmed'] = [`count==${TICKETS}`];
  thresholds['orders_out_of_stock'] = [`count==${VUS - TICKETS}`];
}
else {
  thresholds['orders_confirmed'] = [`count<=${TICKETS}`];
  thresholds['orders_compensated'] = ['count>0'];
}

export const options = {
  scenarios: {
    buyers: {
      executor: 'per-vu-iterations',
      vus: VUS,
      iterations: 1,
      maxDuration: '2m',
    },
  },
  thresholds,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// GET a JSON document.
//
// Parameters
// ----------
// url : str
//     URL to read.
//
// Returns
// -------
// object or null
//     The parsed body, or null if the request failed.
function getJson(url) {
  const response = http.get(url);
  return response.status === 200 ? response.json() : null;
}

// Counters of the three services at one moment.
//
// Returns
// -------
// object
//     `orders` (GET /orders/stats), `payments` (GET /payments/stats) and
//     `inventory` (GET /inventory/{event_id}); each one is null if its
//     request failed.
function snapshot() {
  return {
    orders: getJson(`${ORDER_URL}/orders/stats`),
    payments: getJson(`${PAYMENT_URL}/payments/stats`),
    inventory: getJson(`${INVENTORY_URL}/inventory/${EVENT_ID}`),
  };
}

// Record a consistency check; a failed check is counted and printed.
//
// Parameters
// ----------
// description : str
//     What is being checked.
// ok : boolean
//     Whether it holds.
// detail : str
//     Values to show in the log.
function verify(description, ok, detail) {
  console.log(`${ok ? 'OK  ' : 'FAIL'} ${description} (${detail})`);

  if (!ok)
    consistencyViolations.add(1);
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

// Prepare the event and the Payment service, and read the counters before.
//
// Returns
// -------
// object
//     The `snapshot()` taken before the purchases.
export function setup() {
  const stock = http.post(`${INVENTORY_URL}/inventory/stock`, JSON.stringify({ event_id: EVENT_ID, total_tickets: TICKETS }), JSON_HEADERS);

  if (stock.status !== 200 && stock.status !== 201)
    throw new Error(`Could not set the stock of ${EVENT_ID}: HTTP ${stock.status} ${stock.body}`);

  const faults = MODE === 'faults' ? { error_rate: FAULT_ERROR_RATE, latency_ms: 0 } : NO_FAULTS;
  const payment = http.post(`${PAYMENT_URL}/payments/configure-faults`, JSON.stringify(faults), JSON_HEADERS);

  if (payment.status !== 200)
    throw new Error(`Could not configure the Payment faults: HTTP ${payment.status} ${payment.body}`);

  const before = snapshot();

  if (!before.orders || !before.payments || !before.inventory)
    throw new Error('Could not read the stats of Order, Payment and Inventory');

  console.log(`MODE=${MODE}: ${VUS} VUs buying 1 ticket of ${EVENT_ID}, ${TICKETS} tickets in stock`);
  return before;
}

// One purchase (one iteration per VU).
//
// Notes
// -----
// Each response is classified: 201 confirmed, 409 out of stock, an error
// whose message starts with "Payment" compensated, anything else unexpected.
export default function () {
  const body = JSON.stringify({
    customer_id: `k6-vu-${__VU}`,
    customer_email: `vu${__VU}@k6.test`,
    event_id: EVENT_ID,
    quantity: 1,
  });

  const response = http.post(`${ORDER_URL}/orders`, body, { headers: JSON_HEADERS.headers, timeout: '60s' });
  const duration = response.timings.duration;

  if (response.status === 201) {
    ordersConfirmed.add(1);
    durationSuccess.add(duration);
    return;
  }

  if (response.status === 409) {
    ordersOutOfStock.add(1);
    durationOutOfStock.add(duration);
    return;
  }

  let error = '';

  try { error = response.json('error') || ''; } catch (e) { error = ''; }

  if ([502, 503, 504].includes(response.status) && String(error).startsWith('Payment')) {
    ordersCompensated.add(1);
    durationCompensated.add(duration);
    return;
  }

  ordersUnexpected.add(1);
  console.log(`Unexpected answer: HTTP ${response.status} ${response.body}`);
}

// Restore Payment and check that Inventory, Payment and Order agree.
//
// Parameters
// ----------
// before : object
//     The `snapshot()` returned by `setup`.
//
// Notes
// -----
// Checks (counters are differences between after and before):
// - no order is left PENDING and no compensation FAILED;
// - the CONFIRMED orders, the SUCCESS payments and the tickets taken from the
//   Inventory (total - available) are the same number, and so are the amounts
//   of the CONFIRMED orders and of the SUCCESS payments;
// - in the `stock` mode, exactly TICKETS orders are CONFIRMED and the stock
//   is empty.
// A SUCCESS payment without a CONFIRMED order would be a customer charged
// without a ticket (e.g. the Order gave up waiting for the Payment).
export function teardown(before) {
  http.post(`${PAYMENT_URL}/payments/configure-faults`, JSON.stringify(NO_FAULTS), JSON_HEADERS);

  const after = snapshot();

  if (!after.orders || !after.payments || !after.inventory) {
    verify('stats of Order, Payment and Inventory are readable', false, 'a stats request failed');
    return;
  }

  const confirmed = after.orders.by_status.CONFIRMED - before.orders.by_status.CONFIRMED;
  const failed = after.orders.by_status.FAILED - before.orders.by_status.FAILED;
  const pending = after.orders.by_status.PENDING - before.orders.by_status.PENDING;
  const paid = after.payments.by_status.SUCCESS - before.payments.by_status.SUCCESS;
  const confirmedAmount = after.orders.confirmed_amount - before.orders.confirmed_amount;
  const paidAmount = after.payments.success_amount - before.payments.success_amount;
  const compensationsFailed = after.orders.compensation.FAILED - before.orders.compensation.FAILED;
  const compensationsDone = after.orders.compensation.DONE - before.orders.compensation.DONE;
  const ticketsTaken = after.inventory.total_tickets - after.inventory.available_tickets;

  console.log(`Orders: ${confirmed} CONFIRMED, ${failed} FAILED (${compensationsDone} with the tickets released). Payments: ${paid} SUCCESS.`);
  console.log(`Inventory ${EVENT_ID}: ${after.inventory.available_tickets} of ${after.inventory.total_tickets} tickets available.`);

  verify('no order is left PENDING', pending === 0, `pending=${pending}`);
  verify('no compensation failed', compensationsFailed === 0, `failed compensations=${compensationsFailed}`);
  verify('CONFIRMED orders = SUCCESS payments', confirmed === paid, `orders=${confirmed}, payments=${paid}`);
  verify('CONFIRMED orders = tickets taken from the Inventory', confirmed === ticketsTaken, `orders=${confirmed}, tickets=${ticketsTaken}`);
  verify('amount of the CONFIRMED orders = amount of the SUCCESS payments', Math.abs(confirmedAmount - paidAmount) < 0.005, `orders=${confirmedAmount.toFixed(2)}, payments=${paidAmount.toFixed(2)}`);

  if (MODE === 'stock') {
    verify(`exactly ${TICKETS} orders CONFIRMED`, confirmed === TICKETS, `confirmed=${confirmed}`);
    verify('stock is empty', after.inventory.available_tickets === 0, `available=${after.inventory.available_tickets}`);
  }
}
