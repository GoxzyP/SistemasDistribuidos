// ===========================================================================
// Rate limiter test for TicketMEI (k6)
// ===========================================================================
//
// POST /orders is limited to 5 requests per second per client IP. This test
// sends 20 requests per second from a single IP for 10 seconds, so about 15
// of every 20 must be rejected with HTTP 429.
//
// Usage
// -----
// docker compose
//     docker compose --profile testing run --rm k6 run rate-limit-test.js
// Kubernetes
//     docker run --rm -i -v "${PWD}/k6:/scripts" -e BASE_URL=http://host.docker.internal:8000 grafana/k6:0.49.0 run /scripts/rate-limit-test.js
//
// Environment Variables
// ---------------------
// BASE_URL : str, optional
//     API Gateway URL (default `http://api-gateway:8000`).
//
// Thresholds
// ----------
// orders_accepted_201
//     At least one order must be accepted (the limiter lets traffic through).
// orders_rate_limited_429
//     At least one order must be rejected (the limiter works).
//
// ===========================================================================
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://api-gateway:8000';

// Custom k6 counters: orders accepted (HTTP 201) and rejected (HTTP 429).
const accepted = new Counter('orders_accepted_201');
const limited = new Counter('orders_rate_limited_429');

// Constant arrival rate of 20 requests per second for 10 seconds, whatever
// the response time, plus the pass/fail thresholds.
export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 20,           
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 20,
    },
  },
  thresholds: {
    orders_accepted_201: ['count>0'],     
    orders_rate_limited_429: ['count>0'], 
  },
};

// Request parameters: JSON body, and both 201 and 429 count as expected
// statuses (a 429 is the correct answer here, not a failure).
const params = {
  headers: { 'Content-Type': 'application/json' },
  responseCallback: http.expectedStatuses(201, 429),
};

// One iteration: send one valid POST /orders and count the result.
export default function () {
  const body = JSON.stringify({
    customer_id: `c-${__VU}`,
    customer_email: `user${__VU}@example.com`,
    event_id: 'evt-101',
    quantity: 1,
  });

  const res = http.post(`${BASE_URL}/orders`, body, params);

  if (res.status === 201) accepted.add(1);
  if (res.status === 429) limited.add(1);

  check(res, { 'status is 201 or 429': (r) => r.status === 201 || r.status === 429 });
}
