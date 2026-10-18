import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

// ---------------------------------------------------------------------------
// Rate limiter demo: POST /orders is limited to 5 requests per second per
// client IP. This test sends 20 requests per second from a single IP, so about
// 15 of every 20 must be rejected with HTTP 429.
//
//   docker compose:  docker compose --profile testing run --rm k6 run rate-limit-test.js
//   Kubernetes:      docker run --rm -i -v "${PWD}/k6:/scripts" -e BASE_URL=http://host.docker.internal:8000 grafana/k6:0.49.0 run /scripts/rate-limit-test.js
// ---------------------------------------------------------------------------
const BASE_URL = __ENV.BASE_URL || 'http://api-gateway:8000';

const accepted = new Counter('orders_accepted_202');
const limited = new Counter('orders_rate_limited_429');

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 20,            // iterations (requests) per second
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 20,
    },
  },
  thresholds: {
    orders_accepted_202: ['count>0'],      // some requests go through...
    orders_rate_limited_429: ['count>0'],  // ...and the excess is rejected
  },
};

const params = {
  headers: { 'Content-Type': 'application/json' },
  // 202 and 429 are the expected answers in this test
  responseCallback: http.expectedStatuses(202, 429),
};

export default function () {
  const body = JSON.stringify({
    customer_id: `c-${__VU}`,
    customer_email: `user${__VU}@example.com`,
    event_id: 'evt-101',
    quantity: 1,
  });

  const res = http.post(`${BASE_URL}/orders`, body, params);

  if (res.status === 202) accepted.add(1);
  if (res.status === 429) limited.add(1);

  check(res, { 'status is 202 or 429': (r) => r.status === 202 || r.status === 429 });
}
