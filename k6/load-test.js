import http from 'k6/http';
import { check, sleep } from 'k6';

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

export default function () {
  const res = http.get('http://api-gateway:8000/events');
  check(res, {
    'status is 200': (r) => r.status === 200,
  });
  sleep(0.1);
}