// k6 load test for POST /transfers at a fixed arrival rate.
//   k6 run -e RATE=500 -e ACCOUNTS=1000 -e DURATION=60s -e SUMMARY_OUT=out.json bench/k6/transfers.js
// constant-arrival-rate starts RATE new requests per second whether or not earlier ones
// finished, so a slow server shows up as latency and errors instead of quietly lowering
// the request rate (the "coordinated omission" trap of closed-loop load tests).
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RATE = parseInt(__ENV.RATE || '100', 10);
const DURATION = __ENV.DURATION || '60s';
const ACCOUNTS = parseInt(__ENV.ACCOUNTS || '1000', 10);
const RUN_ID = __ENV.RUN_ID || `${Date.now()}`;
const STARTING_BALANCE = 100000000; // $1M in cents: insufficient funds is effectively impossible
const JSON_HEADERS = { headers: { 'Content-Type': 'application/json' } };

const transfersCreated = new Counter('transfers_created');

export const options = {
  scenarios: {
    transfers: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(50, Math.ceil(RATE / 10)),
      maxVUs: Math.max(500, RATE * 2),
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
  setupTimeout: '300s',
  // Always-true thresholds: they only exist to get per-scenario sub-metrics in the summary,
  // so setup() requests (account creation) don't count toward latency or errors.
  thresholds: {
    'http_req_duration{scenario:transfers}': ['max>=0'],
    'http_req_failed{scenario:transfers}': ['rate>=0'],
    'http_reqs{scenario:transfers}': ['count>=0'],
  },
};

export function setup() {
  const ids = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const acc = http.post(`${BASE}/accounts`, JSON.stringify({ owner: `k6-${i}`, currency: 'USD' }), JSON_HEADERS);
    const id = acc.json('id');
    http.post(`${BASE}/accounts/${id}/deposits`, JSON.stringify({ amount: STARTING_BALANCE }), JSON_HEADERS);
    ids.push(id);
  }
  return { ids };
}

export default function (data) {
  const ids = data.ids;
  const from = ids[Math.floor(Math.random() * ids.length)];
  let to = from;
  while (to === from) {
    to = ids[Math.floor(Math.random() * ids.length)];
  }
  const body = JSON.stringify({
    from, to,
    amount: 1 + Math.floor(Math.random() * 100),
    currency: 'USD',
    idempotencyKey: `${RUN_ID}-${__VU}-${__ITER}`,
  });
  const res = http.post(`${BASE}/transfers`, body, Object.assign({ tags: { name: 'POST /transfers' } }, JSON_HEADERS));
  if (check(res, { 'created (201)': (r) => r.status === 201 })) {
    transfersCreated.add(1);
  }
}

export function handleSummary(data) {
  const m = data.metrics;
  const dur = m['http_req_duration{scenario:transfers}'].values;
  const reqs = m['http_reqs{scenario:transfers}'].values;
  const failed = m['http_req_failed{scenario:transfers}'].values;
  const created = m.transfers_created ? m.transfers_created.values.count : 0;
  const dropped = m.dropped_iterations ? m.dropped_iterations.values.count : 0;
  const seconds = parseFloat(DURATION);
  const result = {
    targetRate: RATE,
    accounts: ACCOUNTS,
    durationSeconds: seconds,
    requests: reqs.count,
    requestsPerSecond: +(reqs.count / seconds).toFixed(1),
    transfersCreated: created,
    transfersPerSecond: +(created / seconds).toFixed(1),
    droppedIterations: dropped,
    errorRatePercent: +(failed.rate * 100).toFixed(3),
    p50ms: +dur.med.toFixed(2),
    p95ms: +dur['p(95)'].toFixed(2),
    p99ms: +dur['p(99)'].toFixed(2),
    maxms: +dur.max.toFixed(2),
  };
  const out = {};
  out.stdout = `LOADRESULT ${JSON.stringify(result)}\n`;
  if (__ENV.SUMMARY_OUT) {
    out[__ENV.SUMMARY_OUT] = JSON.stringify(result, null, 2);
    out[__ENV.SUMMARY_OUT.replace(/\.json$/, '.k6-full.json')] = JSON.stringify(data, null, 2);
  }
  return out;
}
