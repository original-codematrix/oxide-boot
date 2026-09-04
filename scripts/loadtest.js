// k6 scenario for the rest-api example. Driven by scripts/loadtest.sh.
//
// Mixes the three handler shapes the framework has to get right under load:
// a plain read, a write that also publishes to Kafka, and a path-parameter
// read that must return RFC 7807 problem+json for a miss.

// Every request sets an explicit `name` tag: k6 defaults it to the full URL,
// which turns a path parameter into unbounded Prometheus cardinality.

import http from 'k6/http';
import { check, group } from 'k6';
import { Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:18080';
const MGMT = __ENV.MGMT_URL || 'http://127.0.0.1:18081';

const problemShape = new Rate('problem_json_well_formed');

export const options = {
  scenarios: {
    api: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.VUS || '50', 10),
      duration: __ENV.DURATION || '30s',
      gracefulStop: '10s',
    },
  },
  thresholds: {
    // A framework that cannot hold these under 50 VUs on a laptop has a
    // problem worth seeing, so they are failures, not warnings.
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<250', 'p(99)<1000'],
    problem_json_well_formed: ['rate>0.99'],
    checks: ['rate>0.99'],
  },
};

export default function () {
  group('list', () => {
    const res = http.get(`${BASE}/api/notes`, { tags: { name: 'GET /api/notes', route: 'GET /api/notes' } });
    check(res, {
      'list 200': (r) => r.status === 200,
      'list is array': (r) => Array.isArray(r.json()),
    });
  });

  let createdId = null;
  group('create', () => {
    const payload = JSON.stringify({ id: 0, title: `note-${__VU}-${__ITER}`, body: 'load' });
    const res = http.post(`${BASE}/api/notes`, payload, {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'POST /api/notes', route: 'POST /api/notes' },
    });
    const ok = check(res, {
      'create 200': (r) => r.status === 200,
      'create returns id': (r) => r.status === 200 && typeof r.json('id') === 'number',
    });
    if (ok) createdId = res.json('id');
  });

  group('read back', () => {
    if (createdId === null) return;
    const res = http.get(`${BASE}/api/notes/${createdId}`, { tags: { name: 'GET /api/notes/{id}', route: 'GET /api/notes/{id}' } });
    check(res, { 'read back 200': (r) => r.status === 200 });
  });

  group('not found', () => {
    const res = http.get(`${BASE}/api/notes/999999999`, {
      tags: { name: 'GET /api/notes/{id} (miss)', route: 'GET /api/notes/{id} (miss)' },
      // 404 is the expected outcome here, so it must not count as a failure.
      responseCallback: http.expectedStatuses(404),
    });
    const ct = (res.headers['Content-Type'] || '').includes('application/problem+json');
    let body = null;
    try { body = res.json(); } catch (_) { /* handled by the check below */ }
    const wellFormed = res.status === 404 && ct && body !== null
      && typeof body.status === 'number' && typeof body.title === 'string';
    problemShape.add(wellFormed);
    check(res, { 'miss is RFC 7807 problem+json': () => wellFormed });
  });

  group('actuator', () => {
    const res = http.get(`${MGMT}/actuator/health`, { tags: { name: 'GET /actuator/health', route: 'GET /actuator/health' } });
    check(res, {
      'health 200': (r) => r.status === 200,
      'health is UP': (r) => r.json('status') === 'UP',
    });
  });
}
