// k6 scenario for the `showcase` example. Driven by scripts/loadtest-showcase.sh.
//
// The mix is deliberate rather than uniform: reads dominate, and most reads
// target a small hot set so the Redis cache-aside path actually gets hits.
// A run where every read misses would only measure Mongo.

import http from 'k6/http';
import { check, group } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const MGMT = __ENV.MGMT_URL || 'http://127.0.0.1:8081';

const problemShape = new Rate('problem_json_well_formed');
const created = new Counter('notes_created');

// Ids created during the run, shared per VU. Reads pick from here, so the
// cache has something to hit; a global list is not available across VUs in
// k6, and per-VU is enough to produce a hot set.
let hot = [];

export const options = {
  scenarios: {
    api: {
      executor: 'ramping-vus',
      startVUs: 0,
      // Ramp rather than a step: a cliff at t=0 measures cold-start and
      // connection setup, not steady-state behaviour.
      stages: [
        { duration: '10s', target: parseInt(__ENV.VUS || '50', 10) },
        { duration: __ENV.DURATION || '60s', target: parseInt(__ENV.VUS || '50', 10) },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<300', 'p(99)<1500'],
    problem_json_well_formed: ['rate>0.99'],
    checks: ['rate>0.99'],
  },
};

export default function () {
  // ~1 write per 5 iterations, so the read path dominates the way it does in
  // most real services.
  const write = __ITER % 5 === 0;

  if (write) {
    group('create', () => {
      const payload = JSON.stringify({
        title: `note-${__VU}-${__ITER}`,
        body: 'x'.repeat(200),
        tag: ['general', 'urgent', 'archive'][__ITER % 3],
      });
      const res = http.post(`${BASE}/api/notes`, payload, {
        headers: { 'Content-Type': 'application/json' },
        tags: { route: 'POST /api/notes' },
      });
      const ok = check(res, {
        'create 200': (r) => r.status === 200,
        'create returns id': (r) => r.status === 200 && typeof r.json('id') === 'string',
      });
      if (ok) {
        created.add(1);
        hot.push(res.json('id'));
        // Cap the hot set: an unbounded list would drift towards uniform
        // random access and stop exercising the cache.
        if (hot.length > 20) hot.shift();
      }
    });
  }

  group('read', () => {
    if (hot.length === 0) return;
    const id = hot[Math.floor(Math.random() * hot.length)];
    const res = http.get(`${BASE}/api/notes/${id}`, { tags: { route: 'GET /api/notes/{id}' } });
    check(res, {
      'read 200': (r) => r.status === 200,
      'read returns the right note': (r) => r.status === 200 && r.json('id') === id,
    });
  });

  group('list', () => {
    const tag = ['general', 'urgent', 'archive'][__ITER % 3];
    const res = http.get(`${BASE}/api/notes?tag=${tag}`, { tags: { route: 'GET /api/notes' } });
    check(res, {
      'list 200': (r) => r.status === 200,
      'list is array': (r) => Array.isArray(r.json()),
    });
  });

  group('not found', () => {
    const res = http.get(`${BASE}/api/notes/does-not-exist`, {
      tags: { route: 'GET /api/notes/{id} (miss)' },
      // 404 is the expected outcome here, so it must not count as a failure.
      responseCallback: http.expectedStatuses(404),
    });
    const ct = (res.headers['Content-Type'] || '').includes('application/problem+json');
    let body = null;
    try { body = res.json(); } catch (_) { /* covered by the check below */ }
    const wellFormed = res.status === 404 && ct && body !== null
      && typeof body.status === 'number' && typeof body.title === 'string';
    problemShape.add(wellFormed);
    check(res, { 'miss is RFC 7807 problem+json': () => wellFormed });
  });

  // Only occasionally: /api/stats and /actuator/health are there to be
  // watched, and hammering them would distort the mix the dashboard shows.
  if (__ITER % 20 === 0) {
    group('stats and health', () => {
      const stats = http.get(`${BASE}/api/stats`, { tags: { route: 'GET /api/stats' } });
      check(stats, {
        'stats 200': (r) => r.status === 200,
        'stats counts events': (r) => r.status === 200 && typeof r.json('events_consumed') === 'number',
      });
      const health = http.get(`${MGMT}/actuator/health`, { tags: { route: 'GET /actuator/health' } });
      check(health, {
        'health 200': (r) => r.status === 200,
        'health is UP': (r) => r.json('status') === 'UP',
      });
    });
  }
}
