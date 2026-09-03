// Drives ONE route, to separate the framework's own cost from what the
// backing services add. Used by scripts/floor.sh.
//
// Point it at an in-memory handler and you measure the framework; point it at
// a handler that touches Redis or Mongo and the difference is what that
// backend costs.

import http from 'k6/http';
import { check } from 'k6';
export const options = {
  scenarios: { f: { executor: 'constant-vus', vus: parseInt(__ENV.VUS||'100',10), duration: __ENV.DURATION||'20s' } },
};
export default function () {
  const res = http.get(`${__ENV.URL}`);
  check(res, { '200': (r) => r.status === 200 });
}
