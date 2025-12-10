// k6 load test for online feature serving
// Run: k6 run tests/load/online_serving.js

import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
    stages: [
        { duration: '1m', target: 100 },   // Ramp up to 100 RPS
        { duration: '5m', target: 100 },   // Stay at 100 RPS
        { duration: '1m', target: 1000 },  // Ramp up to 1K RPS
        { duration: '5m', target: 1000 },  // Stay at 1K RPS
        { duration: '1m', target: 10000 }, // Ramp up to 10K RPS
        { duration: '5m', target: 10000 }, // Stay at 10K RPS (target)
        { duration: '1m', target: 0 },     // Ramp down
    ],
    thresholds: {
        http_req_duration: ['p(95)<10', 'p(99)<50'], // 95th < 10ms, 99th < 50ms
        http_req_failed: ['rate<0.01'],              // Error rate < 1%
    },
};

const BASE_URL = __ENV.FEATUREDUCK_URL || 'http://localhost:8000';

export default function () {
    // Generate random user_id (simulate 1M users)
    const userId = `user_${Math.floor(Math.random() * 1000000)}`;
    
    const payload = JSON.stringify({
        features: ['user_features:clicks_7d', 'user_features:purchases_7d'],
        entities: [{ user_id: userId }],
    });
    
    const params = {
        headers: { 'Content-Type': 'application/json' },
    };
    
    const res = http.post(`${BASE_URL}/v1/features/online`, payload, params);
    
    check(res, {
        'status is 200': (r) => r.status === 200,
        'has features': (r) => JSON.parse(r.body).features.length > 0,
        'latency < 50ms': (r) => r.timings.duration < 50,
    });
    
    sleep(0.01); // 10ms think time between requests per VU
}

export function handleSummary(data) {
    return {
        'stdout': textSummary(data, { indent: ' ', enableColors: true }),
        'results/load_test_results.json': JSON.stringify(data),
    };
}
