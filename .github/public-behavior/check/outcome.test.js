/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const assert = require('assert');
const { test } = require('node:test');
const { decide, validate } = require('./outcome.js');
const { NO_RFC } = require('./rfc.js');
const { CONTRACT, DEPRECATED } = require('./test-fixtures.js');

test('a PR without contract changes passes before the RFC check', () => {
    assert.deepStrictEqual(decide([], { ...NO_RFC, invalidNames: ['rfcs/x.md'], files: ['rfcs/x.md'] }), { outcome: 'clean' });
});

test('a PR that only removes deprecated or changes experimental API passes with a review warning', () => {
    assert.strictEqual(decide([DEPRECATED], NO_RFC).outcome, 'review');
});

test('a PR that changes only the check passes with a review warning whatever it finds', () => {
    assert.strictEqual(decide([CONTRACT, DEPRECATED], NO_RFC, true).outcome, 'check');
    assert.deepStrictEqual(decide([], NO_RFC, true), { outcome: 'clean' });
});

test('a contract change without an RFC fails and explains the bad links', () => {
    const res = decide([CONTRACT, DEPRECATED], { ...NO_RFC, badLinks: [{ path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' }] });

    assert.strictEqual(res.outcome, 'rfc-missing');
    assert.deepStrictEqual(res.badLinks, [{ path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' }]);
});

test('an invalid RFC filename fails even with a valid RFC and a valid link', () => {
    const res = decide([CONTRACT], {
        files: ['rfcs/IGNITE-1-x.md', 'rfcs/my-proposal.md'], invalidNames: ['rfcs/my-proposal.md'], linked: ['rfcs/IGNITE-7-y.md'], badLinks: [],
    });

    assert.deepStrictEqual([res.outcome, res.invalidNames], ['rfc-name', ['rfcs/my-proposal.md']]);
});

test('an RFC in the PR or a link to an existing RFC passes with a review warning', () => {
    const inPr = decide([CONTRACT], { ...NO_RFC, files: ['rfcs/IGNITE-1-x.md'], linked: ['rfcs/IGNITE-7-y.md'] });
    const linked = decide([CONTRACT], { ...NO_RFC, linked: ['rfcs/IGNITE-7-y.md'] });

    assert.deepStrictEqual([inPr.outcome, inPr.rfcs], ['rfc-found', [{ path: 'rfcs/IGNITE-1-x.md', source: 'pr' }]]);
    assert.deepStrictEqual([linked.outcome, linked.rfcs], ['rfc-found', [{ path: 'rfcs/IGNITE-7-y.md', source: 'link' }]]);
});

test('results that the PR job could not produce are rejected', () => {
    const ok = decide([CONTRACT], NO_RFC);
    const check = decide([DEPRECATED], NO_RFC, true);

    assert.deepStrictEqual(validate(ok), ok);
    assert.deepStrictEqual(validate(check), check);
    assert.deepStrictEqual(validate({ outcome: 'error', step: 'rm -rf' }), { outcome: 'error', step: 'result' });
    assert.throws(() => validate({ outcome: 'approved' }));
    assert.throws(() => validate({ ...ok, findings: [{ ...CONTRACT, key: 'x\n### Approved' }] }));
    assert.throws(() => validate({ ...ok, outcome: 'rfc-found', rfcs: [{ path: 'rfcs/IGNITE-1-x.md)](https://evil', source: 'pr' }] }));
    assert.throws(() => validate({ ...ok, outcome: 'rfc-found', rfcs: [{ path: 'rfcs/IGNITE-1-x.md', source: 'pr', blob: 'not a blob' }] }));
    assert.throws(() => validate({ ...ok, outcome: 'review' }));
    assert.throws(() => validate({ ...check, outcome: 'rfc-missing' }));
    assert.throws(() => validate({ ...ok, outcome: 'rfc-found' }));
    assert.throws(() => validate({ ...ok, notShown: 0 }));
    assert.throws(() => validate({ ...ok, notShown: '1' }));
});
