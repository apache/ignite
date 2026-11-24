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
const { render } = require('./render.js');
const { CONTRACT } = require('./test-fixtures.js');

test('PR data cannot escape code formatting', () => {
    const res = {
        outcome: 'rfc-missing', rfcs: [], invalidNames: [],
        findings: [{ ...CONTRACT, key: 'x ``` @someone [link](https://evil)', after: '````' }],
        badLinks: [{ path: 'rfcs/`@someone`.md', reason: 'name' }],
    };
    const md = render(res, {}, 100);

    assert.ok(md.includes('`rfcs/@someone.md`'), md);
    assert.ok(md.includes('`````diff\n+ x ``` @someone [link](https://evil): ```` (A.java:1)\n`````'), md);
});

test('a linked RFC is shown on the target repository and branch', () => {
    const rfcs = [{ path: 'rfcs/IGNITE-7-y.md', source: 'link' }];
    const res = { outcome: 'rfc-found', findings: [CONTRACT], rfcs, invalidNames: [], badLinks: [] };
    const md = render(res, { baseRepoUrl: 'https://github.com/owner/stand', baseRef: 'release/docs' }, 10);

    assert.ok(md.includes('(https://github.com/owner/stand/blob/release/docs/rfcs/IGNITE-7-y.md)'), md);
});

test('long reports are cut to stay within the GitHub comment limit', () => {
    const findings = Array.from({ length: 30 }, (_, i) => ({ ...CONTRACT, key: 'k' + i, after: 'x'.repeat(i === 0 ? 1000 : 1) }));
    const md = render({ outcome: 'rfc-missing', findings, rfcs: [], invalidNames: [], badLinks: [] }, {}, 10);

    assert.strictEqual(md.split('\n').find(l => l.startsWith('+ k0')).length, 500);
    assert.ok(md.includes('+ k9: x') && !md.includes('+ k10: x'), md);
    assert.ok(md.includes('20 more changes are not shown here.'), md);
});

test('a failed description says who fixes it: the PR for its merge, the target branch for itself', () => {
    const merge = render({ outcome: 'error', step: 'merge' }, {}, 10);
    const base = render({ outcome: 'error', step: 'base' }, {}, 10);

    assert.ok(merge.includes('If the PR does not compile, fix it.') && merge.includes('`.github/public-behavior/describe`'), merge);
    assert.ok(base.includes('the check needs a fix on the target branch'), base);
});

test('a possible change is marked, and the review warning explains it', () => {
    const possible = { ...CONTRACT, before: 'writeInt; ?call:writeObject', after: 'writeInt; writeJdbcObject', kind: 'possible' };
    const md = render({ outcome: 'review', findings: [possible], rfcs: [], invalidNames: [], badLinks: [] }, {}, 10);

    assert.ok(md.includes('writeJdbcObject [possible] (A.java:1)') && md.includes('check whether the bytes'), md);
});
