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
const { rfcLinks } = require('./rfc.js');

test('a link counts if its path is an RFC file on the target branch, whatever branch, tag or commit it names', () => {
    const body = 'Covered by https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-existing.md. ' +
        'See [RFC](https://github.com/apache/ignite/blob/mastr/rfcs/IGNITE-9-typo.md#compatibility), ' +
        'https://github.com/apache/ignite/blob/1a2b3c/rfcs/IGNITE-10-permalink.md, ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-absent.md, ' +
        'https://github.com/apache/ignite/blob/master/rfcs/Bad_Name.md and https://github.com/apache/ignite/pull/13760.';
    const onTarget = f => ['rfcs/IGNITE-7-existing.md', 'rfcs/IGNITE-9-typo.md', 'rfcs/IGNITE-10-permalink.md'].includes(f);

    assert.deepStrictEqual(rfcLinks(body, 'apache/ignite', onTarget), {
        linked: ['rfcs/IGNITE-7-existing.md', 'rfcs/IGNITE-9-typo.md', 'rfcs/IGNITE-10-permalink.md'],
        badLinks: [{ path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' }, { path: 'rfcs/Bad_Name.md', reason: 'name' }],
    });
});

test('each file is reported once, and a path past the file is not a link to it', () => {
    const body = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md, again ' +
        'https://github.com/apache/ignite/tree/master/rfcs/IGNITE-7-y.md, also ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-z.md/extra and ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-z.md/extra';
    const onTarget = f => ['rfcs/IGNITE-7-y.md', 'rfcs/IGNITE-8-z.md'].includes(f);

    assert.deepStrictEqual(rfcLinks(body, 'apache/ignite', onTarget), {
        linked: ['rfcs/IGNITE-7-y.md'],
        badLinks: [{ path: 'rfcs/IGNITE-8-z.md/extra', reason: 'name' }],
    });
});

test('only links to the target repository count', () => {
    const body = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md ' +
        'https://github.com/owner/standXrepo/blob/master/rfcs/IGNITE-8-z.md ' +
        'https://github.com/owner/stand.repo/blob/master/rfcs/IGNITE-9-w.md';

    assert.deepStrictEqual(rfcLinks(body, 'owner/stand.repo', () => true), { linked: ['rfcs/IGNITE-9-w.md'], badLinks: [] });
});

test('a branch name may contain slashes', () => {
    const body = 'https://github.com/apache/ignite/blob/release/docs/rfcs/IGNITE-7-y.md';

    assert.deepStrictEqual(rfcLinks(body, 'apache/ignite', () => true), { linked: ['rfcs/IGNITE-7-y.md'], badLinks: [] });
});
