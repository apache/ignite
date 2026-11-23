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

test('a link counts only if its file is on the target branch and the link leads to it', async () => {
    const body = 'Covered by https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-existing.md. ' +
        'See [RFC](https://github.com/apache/ignite/blob/mastr/rfcs/IGNITE-9-typo.md#compatibility), ' +
        'https://github.com/apache/ignite/blob/1a2b3c/rfcs/IGNITE-10-permalink.md, ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-absent.md, ' +
        'https://github.com/apache/ignite/blob/master/rfcs/Bad_Name.md and https://github.com/apache/ignite/pull/13760.';
    const onTarget = f => ['rfcs/IGNITE-7-existing.md', 'rfcs/IGNITE-9-typo.md', 'rfcs/IGNITE-10-permalink.md'].includes(f);
    const checked = [];
    const atRef = async (ref, f) => checked.push(`${ref}:${f}`) && ref === '1a2b3c';

    assert.deepStrictEqual(await rfcLinks(body, 'apache/ignite', 'master', onTarget, atRef), {
        linked: ['rfcs/IGNITE-7-existing.md', 'rfcs/IGNITE-10-permalink.md'],
        badLinks: [
            { path: 'rfcs/IGNITE-9-typo.md', reason: 'url' },
            { path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' },
            { path: 'rfcs/Bad_Name.md', reason: 'name' },
        ],
    });
    assert.deepStrictEqual(checked, ['mastr:rfcs/IGNITE-9-typo.md', '1a2b3c:rfcs/IGNITE-10-permalink.md']);
});

test('a broken link does not hide a working link to the same file, and a path past the file is not a link to it', async () => {
    const body = 'https://github.com/apache/ignite/blob/mastr/rfcs/IGNITE-7-y.md and then ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md, again ' +
        'https://github.com/apache/ignite/tree/master/rfcs/IGNITE-7-y.md, also ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-z.md/extra';
    const onTarget = f => ['rfcs/IGNITE-7-y.md', 'rfcs/IGNITE-8-z.md'].includes(f);

    assert.deepStrictEqual(await rfcLinks(body, 'apache/ignite', 'master', onTarget, async () => false), {
        linked: ['rfcs/IGNITE-7-y.md'],
        badLinks: [{ path: 'rfcs/IGNITE-8-z.md/extra', reason: 'name' }],
    });
});

test('only links to the target repository count', async () => {
    const body = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md ' +
        'https://github.com/owner/standXrepo/blob/master/rfcs/IGNITE-8-z.md ' +
        'https://github.com/owner/stand.repo/blob/master/rfcs/IGNITE-9-w.md';

    assert.deepStrictEqual(await rfcLinks(body, 'owner/stand.repo', 'master', () => true, async () => false),
        { linked: ['rfcs/IGNITE-9-w.md'], badLinks: [] });
});

test('a branch name may contain slashes', async () => {
    const body = 'https://github.com/apache/ignite/blob/release/docs/rfcs/IGNITE-7-y.md';
    const checked = [];
    const atRef = async (ref, f) => checked.push(`${ref}:${f}`) > 0;

    assert.deepStrictEqual(await rfcLinks(body, 'apache/ignite', 'master', () => true, atRef),
        { linked: ['rfcs/IGNITE-7-y.md'], badLinks: [] });
    assert.deepStrictEqual(checked, ['release/docs:rfcs/IGNITE-7-y.md']);
});

test('one confirmed RFC is enough when GitHub cannot check another link, in either order of the links', async () => {
    const onTarget = () => true;
    const atRef = async () => { throw Object.assign(new Error('Server Error'), { status: 502 }); };
    const local = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md';

    for (const other of ['rfcs/IGNITE-7-y.md', 'rfcs/IGNITE-8-z.md']) {
        const remote = `https://github.com/apache/ignite/blob/1a2b3c/${other}`;

        for (const body of [`${local} ${remote}`, `${remote} ${local}`]) {
            assert.deepStrictEqual(await rfcLinks(body, 'apache/ignite', 'master', onTarget, atRef),
                { linked: ['rfcs/IGNITE-7-y.md'], badLinks: [] }, body);
        }
    }

    const unchecked = 'https://github.com/apache/ignite/blob/1a2b3c/rfcs/IGNITE-8-z.md';

    await assert.rejects(rfcLinks(unchecked, 'apache/ignite', 'master', onTarget, atRef), /Server Error/);
});
