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
const { compare } = require('./compare.js');
const { line, property, BASE } = require('./test-fixtures.js');

test('a declaration that only moved in the source is not a change', () => {
    assert.deepStrictEqual(compare(BASE, BASE.replace(/A\.java:1/g, 'B.java:7')), []);
});

test('added, removed and changed declarations are found, each at its location in the PR or, if removed, on the target branch', () => {
    const merge = (BASE
        .replace(property('IGNITE_QUIET', 'Boolean, default true'), property('IGNITE_QUIET', 'Boolean, default false'))
        .replace(property('IGNITE_UPDATE_NOTIFIER', 'Boolean, default true') + '\n', '')
        + '\n' + property('IGNITE_NEW', 'Long, default 10')).replace(/A\.java:1/g, 'B.java:7');

    assert.deepStrictEqual(compare(BASE, merge).map(f => [f.key, f.before, f.after, f.kind, f.location]), [
        ['system property IGNITE_NEW', null, 'Long, default 10', 'contract', 'B.java:7'],
        ['system property IGNITE_QUIET', 'Boolean, default true', 'Boolean, default false', 'contract', 'B.java:7'],
        ['system property IGNITE_UPDATE_NOTIFIER', 'Boolean, default true', null, 'contract', 'A.java:1'],
    ]);
});

test('a removed deprecated declaration is left to the reviewer', () => {
    const merge = BASE.replace(/A\.java:1/g, 'B.java:7').replace(/\n.*IGNITE_EVICTION_PERMITS.*/, '');

    assert.deepStrictEqual(compare(BASE, merge).map(f => [f.key, f.kind, f.location]),
        [['system property IGNITE_EVICTION_PERMITS', 'deprecated-removal', 'A.java:1']]);
});

test('deprecating a declaration or changing a deprecated one needs an RFC', () => {
    const deprecated = property('IGNITE_QUIET', 'Boolean, default true, deprecated', 'deprecated');
    const changed = property('IGNITE_QUIET', 'Boolean, default false, deprecated', 'deprecated');

    assert.strictEqual(compare(property('IGNITE_QUIET', 'Boolean, default true'), deprecated)[0].kind, 'contract');
    assert.strictEqual(compare(deprecated, changed)[0].kind, 'contract');
});

test('values are compared as written: an escaped space or bar is a different value', () => {
    const value = v => property('IGNITE_QUIET', v);

    assert.strictEqual(compare(value('Boolean'), value('Boolean\\s')).length, 1);
    assert.strictEqual(compare(value('String, default a \\| b'), value('String, default a | b')).length, 1);
});

test('a description with fewer columns, from an older version of the check, is read with the missing columns empty', () => {
    const older = BASE.split('\n').map(l => l.split('\t').slice(0, 3).join('\t')).join('\n');

    assert.deepStrictEqual(compare(older, BASE), []);
    assert.deepStrictEqual(compare(line('Configuration', 'k', 'v').split('\t').slice(0, 3).join('\t'), '').map(f => [f.kind, f.location]),
        [['contract', '']]);
});
