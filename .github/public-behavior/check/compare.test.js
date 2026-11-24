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
const { line, constant, column, BASE } = require('./test-fixtures.js');

test('a declaration that only moved in the source is not a change', () => {
    assert.deepStrictEqual(compare(BASE, BASE.replace(/A\.java:1/g, 'B.java:7')), []);
});

test('added, removed and changed declarations are found', () => {
    const merge = BASE
        .replace('position 1, java.lang.String', 'position 2, java.lang.String')
        .replace('position 2, int', 'position 1, int')
        .replace(line('CLI', 'control.sh --cache list --groups', 'optional, boolean') + '\n', '')
        + '\n' + line('CLI', 'control.sh --cache list --seq', 'optional, boolean');

    assert.deepStrictEqual(compare(BASE, merge).map(f => [f.key, f.before, f.after, f.kind]), [
        ['control.sh --cache list --groups', 'optional, boolean', null, 'contract'],
        ['control.sh --cache list --seq', null, 'optional, boolean', 'contract'],
        ['system view CacheView column cacheId', 'position 2, int', 'position 1, int', 'contract'],
        ['system view CacheView column cacheName', 'position 1, java.lang.String', 'position 2, java.lang.String', 'contract'],
    ]);
});

test('renaming an internal constant or a view row class is not a change', () => {
    const base = [constant('REST', 'command CACHE_GET', 'get'), BASE].join('\n');
    const merge = [constant('REST', 'command CACHE_GET_VALUE', 'get'), BASE.replace(/CacheView/g, 'CachesView')].join('\n');

    assert.deepStrictEqual(compare(base, merge), []);
});

test('two operations that swap their codes are reported, and so is a renamed view with a moved column', () => {
    const base = [constant('Thin client protocol', 'operation OP_GET', '1000'), constant('Thin client protocol', 'operation OP_PUT', '1001'),
        BASE].join('\n');
    const merge = [constant('Thin client protocol', 'operation OP_GET', '1001'), constant('Thin client protocol', 'operation OP_PUT', '1000'),
        BASE.replace(/CacheView/g, 'CachesView').replace('position 2, int', 'position 3, int')].join('\n');

    assert.deepStrictEqual(compare(base, merge).map(f => [f.key, f.before, f.after]), [
        ['system view CachesView column cacheId', null, 'position 3, int'],
        ['system view CachesView column cacheName', null, 'position 1, java.lang.String'],
        ['system view CacheView column cacheId', 'position 2, int', null],
        ['system view CacheView column cacheName', 'position 1, java.lang.String', null],
        ['operation OP_GET', '1000', '1001'],
        ['operation OP_PUT', '1001', '1000'],
    ]);
});

test('a removed operation and a new error code with the same value are both reported', () => {
    const base = constant('Thin client protocol', 'operation OP_CACHE_GET_ALL', '1003');
    const merge = constant('Thin client protocol', 'status CACHE_LOCKED', '1003');

    assert.deepStrictEqual(compare(base, merge).map(f => [f.key, f.before, f.after]), [
        ['operation OP_CACHE_GET_ALL', '1003', null],
        ['status CACHE_LOCKED', null, '1003'],
    ]);
});

test('a code renamed in one group is a rename when another group has a code with the old name', () => {
    const server = constant('Thin client protocol', 'server feature EXECUTE_TASK_BY_NAME', '1');
    const client = constant('Thin client protocol', 'Java client feature EXECUTE_TASK_BY_NAME', '1');
    const renamed = constant('Thin client protocol', 'server feature EXECUTE_TASK', '1');

    assert.deepStrictEqual(compare([server, client].join('\n'), [renamed, client].join('\n')), []);
});

test('removing a deprecated declaration and changing experimental API are left to the reviewer', () => {
    const merge = BASE
        .replace(line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated') + '\n', '')
        .replace(line('Java API', 'org.apache.ignite.Exp#run()', 'public void run()', 'experimental'),
            line('Java API', 'org.apache.ignite.Exp#run()', 'public int run()', 'experimental'))
        + '\n' + line('Java API', 'org.apache.ignite.Exp#stop()', 'public void stop()', 'experimental');

    assert.deepStrictEqual(compare(BASE, merge).map(f => [f.key, f.kind]), [
        ['org.apache.ignite.Exp#run()', 'experimental'],
        ['org.apache.ignite.Exp#stop()', 'experimental'],
        ['org.apache.ignite.Old#run()', 'deprecated-removal'],
    ]);
});

test('deprecating a declaration, changing a deprecated one or marking stable API experimental needs an RFC', () => {
    const base = line('Java API', 'org.apache.ignite.A#run()', 'public void run()');
    const deprecated = line('Java API', 'org.apache.ignite.A#run()', '@Deprecated public void run()', 'deprecated');
    const changedDeprecated = line('Java API', 'org.apache.ignite.A#run()', '@Deprecated public int run()', 'deprecated');
    const experimental = line('Java API', 'org.apache.ignite.A#run()', '@IgniteExperimental public void run()', 'experimental');

    assert.strictEqual(compare(base, deprecated)[0].kind, 'contract');
    assert.strictEqual(compare(deprecated, changedDeprecated)[0].kind, 'contract');
    assert.strictEqual(compare(base, experimental)[0].kind, 'contract');
});

test('a changed deprecated parameter points at its declaration, a removed one at the deprecation of its command', () => {
    const decl = value => line('CLI', 'control.sh --deactivate --force', value, 'deprecated', 'DeactivateCommandArg.java:33', '',
        'DeactivateCommand.java:33');
    const location = merge => compare(decl('optional, boolean'), merge).map(f => [f.kind, f.location]);

    assert.deepStrictEqual(location(decl('required, boolean')), [['contract', 'DeactivateCommandArg.java:33']]);
    assert.deepStrictEqual(location(''), [['deprecated-removal', 'DeactivateCommand.java:33']]);
});

test('values are compared as written: an escaped space or bar is a different value', () => {
    const value = v => line('REST', 'command CACHE_GET', v);

    assert.strictEqual(compare(value('get'), value('get\\s')).length, 1);
    assert.strictEqual(compare(value('a \\| b'), value('a | b')).length, 1);
});

test('a description from a version of the check with fewer columns is read with the missing columns empty', () => {
    const older = BASE.split('\n').map(l => l.split('\t').slice(0, 3).join('\t')).join('\n');

    assert.deepStrictEqual(compare(older, BASE), []);
});

test('a layout added, removed or changed only in unresolved items is a possible change, any other change of it is certain', () => {
    const layout = items => line('JDBC protocol', 'layout JdbcQuery.writeBinary', items.join('; '), 'itemized', 'A.java:1', 'JdbcQuery');
    const base = ['writeString', '#if:ctx.isSupported()', 'writeInt', '#end', '#loop', '?call:writeObject', '#end'];
    const kind = after => compare(layout(base), layout(after)).map(f => f.kind);

    assert.deepStrictEqual(kind(base.map(i => i === '#if:ctx.isSupported()' ? '#if:context.isSupported()' : i)), ['possible']);
    assert.deepStrictEqual(kind(base.map(i => i === '?call:writeObject' ? 'writeJdbcObject' : i)), ['possible']);
    assert.deepStrictEqual(kind(base.map(i => i === 'writeInt' ? 'writeLong' : i)), ['contract']);
    assert.deepStrictEqual(kind([...base.map(i => i === '?call:writeObject' ? 'writeJdbcObject' : i), 'writeBoolean']), ['contract']);
    assert.deepStrictEqual(compare(layout(['writeInt']), layout(['writeLong'])).map(f => f.kind), ['contract']);
    assert.deepStrictEqual(kind(['writeString', '#if:ctx.isSupported()', 'writeInt', '#end', '#else', '#loop', '?call:writeObject', '#end', '#end']),
        ['possible']);
    assert.deepStrictEqual(kind([...base, '#if:ctx.isTxSupported()', 'writeBoolean', '#end']), ['contract']);
    assert.deepStrictEqual(compare(layout(base), '').map(f => f.kind), ['possible']);
    assert.deepStrictEqual(compare('', layout(base)).map(f => f.kind), ['possible']);
    assert.deepStrictEqual(compare(line('JDBC protocol', 'k', 'a; ?b'), line('JDBC protocol', 'k', 'c; ?b')).map(f => f.kind), ['contract']);
});
