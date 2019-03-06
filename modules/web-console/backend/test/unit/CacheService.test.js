/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

const assert = require('chai').assert;
const injector = require('../injector');
const testCaches = require('../data/caches.json');
const testAccounts = require('../data/accounts.json');
const testSpaces = require('../data/spaces.json');

let cachesService;
let mongo;
let errors;
let db;

suite('CacheServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/caches'),
            injector('mongo'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_cacheService, _mongo, _errors, _db]) => {
                mongo = _mongo;
                cachesService = _cacheService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Get cache', (done) => {
        const _id = testCaches[0]._id;

        cachesService.get(testCaches[0].space, false, _id)
            .then((cache) => {
                assert.isNotNull(cache);
                assert.equal(cache._id, _id);
            })
            .then(done)
            .catch(done);
    });

    test('Create new cache', (done) => {
        const dupleCache = Object.assign({}, testCaches[0], {name: 'Other name'});

        delete dupleCache._id;

        cachesService.merge(dupleCache)
            .then((cache) => mongo.Cache.findById(cache._id))
            .then((cache) => assert.isNotNull(cache))
            .then(done)
            .catch(done);
    });

    test('Update existed cache', (done) => {
        const newName = 'NewUniqueName';

        const cacheBeforeMerge = Object.assign({}, testCaches[0], {name: newName});

        cachesService.merge(cacheBeforeMerge)
            .then((cache) => mongo.Cache.findById(cache._id))
            .then((cacheAfterMerge) => assert.equal(cacheAfterMerge.name, newName))
            .then(done)
            .catch(done);
    });

    test('Create duplicated cache', (done) => {
        const dupleCache = Object.assign({}, testCaches[0]);

        delete dupleCache._id;

        cachesService.merge(dupleCache)
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed cache', (done) => {
        cachesService.remove(testCaches[0]._id)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 1)
            )
            .then(() => mongo.Cache.findById(testCaches[0]._id))
            .then((notFoundCache) =>
                assert.isNull(notFoundCache)
            )
            .then(done)
            .catch(done);
    });

    test('Remove cache without identifier', (done) => {
        cachesService.remove()
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed cache', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        cachesService.remove(validNoExistingId)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 0)
            )
            .then(done)
            .catch(done);
    });

    test('Get all caches by space', (done) => {
        cachesService.listBySpaces(testSpaces[0]._id)
            .then((caches) =>
                assert.equal(caches.length, 7)
            )
            .then(done)
            .catch(done);
    });

    test('Remove all caches in space', (done) => {
        cachesService.removeAll(testAccounts[0]._id, false)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 7)
            )
            .then(done)
            .catch(done);
    });

    test('List of all caches in cluster', (done) => {
        cachesService.shortList(testAccounts[0]._id, false, testCaches[0].clusters[0])
            .then((caches) => {
                assert.equal(caches.length, 2);
                assert.isNotNull(caches[0]._id);
                assert.isNotNull(caches[0].name);
                assert.isNotNull(caches[0].cacheMode);
                assert.isNotNull(caches[0].atomicityMode);
            })
            .then(done)
            .catch(done);
    });

    test('Update linked entities on update cache', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove cache', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove all caches in space', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });
});
