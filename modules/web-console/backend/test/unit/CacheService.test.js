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

const assert = require('chai').assert;
const injector = require('../injector');
const testCaches = require('../data/caches.json');
const testAccounts = require('../data/accounts.json');
const testSpaces = require('../data/spaces.json');

let cacheService;
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
                cacheService = _cacheService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Create new cache', (done) => {
        const dupleCache = Object.assign({}, testCaches[0], {name: 'Other name'});

        delete dupleCache._id;

        cacheService.merge(dupleCache)
            .then((cache) => mongo.Cache.findById(cache._id))
            .then((cache) => assert.isNotNull(cache))
            .then(done)
            .catch(done);
    });

    test('Update existed cache', (done) => {
        const newName = 'NewUniqueName';

        const cacheBeforeMerge = Object.assign({}, testCaches[0], {name: newName});

        cacheService.merge(cacheBeforeMerge)
            .then((cache) => mongo.Cache.findById(cache._id))
            .then((cacheAfterMerge) => assert.equal(cacheAfterMerge.name, newName))
            .then(done)
            .catch(done);
    });

    test('Create duplicated cache', (done) => {
        const dupleCache = Object.assign({}, testCaches[0]);

        delete dupleCache._id;

        cacheService.merge(dupleCache)
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed cache', (done) => {
        cacheService.remove(testCaches[0]._id)
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
        cacheService.remove()
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed cache', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        cacheService.remove(validNoExistingId)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 0)
            )
            .then(done)
            .catch(done);
    });

    test('Get all caches by space', (done) => {
        cacheService.listBySpaces(testSpaces[0]._id)
            .then((caches) =>
                assert.equal(caches.length, 5)
            )
            .then(done)
            .catch(done);
    });

    test('Remove all caches in space', (done) => {
        cacheService.removeAll(testAccounts[0]._id, false)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 5)
            )
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
