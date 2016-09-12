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


import {assert} from 'chai';
import injector from '../injector';
import testCaches from '../data/caches.json';
import testAccounts from '../data/accounts.json';

let cacheService;
let mongo;
let errors;

suite('CacheServiceTestsSuite', () => {
    const prepareUserSpaces = () => {
        return mongo.Account.create(testAccounts)
            .then((accounts) => {
                return Promise.all(
                    accounts.map((account) => mongo.Space.create(
                        [
                            {name: 'Personal space', owner: account._id, demo: false},
                            {name: 'Demo space', owner: account._id, demo: true}
                        ]
                    )))
                    .then((spaces) => [accounts, spaces]);
            });
    };

    suiteSetup(() => {
        return Promise.all([injector('services/caches'),
            injector('mongo'),
            injector('errors')])
            .then(([_cacheService, _mongo, _errors]) => {
                mongo = _mongo;
                cacheService = _cacheService;
                errors = _errors;
            });
    });

    setup(() => {
        return Promise.all([
            mongo.Cache.remove().exec(),
            mongo.Account.remove().exec(),
            mongo.Space.remove().exec()
        ]);
    });

    test('Create new cache', (done) => {
        cacheService.merge(testCaches[0])
            .then((cache) => {
                assert.isNotNull(cache._id);

                return cache._id;
            })
            .then((cacheId) => mongo.Cache.findById(cacheId))
            .then((cache) => {
                assert.isNotNull(cache);
            })
            .then(done)
            .catch(done);
    });

    test('Update existed cache', (done) => {
        const newName = 'NewUniqueName';

        cacheService.merge(testCaches[0])
            .then((cache) => {
                const cacheBeforeMerge = {...testCaches[0], _id: cache._id, name: newName};

                return cacheService.merge(cacheBeforeMerge);
            })
            .then((cache) => mongo.Cache.findById(cache._id))
            .then((cacheAfterMerge) => {
                assert.equal(cacheAfterMerge.name, newName);
            })
            .then(done)
            .catch(done);
    });

    test('Create duplicated cache', (done) => {
        cacheService.merge(testCaches[0])
            .then(() => cacheService.merge(testCaches[0]))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed cache', (done) => {
        cacheService.merge(testCaches[0])
            .then((createdCache) => {
                return mongo.Cache.findById(createdCache._id)
                    .then((foundCache) => foundCache._id)
                    .then(cacheService.remove)
                    .then(({rowsAffected}) => {
                        assert.equal(rowsAffected, 1);
                    })
                    .then(() => mongo.Cache.findById(createdCache._id))
                    .then((notFoundCache) => {
                        assert.isNull(notFoundCache);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Remove cache without identifier', (done) => {
        cacheService.merge(testCaches[0])
            .then(() => cacheService.remove())
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed cache', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        cacheService.merge(testCaches[0])
            .then(() => cacheService.remove(validNoExistingId))
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 0);
            })
            .then(done)
            .catch(done);
    });

    test('Remove all caches in space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const currentUser = accounts[0];
                const userCache = {...testCaches[0], space: spaces[0][0]._id};

                return cacheService.merge(userCache)
                    .then(() => cacheService.removeAll(currentUser._id, false));
            })
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 1);
            })
            .then(done)
            .catch(done);
    });

    test('Get all caches by space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const userCache = {...testCaches[0], space: spaces[0][0]._id};

                return cacheService.merge(userCache)
                    .then((cache) => {
                        return cacheService.listBySpaces(spaces[0][0]._id)
                            .then((caches) => {
                                assert.equal(caches.length, 1);
                                assert.equal(caches[0]._id.toString(), cache._id.toString());
                            });
                    });
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
