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
import fireUp from '../injector';
import testCaches from '../data/caches.json';
import testAccounts from '../data/accounts.json';

let cacheService;
let mongo;
let errors;

suite('CacheService', () => {

    const prepareUserSpaces = () => {
        return mongo.Account.create(testAccounts)
            .then((accounts) => {
                return Promise.all(accounts.map((account)=> {
                    return mongo.Space.create([
                        {name: 'Personal space', owner: account._id, demo: false},
                        {name: 'Demo space', owner: account._id, demo: true}
                    ])
                }))
                    .then((spaces)=> {
                        return [accounts, spaces];
                    });
            });
    };

    suiteSetup(() => {
        return Promise.all([fireUp('services/cache'), fireUp('mongo'), fireUp('errors')])
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

    test('Cache merge.', (done) => {
        cacheService.merge(testCaches[0])
            .then((cacheId) => {
                assert.isNotNull(cacheId);

                return cacheId;
            })
            .then((cacheId) => {
                return mongo.Cache.findById(cacheId)
                    .then((cache) => {
                        assert.isNotNull(cache);
                    })
            })
            .then(done)
            .catch(done);
    });

    test('Try to save same cache twice.', (done) => {
        cacheService.merge(testCaches[0])
            .then(() => cacheService.merge(testCaches[0]))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);
                done();
            });
    });

    test('Try to update cache.', (done) => {
        cacheService.merge(testCaches[0])
            .then((cacheId) => {
                const cacheBeforeMerge = {...testCaches[0], _id: cacheId};
                cacheBeforeMerge.name = 'NewUniqueName';

                return cacheService.merge(cacheBeforeMerge)
                    .then((cacheId) => {
                        return mongo.Cache.findById(cacheId)
                            .then((cacheAfterMerge) => {
                                assert.equal(cacheBeforeMerge.name, cacheAfterMerge.name);
                            })
                            .then(done);
                    });
            })
            .catch(done);
    });


    test('Remove cache.', (done) => {
        cacheService.merge(testCaches[0])
            .then((cacheId) => {
                return mongo.Cache.findById(cacheId)
                    .then((cache) => cache._id)
                    .then(cacheService.remove)
                    .then((results) => {
                        assert.equal(results.rowsAffected, 1);

                        return mongo.Cache.findById(cacheId)
                            .then((notFoundCache) => {
                                assert.isNull(notFoundCache);
                            });
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Remove null cache must be throw exception.', (done) => {
        cacheService.merge(testCaches[0])
            .then(()=> cacheService.remove())
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);
                done();
            })
    });


    test('Remove all caches by user.', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const currentUser = accounts[0];
                const userCache = {...testCaches[0], space: spaces[0][0]._id};

                return cacheService.merge(userCache)
                    .then(() => {
                        return cacheService.removeAll(currentUser._id, false)
                            .then(({rowsAffected})=> {
                                assert.equal(rowsAffected, 1);
                            })
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Load all caches by space.', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const currentUser = accounts[0];
                const userCache = {...testCaches[0], space: spaces[0][0]._id};

                return cacheService.merge(userCache)
                    .then((cacheId) => {
                        return cacheService.listByUser(currentUser._id, false)
                            .then(({caches}) => {
                                assert.equal(caches.length, 1);
                                assert.equal(caches[0]._id.toString(), cacheId.toString());
                            });
                    });
            })
            .then(done)
            .catch(done);
    });

    // TODO
    // test('Test link entities on merge', () => {})

    // TODO
    // test('Test link entities on remove', () => {})
});
