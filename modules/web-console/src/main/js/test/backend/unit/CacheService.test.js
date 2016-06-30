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
import testSpaces from '../data/spaces.json';

let cacheService;
let mongo;
let errors;

suite('SpaceService', () => {

    const injectSpaceInCaches = (spaceId) => {
        return testCaches.map((cache) => ({...cache, space: spaceId}));
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
            mongo.Cache.remove().exec()
        ]);
    });

    test('Cache merge', (done) => {
        return cacheService.merge(testCaches[0])
            .then((cacheId) => {
                assert.isNotNull(cacheId);

                return cacheId;
            })
            .then((cacheId) => {
                return mongo.Cache.findById(cacheId)
                    .then((cache) => {
                        assert.isNotNull(cache);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('double save same cache', (done) => {
        return cacheService.merge(testCaches[0])
            .then(() => cacheService.merge(testCaches[0]))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove cache', (done) => {
        return cacheService.merge(testCaches[0])
            .then((cacheId) => {

                assert.isNotNull(cacheId);

                return mongo.Cache.findById(cacheId)
                    .then((cache) => {
                        assert.isNotNull(cache);

                        return cache;
                    })
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

    // TODO implement test
    test('Remove all caches by user', (done) => {
        done();
    });

    // TODO Implement test
    test('Load all caches by space', (done) => {
        done();
    });
});
