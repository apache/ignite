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

let cacheService, mongo, errors;

suite('CacheService', () => {
    suiteSetup(() => {
        return Promise.all([fireUp('services/cacheService'), fireUp('mongo'), fireUp('errors')])
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
            .then((cacheId)=> {
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

    test('double save same cache', (done) => {
        return cacheService.merge(testCaches[0])
            .then(() => cacheService.merge(testCaches[0]))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done()
            })
    });

    test('Remove cache', (done)=> {
        return cacheService.merge(testCaches[0])
            .then((cacheId)=> {

                assert.isNotNull(cacheId);

                return mongo.Cache.findById(cacheId)
                    .then((cache)=> {
                        assert.isNotNull(cache);

                        return cache;
                    })
                    .then(cacheService.remove)
                    .then(() => {
                        return mongo.Cache.findById(cacheId)
                            .then((notFoundCache) => {
                                assert.isNull(notFoundCache);
                            })
                    })
            })
            .then(done)
            .catch(done);
    });

    test('Remove all caches for user', (done) => {
        return Promise.all([cacheService.merge(testCaches[0]), cacheService.merge(testCaches[1])])
            .then((results) => {

            })
            .then(done)
            .catch(done);
    });

});