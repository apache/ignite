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

require('jasmine-expect');

const config = require('../config');
const TestingHelper = require('../TestingHelper');
const IgniteClient = require('apache-ignite-client');
const Errors = IgniteClient.Errors;

const CACHE_NAME = '__test_cache';
const CACHE_NAME2 = '__test_cache2';

describe('cache configuration operations test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    afterAll((done) => {
        Promise.resolve().
            then(async () => {
                await igniteClient.destroyCache(CACHE_NAME);
                await igniteClient.destroyCache(CACHE_NAME2);
                await TestingHelper.cleanUp();
            }).
            then(done).
            catch(error => done());
    }, TestingHelper.TIMEOUT);

    it('create cache', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                await testCache(cache, false, done);
                cache = await igniteClient.createCache(CACHE_NAME);
                await testCache(cache, true, done);
                cache = igniteClient.getCache(CACHE_NAME);
                await testCache(cache, true, done);
                await igniteClient.destroyCache(CACHE_NAME);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('create cache twice', (done) => {
        Promise.resolve().
            then(async () => {
                try {
                    let cache = igniteClient.getOrCreateCache(CACHE_NAME);
                    cache = await igniteClient.createCache(CACHE_NAME);
                }
                catch (err) {
                    if (!(err instanceof Errors.OperationError)) {
                        done.fail('cache successully created twice');
                    }
                }
                finally {
                    await igniteClient.destroyCache(CACHE_NAME);
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get or create cache', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME2);
                await testCache(cache, false, done);
                cache = await igniteClient.getOrCreateCache(CACHE_NAME2);
                await testCache(cache, true, done);
                cache = igniteClient.getCache(CACHE_NAME2);
                await testCache(cache, true, done);
                await igniteClient.destroyCache(CACHE_NAME2);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get cache names', (done) => {
        Promise.resolve().
            then(async () => {
                await igniteClient.getOrCreateCache(CACHE_NAME);
                await igniteClient.getOrCreateCache(CACHE_NAME2);
                const cacheNames = await igniteClient.cacheNames();
                expect(cacheNames.includes(CACHE_NAME)).toBe(true);
                expect(cacheNames.includes(CACHE_NAME2)).toBe(true);
                await igniteClient.destroyCache(CACHE_NAME);
                await igniteClient.destroyCache(CACHE_NAME2);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('destroy cache', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = await igniteClient.getOrCreateCache(CACHE_NAME);
                await igniteClient.destroyCache(CACHE_NAME);
                try {
                    await igniteClient.destroyCache(CACHE_NAME);
                    done.fail('cache successfully deleted twice');
                }
                catch (err) {
                    if (!(err instanceof Errors.OperationError)) {
                        done.fail('unexpected error: ' + err);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('create cache with wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                const method = igniteClient.createCache.bind(igniteClient);
                obtainCacheWithWrongName(method, done);
                obtainCacheWithWrongConfig(method, done);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get or create cache with wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                const method = igniteClient.getOrCreateCache.bind(igniteClient);
                obtainCacheWithWrongName(method, done);
                obtainCacheWithWrongConfig(method, done);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get cache with wrong args', (done) => {
        Promise.resolve().
            then(async () => {
                const method = igniteClient.getCache.bind(igniteClient);
                obtainCacheWithWrongName(method, done);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function obtainCacheWithWrongName(method, done) {
        const wrongNames = [undefined, null, ''];
        for (let name of wrongNames) {
            let cache;
            try {
                cache = await method(name);
            }
            catch (err) {
                checkIllegalArgumentError(err, done);
            }
        }
    }

    async function obtainCacheWithWrongConfig(method, done) {
        const wrongConfigs = ['', new IgniteClient(), new Array()];
        for (let config of wrongConfigs) {
            let cache;
            try {
                cache = await method(CACHE_NAME, config);
            }
            catch (err) {
                checkIllegalArgumentError(err, done);
            }
        }
    }

    function checkIllegalArgumentError(error, done) {
        if (!(error instanceof Errors.IllegalArgumentError)) {
            done.fail('unexpected error: ' + error);
        }
    }

    async function testCache(cache, cacheExists, done) {
        try {
            await cache.put(0, 0);
            if (!cacheExists) {
                done.fail('operation with absent cache succeeded');
            }
        }
        catch (err) {
            if (!(err instanceof Errors.OperationError)) {
                done.fail('unexpected error: ' + err);
            }
        }
    }
});
