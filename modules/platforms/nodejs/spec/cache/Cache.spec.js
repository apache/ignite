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
const CacheConfiguration = IgniteClient.CacheConfiguration;
const QueryEntity = IgniteClient.QueryEntity;
const QueryField = IgniteClient.QueryField;
const QueryIndex = IgniteClient.QueryIndex;

const CACHE_NAME = '__test_cache';
const CACHE_NAME2 = '__test_cache2';
const CACHE_NAME3 = '__test_cache3';

describe('cache configuration operations test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await testSuiteCleanup(done);
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    afterAll((done) => {
        Promise.resolve().
            then(async () => {
                await testSuiteCleanup(done);
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
                    let cache = await igniteClient.getOrCreateCache(CACHE_NAME);
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

    it('create cache with configuration', (done) => {
        Promise.resolve().
            then(async () => {
                const cacheCfg = new CacheConfiguration().
                    setQueryEntities(
                        new QueryEntity().
                            setKeyTypeName('INT').
                            setValueTypeName('Person').
                            setTableName('Person').
                            setKeyFieldName('id').
                            setValueFieldName('salary').
                            setFields([
                                new QueryField('id', 'INT').
                                    setIsKeyField(true),
                                new QueryField('firstName', 'VARCHAR').
                                    setIsNotNull(true),
                                new QueryField('lastName', 'VARCHAR').
                                    setDefaultValue('lastName'),
                                new QueryField('salary', 'DOUBLE').
                                    setPrecision(10).
                                    setScale(10)
                            ]).
                            setAliases(new Map([['id', 'id'], ['firstName', 'firstName']])).
                            setIndexes([
                                new QueryIndex('id_idx', QueryIndex.INDEX_TYPE.SORTED).
                                    setName('id_idx').
                                    setType(QueryIndex.INDEX_TYPE.SORTED).
                                    setInlineSize(10).
                                    setFields(new Map([['id', true], ['firstName', false]]))
                            ]));
                let cache = await igniteClient.createCache(CACHE_NAME3, cacheCfg);
                let cfg = await igniteClient.getCacheConfiguration(CACHE_NAME3);
                await igniteClient.destroyCache(CACHE_NAME3);

                cache = await igniteClient.getOrCreateCache(CACHE_NAME3, cfg);
                let cfg2 = await igniteClient.getCacheConfiguration(CACHE_NAME3);
                await igniteClient.destroyCache(CACHE_NAME3);
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

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
        await TestingHelper.destroyCache(CACHE_NAME2, done);
        await TestingHelper.destroyCache(CACHE_NAME3, done);
    }

    async function obtainCacheWithWrongName(method, done) {
        const wrongNames = [undefined, null, ''];
        for (let name of wrongNames) {
            let cache;
            try {
                cache = await method(name);
            }
            catch (err) {
                TestingHelper.checkIllegalArgumentError(err, done);
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
                TestingHelper.checkIllegalArgumentError(err, done);
            }
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
