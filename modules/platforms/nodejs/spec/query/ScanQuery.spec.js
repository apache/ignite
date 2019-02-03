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
const ScanQuery = IgniteClient.ScanQuery;
const ObjectType = IgniteClient.ObjectType;

const CACHE_NAME = '__test_cache';
const ELEMENTS_NUMBER = 10;

describe('scan query test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await testSuiteCleanup(done);
                await igniteClient.getOrCreateCache(CACHE_NAME);
                await generateData(done);
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

    it('get all', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery());
                const set = new Set();
                for (let cacheEntry of await cursor.getAll()) {
                    expect(generateValue(cacheEntry.getKey()) === cacheEntry.getValue()).toBe(true);
                    set.add(cacheEntry.getKey());
                    expect(cacheEntry.getKey() >= 0 && cacheEntry.getKey() < ELEMENTS_NUMBER).toBe(true);
                }
                expect(set.size).toBe(ELEMENTS_NUMBER);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get all with page size', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery().setPageSize(1));
                const set = new Set();
                for (let cacheEntry of await cursor.getAll()) {
                    expect(generateValue(cacheEntry.getKey()) === cacheEntry.getValue()).toBe(true);
                    set.add(cacheEntry.getKey());
                    expect(cacheEntry.getKey() >= 0 && cacheEntry.getKey() < ELEMENTS_NUMBER).toBe(true);
                }
                expect(set.size).toBe(ELEMENTS_NUMBER);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get value', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery());
                const set = new Set();
                do {
                    let cacheEntry = await cursor.getValue();
                    expect(generateValue(cacheEntry.getKey()) === cacheEntry.getValue()).toBe(true);
                    set.add(cacheEntry.getKey());
                    expect(cacheEntry.getKey() >= 0 && cacheEntry.getKey() < ELEMENTS_NUMBER).toBe(true);
                } while (cursor.hasMore());
                expect(set.size).toBe(ELEMENTS_NUMBER);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get value with page size', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery().setPageSize(2));
                const set = new Set();
                do {
                    let cacheEntry = await cursor.getValue();
                    expect(generateValue(cacheEntry.getKey()) === cacheEntry.getValue()).toBe(true);
                    set.add(cacheEntry.getKey());
                    expect(cacheEntry.getKey() >= 0 && cacheEntry.getKey() < ELEMENTS_NUMBER).toBe(true);
                } while (cursor.hasMore());
                expect(set.size).toBe(ELEMENTS_NUMBER);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('close cursor', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery().setPageSize(1));
                await cursor.getValue();
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('close cursor after get all', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery());
                await cursor.getAll();
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('scan query settings', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new ScanQuery().
                    setPartitionNumber(0).
                    setPageSize(2).
                    setLocal(true));
                await cursor.getAll();
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });


    it('scan empty cache', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                await cache.removeAll();
                let cursor = await cache.query(new ScanQuery());
                const cacheEntries = await cursor.getAll();
                expect(cacheEntries.length).toBe(0);
                await cursor.close();

                cursor = await cache.query(new ScanQuery());
                expect(await cursor.getValue()).toBe(null);
                expect(cursor.hasMore()).toBe(false);
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }

    async function generateData(done) {
        try {
            let cache = igniteClient.getCache(CACHE_NAME).
                setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);
            for (let i = 0; i < ELEMENTS_NUMBER; i++) {
                await cache.put(i, generateValue(i));
            }
        }
        catch (err) {
            done.fail('unexpected error: ' + err);
        }
    }

    function generateValue(key) {
        return 'value' + key;
    }
});
