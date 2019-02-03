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
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const ObjectType = IgniteClient.ObjectType;
const CacheConfiguration = IgniteClient.CacheConfiguration;

const CACHE_NAME = '__test_cache';
const TABLE_NAME = '__test_SqlFieldsQuery_table';
const ELEMENTS_NUMBER = 10;

describe('sql fields query test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await testSuiteCleanup(done);
                await igniteClient.getOrCreateCache(CACHE_NAME, new CacheConfiguration().setSqlSchema('PUBLIC'));
                await generateData(done);
            }).
            then(done).
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    afterAll((done) => {
        Promise.resolve().
            then(async () => {
                await dropTables(done);
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
                const cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`));
                const set = new Set();
                for (let fields of await cursor.getAll()) {
                    expect(fields.length).toBe(2);
                    expect(generateValue(fields[0]) === fields[1]).toBe(true);
                    set.add(fields[0]);
                    expect(fields[0] >= 0 && fields[0] < ELEMENTS_NUMBER).toBe(true);
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
                const cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`).setPageSize(1));
                const set = new Set();
                for (let fields of await cursor.getAll()) {
                    expect(fields.length).toBe(2);
                    expect(generateValue(fields[0]) === fields[1]).toBe(true);
                    set.add(fields[0]);
                    expect(fields[0] >= 0 && fields[0] < ELEMENTS_NUMBER).toBe(true);
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
                const cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`));
                const set = new Set();
                do {
                    let fields = await cursor.getValue();
                    expect(fields.length).toBe(2);
                    expect(generateValue(fields[0]) === fields[1]).toBe(true);
                    set.add(fields[0]);
                    expect(fields[0] >= 0 && fields[0] < ELEMENTS_NUMBER).toBe(true);
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
                const cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`).setPageSize(2));
                const set = new Set();
                do {
                    let fields = await cursor.getValue();
                    expect(fields.length).toBe(2);
                    expect(generateValue(fields[0]) === fields[1]).toBe(true);
                    set.add(fields[0]);
                    expect(fields[0] >= 0 && fields[0] < ELEMENTS_NUMBER).toBe(true);
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
                const cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`).setPageSize(1));
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
                const cursor = await cache.query(new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`));
                await cursor.getAll();
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('sql fields query settings', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                const cursor = await cache.query(new SqlFieldsQuery(`SELECT * FROM ${TABLE_NAME}`).
                    setPageSize(2).
                    setLocal(false).
                    setSql(`INSERT INTO ${TABLE_NAME} (field1, field2) VALUES (?, ?)`).
                    setArgTypes(ObjectType.PRIMITIVE_TYPE.INTEGER, ObjectType.PRIMITIVE_TYPE.STRING).
                    setArgs(50, 'test').
                    setDistributedJoins(true).
                    setReplicatedOnly(false).
                    setTimeout(10000).
                    setSchema('PUBLIC').
                    setMaxRows(20).
                    setStatementType(SqlFieldsQuery.STATEMENT_TYPE.ANY).
                    setEnforceJoinOrder(true).
                    setCollocated(false).
                    setLazy(true).
                    setIncludeFieldNames(true));
                await cursor.getAll();
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('get empty results', (done) => {
        Promise.resolve().
            then(async () => {
                let cache = igniteClient.getCache(CACHE_NAME);
                await cache.removeAll();
                let cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT field1 FROM ${TABLE_NAME} WHERE field1 > 100`));
                const cacheEntries = await cursor.getAll();
                expect(cacheEntries.length).toBe(0);
                await cursor.close();

                cursor = await cache.query(
                    new SqlFieldsQuery(`SELECT field1 FROM ${TABLE_NAME} WHERE field1 > 100`));
                expect(await cursor.getValue()).toBe(null);
                expect(cursor.hasMore()).toBe(false);
                await cursor.close();
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function dropTables(done) {
        try {
            let cache = igniteClient.getCache(CACHE_NAME);
            (await cache.query(new SqlFieldsQuery(`DROP TABLE ${TABLE_NAME}`))).getAll();
        }
        catch (err) {
            done.fail('unexpected error: ' + err);
        }
    }

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }

    async function generateData(done) {
        try {
            let cache = igniteClient.getCache(CACHE_NAME);
            (await cache.query(new SqlFieldsQuery(
                `CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (field1 INT, field2 VARCHAR, PRIMARY KEY (field1))`))).getAll();

            const insertQuery = new SqlFieldsQuery(`INSERT INTO ${TABLE_NAME} (field1, field2) VALUES (?, ?)`).
                setArgTypes(ObjectType.PRIMITIVE_TYPE.INTEGER);

            for (let i = 0; i < ELEMENTS_NUMBER; i++) {
                (await cache.query(insertQuery.setArgs(i, generateValue(i)))).getAll();
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
