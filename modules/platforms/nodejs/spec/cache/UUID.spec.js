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

const TestingHelper = require('../TestingHelper');
const IgniteClient = require('apache-ignite-client');
const CacheConfiguration = IgniteClient.CacheConfiguration;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const ObjectType = IgniteClient.ObjectType;

const CACHE_NAME = '__test_cache';
const TABLE_NAME = '__test_UUID_table';
const UUID_STRINGS = [
    'd57babad-7bc1-4c82-9f9c-e72841b92a85',
    '5946c0c0-2b76-479d-8694-a2e64a3968da',
    'a521723d-ad5d-46a6-94ad-300f850ef704'
];

describe('uuid test suite >', () => {
    let igniteClient = null;

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await testSuiteCleanup(done);
                await igniteClient.getOrCreateCache(CACHE_NAME, new CacheConfiguration().setSqlSchema('PUBLIC'));
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
            catch(error => done.fail(error));
    }, TestingHelper.TIMEOUT);

    it('insert byte array and select string to check uuid marshalling', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME);
                const createTable = `CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (id INTEGER PRIMARY KEY, uuid_field UUID)`;
                (await cache.query(new SqlFieldsQuery(createTable))).getAll();

                const insertQuery = new SqlFieldsQuery(`INSERT INTO ${TABLE_NAME} (id, uuid_field) VALUES (?, ?)`);
                const dataTypes = [ObjectType.PRIMITIVE_TYPE.INTEGER, ObjectType.PRIMITIVE_TYPE.UUID];

                let id = 1;
                for (let uuidStr of UUID_STRINGS) {
                    const args = [id++, uuidToBytes(uuidStr)];
                    (await cache.query(insertQuery.setArgs(...args).setArgTypes(...dataTypes))).getAll();
                }

                const selectQuery = `SELECT * FROM ${TABLE_NAME} WHERE uuid_field = ?`;

                for (let uuidStr of UUID_STRINGS) {
                    const cursor = await cache.query(new SqlFieldsQuery(selectQuery).setArgs(uuidStr));
                    const rows = await cursor.getAll();
                    expect(rows.length).toBe(1);
                    expect(rows[0][1]).toEqual(uuidToBytes(uuidStr));
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    function uuidToBytes(uuidStr) {
        const buf = [];
        uuidStr.toLowerCase().replace(/[0-9a-f]{2}/g, function(oct) {
            buf.push(parseInt(oct, 16));
        });

        return buf;
    }

    async function testSuiteCleanup(done) {
        const cache = await igniteClient.getOrCreateCache(CACHE_NAME, new CacheConfiguration().setSqlSchema('PUBLIC'));
        const dropTable = `DROP TABLE IF EXISTS ${TABLE_NAME}`;
        (await cache.query(new SqlFieldsQuery(dropTable))).getAll();
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }
});
