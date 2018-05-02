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

const Util = require('util');
const IgniteClient = require('apache-ignite-client');
const ObjectType = IgniteClient.ObjectType;
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const CacheConfiguration = IgniteClient.CacheConfiguration;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;

const ENDPOINT = '127.0.0.1:10800';

const PERSON_CACHE_NAME = 'SqlFieldsQueryExample_person';

// This example demonstrates basic SQL Fields query operations.
// - connects to ENDPOINT node
// - creates a table (CREATE TABLE)
// - writes data into the table (INSERT INTO table)
// - reads data from the table (SELECT ...)
// - deletes the table (DROP TABLE)
class SqlFieldsQueryExample {
    constructor() {
        this._cache = null;
    }

    async start() {
        const igniteClient = new IgniteClient(this.onStateChanged.bind(this));
        try {
            await igniteClient.connect(new IgniteClientConfiguration(ENDPOINT));

            const cacheCfg = new CacheConfiguration().
                setSqlSchema('PUBLIC');
            this._cache = await igniteClient.getOrCreateCache(PERSON_CACHE_NAME, cacheCfg);

            (await this._cache.query(new SqlFieldsQuery(
               'CREATE TABLE Person (id INTEGER PRIMARY KEY, firstName VARCHAR, lastName VARCHAR, salary DOUBLE)'))).getAll();
            console.log("Database objects created");

            await this.generateData();

            const sqlFieldsCursor = await this._cache.query(
                new SqlFieldsQuery("SELECT concat(firstName, ' ', lastName) from Person").
                    setPageSize(1));

            console.log('Query results:');
            console.log(await sqlFieldsCursor.getValue());
            while (sqlFieldsCursor.hasMore()) {
                console.log(await sqlFieldsCursor.getValue());
            }

            (await this._cache.query(new SqlFieldsQuery("DROP TABLE Person"))).getAll();
            console.log("Database objects dropped");

            await igniteClient.destroyCache(PERSON_CACHE_NAME);
        }
        catch (err) {
            console.log(err.message);
        }
        finally {
            igniteClient.disconnect();
        }
    }

    async generateData() {
        const query = new SqlFieldsQuery('INSERT INTO Person (id, firstName, lastName, salary) values (?, ?, ?, ?)').
            setArgTypes(ObjectType.PRIMITIVE_TYPE.INTEGER);

        const persons = [
            ['John', 'Doe', 1000],
            ['Jane', 'Roe', 2000],
            ['Mary', 'Major', 1500],
            ['Richard', 'Miles', 800]
        ];

        for (let i = 0; i < persons.length; i++) {
            (await this._cache.query(query.setArgs(i, ...persons[i]))).getAll();
        }

        console.log('Data is generated');
    }

    onStateChanged(state, reason) {
        if (state === IgniteClient.STATE.CONNECTED) {
            this._connected = true;
            console.log('Client is started');
        }
        else if (state === IgniteClient.STATE.DISCONNECTED) {
            this._connected = false;
            console.log('Client is stopped');
            if (reason) {
                console.log(reason);
            }
        }
    }
}

const sqlFieldsQueryExample = new SqlFieldsQueryExample();
sqlFieldsQueryExample.start();
