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
const ComplexObjectType = IgniteClient.ComplexObjectType;
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const CacheConfiguration = IgniteClient.CacheConfiguration;
const QueryEntity = IgniteClient.QueryEntity;
const QueryField = IgniteClient.QueryField;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;
const SqlQuery = IgniteClient.SqlQuery;

const ENDPOINT = '127.0.0.1:10800';

const PERSON_CACHE_NAME = 'SqlQueryEntriesExample_person';

class Person {
    constructor(firstName = null, lastName = null, salary = null) {
        this.id = Person.generateId();
        this.firstName = firstName;
        this.lastName = lastName;
        this.salary = salary;
    }

    static generateId() {
        if (!Person.id) {
            Person.id = 0;
        }
        const id = Person.id;
        Person.id++;
        return id;
    }
}

// This example demonstrates basic Cache, Key-Value Queries and SQL Query operations:
// - connects to a node
// - creates a cache from CacheConfiguration, if it doesn't exist
// - writes data of primitive and Complex Object types into the cache using Key-Value put operation
// - reads data from the cache using SQL Query
// - destroys the cache
class SqlQueryEntriesExample {
    constructor() {
        this._cache = null;
    }

    async start() {
        const igniteClient = new IgniteClient(this.onStateChanged.bind(this));
        try {
            await igniteClient.connect(new IgniteClientConfiguration(ENDPOINT));

            const cacheCfg = new CacheConfiguration().
                setQueryEntities(
                    new QueryEntity().
                        setValueTypeName('Person').
                        setFields([
                            new QueryField('id', 'java.lang.Integer'),
                            new QueryField('firstName', 'java.lang.String'),
                            new QueryField('lastName', 'java.lang.String'),
                            new QueryField('salary', 'java.lang.Double')
                        ]));
            this._cache = (await igniteClient.getOrCreateCache(PERSON_CACHE_NAME, cacheCfg)).
                setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER).
                setValueType(new ComplexObjectType(new Person()).
                    setFieldType('id', ObjectType.PRIMITIVE_TYPE.INTEGER));

            await this.generateData();

            const sqlCursor = await this._cache.query(
                new SqlQuery('Person', 'salary > ? and salary <= ?').
                    setArgs(900, 1600));

            console.log('SqlQuery results (salary between 900 and 1600):');
            let person;
            do {
                person = (await sqlCursor.getValue()).getValue();
                console.log(Util.format('  name: %s %s, salary: %d',
                    person.firstName, person.lastName, person.salary));
            } while (sqlCursor.hasMore());

            await igniteClient.destroyCache(PERSON_CACHE_NAME);
        }
        catch (err) {
            console.log('ERROR: ' + err.message);
        }
        finally {
            igniteClient.disconnect();
        }
    }

    async generateData() {
        const persons = [
            ['John', 'Doe', 1000],
            ['Jane', 'Roe', 2000],
            ['Mary', 'Major', 1500],
            ['Richard', 'Miles', 800]
        ];

        for (let data of persons) {
            let person = new Person(...data);
            await this._cache.put(person.id, person);
        }

        console.log('Data is generated');
    }

    onStateChanged(state, reason) {
        if (state === IgniteClient.STATE.CONNECTED) {
            console.log('Client is started');
        }
        else if (state === IgniteClient.STATE.DISCONNECTED) {
            console.log('Client is stopped');
            if (reason) {
                console.log(reason);
            }
        }
    }
}

const sqlQueryEntriesExample = new SqlQueryEntriesExample();
sqlQueryEntriesExample.start();
