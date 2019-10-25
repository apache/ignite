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

const IgniteClient = require('apache-ignite-client');
const ObjectType = IgniteClient.ObjectType;
const ComplexObjectType = IgniteClient.ComplexObjectType;
const BinaryObject = IgniteClient.BinaryObject;
const CacheEntry = IgniteClient.CacheEntry;
const ScanQuery = IgniteClient.ScanQuery;
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

const ENDPOINT = '127.0.0.1:10800';

const CACHE_NAME = 'CachePutGetExample_person';
const PERSON_TYPE_NAME = 'Person';

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

// This example demonstrates basic Cache, Key-Value Queries and Scan Query operations:
// - connects to a node
// - creates a cache, if it doesn't exist
//   - specifies key type as Integer
// - executes different cache operations with Complex Objects and Binary Objects
//   - put several objects in parallel
//   - putAll
//   - get
//   - getAll
//   - ScanQuery
// - destroys the cache
class CachePutGetExample {

    constructor() {
        this._personCache = null;
        this._personObjectType = null;
        this._binaryObjectCache = null;
    }

    async start() {
        const igniteClient = new IgniteClient(this.onStateChanged.bind(this));
        try {
            await igniteClient.connect(new IgniteClientConfiguration(ENDPOINT));

            this._personObjectType = new ComplexObjectType(new Person(), PERSON_TYPE_NAME).
                setFieldType('id', ObjectType.PRIMITIVE_TYPE.INTEGER);

            this._personCache = (await igniteClient.getOrCreateCache(CACHE_NAME)).
                setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER).
                setValueType(this._personObjectType);

            this._binaryObjectCache = igniteClient.getCache(CACHE_NAME).
                setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);

            await this.putComplexObjects();
            await this.putAllBinaryObjects();

            await this.getAllComplexObjects();
            await this.getBinaryObjects();

            await this.scanQuery();

            await igniteClient.destroyCache(CACHE_NAME);
        }
        catch (err) {
            console.log('ERROR: ' + err.message);
        }
        finally {
            igniteClient.disconnect();
        }
    }

    async putComplexObjects() {
        const person1 = new Person('John', 'Doe', 1000);
        const person2 = new Person('Jane', 'Roe', 2000);

        // put multiple values in parallel
        await Promise.all([
            await this._personCache.put(person1.id, person1),
            await this._personCache.put(person2.id, person2)
        ]);

        console.log('Complex Objects put successfully');
    }

    async putAllBinaryObjects() {
        // create binary object from scratch
        const personBinaryObject1 = new BinaryObject(PERSON_TYPE_NAME).
            setField('id', Person.generateId(), ObjectType.PRIMITIVE_TYPE.INTEGER).
            setField('firstName', 'Mary').
            setField('lastName', 'Major').
            setField('salary', 1500);

        // create binary object from complex object
        const personBinaryObject2 = await BinaryObject.fromObject(
            new Person('Richard', 'Miles', 800), this._personObjectType);

        await this._binaryObjectCache.putAll([
            new CacheEntry(await personBinaryObject1.getField('id'), personBinaryObject1),
            new CacheEntry(await personBinaryObject2.getField('id'), personBinaryObject2)
        ]);

        console.log('Binary Objects put successfully using putAll()');
    }

    async getAllComplexObjects() {
        const persons = await this._personCache.getAll([2, 3]);
        console.log('Complex Objects getAll:');
        for (let person of persons) {
            this.printPersonObject(person.getValue());
        }
    }

    async getBinaryObjects() {
        const personBinaryObject = await this._binaryObjectCache.get(3);
        console.log('Binary Object get:');
        console.log('  ' + personBinaryObject.getTypeName());
        let fieldValue;
        for (let fieldName of personBinaryObject.getFieldNames()) {
            fieldValue = await personBinaryObject.getField(fieldName);
            this.printPersonField(fieldName, fieldValue);
        }
    }

    async scanQuery() {
        const cursor = await this._personCache.query(new ScanQuery());
        console.log('Scan query results:');
        for (let cacheEntry of await cursor.getAll()) {
            this.printPersonObject(cacheEntry.getValue());
        }
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

    printPersonObject(person) {
        console.log('  ' + PERSON_TYPE_NAME);
        for (let key in person) {
            this.printPersonField(key, person[key]);
        }
    }

    printPersonField(fieldName, fieldValue) {
        console.log('    ' + fieldName + ' : ' + fieldValue);
    }
}

const cachePutGetExample = new CachePutGetExample();
cachePutGetExample.start();
