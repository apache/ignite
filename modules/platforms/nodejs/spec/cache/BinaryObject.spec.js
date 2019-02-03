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

const Util = require('util');
const TestingHelper = require('../TestingHelper');
const IgniteClient = require('apache-ignite-client');
const ObjectType = IgniteClient.ObjectType;
const MapObjectType = IgniteClient.MapObjectType;
const ComplexObjectType = IgniteClient.ComplexObjectType;
const BinaryObject = IgniteClient.BinaryObject;

const CACHE_NAME = '__test_cache';

describe('binary object test suite >', () => {
    let igniteClient = null;
    const typeName = 'TestClass1';
    const stringValue = 'abc';
    const doubleValue = 123.45;
    const boolValue = false;
    const intValue = 456;
    const dateValue = new Date();

    beforeAll((done) => {
        Promise.resolve().
            then(async () => {
                await TestingHelper.init();
                igniteClient = TestingHelper.igniteClient;
                await testSuiteCleanup(done);
                await igniteClient.getOrCreateCache(CACHE_NAME);
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

    it('binary objects set get fields', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME).setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);
                try {
                    const obj1 = new BinaryObject(typeName);
                    obj1.setField('field_double', doubleValue);
                    obj1.setField('field_string', stringValue);

                    await cache.put(2, obj1);
                    await cache.put(3, obj1);
                    const obj2 = await cache.get(2);
                    expect(await TestingHelper.compare(obj1, obj2)).toBe(true);

                    const obj3 = await cache.get(3);
                    obj2.setField('field_double', await obj1.getField('field_double'));
                    obj2.setField('field_string', await obj1.getField('field_string'));
                    expect(await TestingHelper.compare(obj1, obj2)).toBe(true);

                    obj1.setField('field_double', await obj3.getField('field_double'));
                    obj1.setField('field_string', await obj3.getField('field_string'));
                    expect(await TestingHelper.compare(obj1, obj3)).toBe(true);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('binary object remove field', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME).setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);
                try {
                    const obj1 = new BinaryObject(typeName);
                    obj1.setField('field_double', doubleValue);
                    obj1.setField('field_string', stringValue);
                    obj1.setField('field_bool', boolValue);
                    expect(obj1.hasField('field_bool')).toBe(true);
                    expect(await obj1.getField('field_bool')).toBe(boolValue);

                    obj1.removeField('field_bool');
                    expect(obj1.hasField('field_bool')).toBe(boolValue);
                    expect(await obj1.getField('field_bool')).toBe(undefined);

                    await cache.put(3, obj1);
                    const obj2 = await cache.get(3);
                    expect(await TestingHelper.compare(obj1, obj2)).toBe(true);

                    obj2.setField('field_bool', boolValue);
                    expect(obj2.hasField('field_bool')).toBe(true);
                    expect(await obj2.getField('field_bool')).toBe(boolValue);

                    obj2.removeField('field_bool');
                    expect(obj2.hasField('field_bool')).toBe(boolValue);
                    expect(await obj2.getField('field_bool')).toBe(undefined);

                    obj2.setField('field_bool', boolValue);
                    await cache.put(4, obj2);

                    obj1.setField('field_bool', boolValue);
                    await cache.put(5, obj1);

                    expect(await TestingHelper.compare(await cache.get(4), await cache.get(5))).toBe(true);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('binary objects of different schemas', (done) => {
        Promise.resolve().
            then(async () => {
                const cache = igniteClient.getCache(CACHE_NAME).setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);
                try {
                    const obj1 = new BinaryObject(typeName);
                    obj1.setField('field_int', intValue, ObjectType.PRIMITIVE_TYPE.INTEGER);
                    obj1.setField('field_string', stringValue);
                    obj1.setField('field_bool', false);
                    await cache.put(1, obj1);

                    const obj2 = new BinaryObject(typeName);
                    obj2.setField('field_int', intValue, ObjectType.PRIMITIVE_TYPE.INTEGER);
                    obj2.setField('field_bool', false);
                    obj2.setField('field_date', dateValue);
                    await cache.put(2, obj2);

                    const obj3 = await cache.get(1, obj1);
                    obj3.removeField('field_string');
                    const obj4 = await cache.get(2, obj1);
                    obj4.removeField('field_date');
                    expect(await TestingHelper.compare(obj3, obj4)).toBe(true);

                    await cache.put(3, obj3);
                    await cache.put(4, obj4);
                    expect(await TestingHelper.compare(await cache.get(3), await cache.get(4))).toBe(true);
                }
                finally {
                    await cache.removeAll();
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }
});
