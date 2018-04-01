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
const ObjectType = IgniteClient.ObjectType;
const MapObjectType = IgniteClient.MapObjectType;

const CACHE_NAME = '__test_cache';

describe('cache put get test suite >', () => {
    let igniteClient = null;

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

    it('put get primitive values of different types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type1 of Object.keys(primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = primitiveValues[type1];
                    for (let type2 of Object.keys(primitiveValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = primitiveValues[type2];
                        const comparator = typeInfo2.comparator;
                        const modificator = typeInfo2.modificator;
                        for (let value1 of typeInfo1.values) {
                            for (let value2 of typeInfo2.values) {
                                await putGetPrimitiveValues(type1, type2, value1, value2, modificator, comparator);
                                await putGetPrimitiveValues(
                                    new ObjectType(type1),
                                    new ObjectType(type2),
                                    value1,
                                    value2,
                                    modificator,
                                    typeInfo2.comparator);
                                if (typeInfo1.typeOptional) {
                                    await putGetPrimitiveValues(null, type2, value1, value2, modificator, comparator);
                                }
                                if (typeInfo2.typeOptional) {
                                    await putGetPrimitiveValues(type1, null, value1, value2, modificator, comparator);
                                }
                            }
                        }
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get arrays of different types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(arrayValues)) {
                    type = parseInt(type);
                    const typeInfo = arrayValues[type];
                    const primitiveType = typeInfo.elemType;
                    const values = primitiveValues[primitiveType].values;
                    const comparator = primitiveValues[primitiveType].comparator;
                    await putGetArrays(primitiveType, type, values[0], values, comparator);
                    await putGetArrays(primitiveType, new ObjectType(type), values[0], [], comparator);
                    if (typeInfo.typeOptional) {
                        await putGetArrays(primitiveType, null, values[0], values, comparator);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get maps of different key/value types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type1 of Object.keys(primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = primitiveValues[type1];
                    if (typeInfo1.comparator) {
                        continue;
                    }
                    for (let type2 of Object.keys(primitiveValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = primitiveValues[type2];
                        const map = new Map();
                        let index2 = 0;
                        for (let value1 of typeInfo1.values) {
                            let value2 = typeInfo2.values[index2];
                            index2++;
                            if (index2 >= typeInfo2.values.length) {
                                index2 = 0;
                            }
                            map.set(value1, value2);
                        }
                        await putGetMaps(
                            new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, type2),
                            map, typeInfo2.comparator);
                        await putGetMaps(
                            new MapObjectType(
                                MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, new ObjectType(type1), new ObjectType(type2)), 
                            map, typeInfo2.comparator);
                        if (typeInfo1.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, null, type2),
                            map, typeInfo2.comparator);
                        }
                        if (typeInfo2.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, null),
                            map, typeInfo2.comparator);
                        }
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get maps with arrays of different types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type1 of Object.keys(primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = primitiveValues[type1];
                    if (typeInfo1.comparator) {
                        continue;
                    }
                    for (let type2 of Object.keys(arrayValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = arrayValues[type2];
                        const primitiveType2 = typeInfo2.elemType;
                        const values2 = primitiveValues[primitiveType2].values;
                        const comparator = primitiveValues[primitiveType2].comparator;
                        const map = new Map();
                        let index2 = 0;
                        const arrayValues2 = [values2, null, values2.reverse()];
                        for (let value1 of typeInfo1.values) {
                            map.set(value1, arrayValues2[index2]);
                            index2++;
                            if (index2 >= arrayValues2.length) {
                                index2 = 0;
                            }
                        }
                        await putGetMaps(
                            new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, type2),
                            map, comparator);
                        await putGetMaps(
                            new MapObjectType(MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, new ObjectType(type1), new ObjectType(type2)), 
                            map, comparator);
                        if (typeInfo1.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, null, type2),
                            map, comparator);
                        }
                        if (typeInfo2.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, null),
                            map, comparator);
                        }
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    const dateComparator = (date1, date2) => { return !date1 && !date2 || date1.value === date2.value; };
    const floatComparator = (date1, date2) => { return date1 - date2 < 0.00001; };

    const numericValueModificator = (data) => { return data > 0 ? data - 10 : data + 10; };
    const charValueModificator = (data) => { return String.fromCharCode(data.charCodeAt(0) + 5); };
    const booleanValueModificator = (data) => { return !data; };
    const stringValueModificator = (data) => { return data + 'xxx'; };
    const dateValueModificator = (data) => { return new Date(data.value + 12345); };

    const primitiveValues = {
        [ObjectType.TYPE_CODE.BYTE] : { 
            values : [-128, 0, 127],
            modificator : numericValueModificator
        },
        [ObjectType.TYPE_CODE.SHORT] : {
            values : [-32768, 0, 32767],
            modificator : numericValueModificator
        },
        [ObjectType.TYPE_CODE.INTEGER] : {
            values : [12345, 0, -54321],
            modificator : numericValueModificator
        },
        [ObjectType.TYPE_CODE.LONG] : {
            values : [12345678912345, 0, -98765432112345],
            modificator : numericValueModificator
        },
        [ObjectType.TYPE_CODE.FLOAT] : {
            values : [-1.155, 0, 123e-5],
            comparator : floatComparator,
            modificator : numericValueModificator
        },
        [ObjectType.TYPE_CODE.DOUBLE] : {
            values : [-123e5, 0, 0.0001],
            typeOptional : true,
            comparator : floatComparator,
            modificator : numericValueModificator
        },
        [ObjectType.TYPE_CODE.CHAR] : {
            values : ['a', String.fromCharCode(0x1234)],
            modificator : charValueModificator
        },
        [ObjectType.TYPE_CODE.BOOLEAN] : {
            values : [true, false],
            typeOptional : true,
            modificator : booleanValueModificator
        },
        [ObjectType.TYPE_CODE.STRING] : {
            values : ['abc', ''],
            typeOptional : true,
            modificator : stringValueModificator
        },
        [ObjectType.TYPE_CODE.DATE] : {
            values : [new Date(), new Date('1995-12-17'), new Date(0)],
            typeOptional : true,
            comparator : dateComparator,
            modificator : dateValueModificator
        }
    };

    async function putGetPrimitiveValues(keyType, valueType, key, value, modificator, comparator = null) {
        const cache = await igniteClient.getCache(CACHE_NAME).
            setKeyType(keyType).
            setValueType(valueType);
        if (!comparator) {
            comparator = (value1, value2) => { return value1 === value2; }
        }
        await putGetPrimitive(cache, key, value, valueType, comparator);
        const newValue = modificator(value);
        await putGetPrimitive(cache, key, newValue, valueType, comparator);
        await cache.removeAll();
    }

    async function putGetPrimitive(cache, key, value, valueType, comparator) {
        await cache.put(key, value);
        let result = await cache.get(key);
        expect(comparator(result, value)).toBe(true,
            `values are not equal: valueType=${valueType}, put value=${value}, get value=${result}`);
    }

    const arrayValues = {
        [ObjectType.TYPE_CODE.BYTE_ARRAY] : { elemType : ObjectType.TYPE_CODE.BYTE },
        [ObjectType.TYPE_CODE.SHORT_ARRAY] : { elemType : ObjectType.TYPE_CODE.SHORT },
        [ObjectType.TYPE_CODE.INTEGER_ARRAY] : { elemType : ObjectType.TYPE_CODE.INTEGER },
        [ObjectType.TYPE_CODE.LONG_ARRAY] : { elemType : ObjectType.TYPE_CODE.LONG },
        [ObjectType.TYPE_CODE.FLOAT_ARRAY] : { elemType : ObjectType.TYPE_CODE.FLOAT },
        [ObjectType.TYPE_CODE.DOUBLE_ARRAY] : { elemType : ObjectType.TYPE_CODE.DOUBLE, typeOptional : true },
        [ObjectType.TYPE_CODE.CHAR_ARRAY] : { elemType : ObjectType.TYPE_CODE.CHAR },
        [ObjectType.TYPE_CODE.BOOLEAN_ARRAY] : { elemType : ObjectType.TYPE_CODE.BOOLEAN, typeOptional : true },
        [ObjectType.TYPE_CODE.STRING_ARRAY] : { elemType : ObjectType.TYPE_CODE.STRING, typeOptional : true },
        [ObjectType.TYPE_CODE.DATE_ARRAY] : { elemType : ObjectType.TYPE_CODE.DATE, typeOptional : true }
    };

    async function putGetArrays(keyType, valueType, key, value, comparator = null) {
        const cache = await igniteClient.getCache(CACHE_NAME).
            setKeyType(keyType).
            setValueType(valueType);
        await cache.put(key, value);
        let result = await cache.get(key);
        if (!comparator) {
            comparator = (value1, value2) => { return value1 === value2; }
        }
        expect(result instanceof Array).toBe(true,
            `result is not Array: arrayType=${valueType}, result=${result}`);
        expect(result.length).toBe(value.length,
            `unexpected array length: arrayType=${valueType}, put array=${value}, get array=${result}`);
        expect(result.every((elem, i) => { return comparator(elem, value[i]); })).toBe(true,
            `arrays are different: arrayType=${valueType}, put array=${value}, get array=${result}`);
        await cache.removeAll();
    }

    async function putGetMaps(valueType, value, comparator = null) {
        const key = new Date();
        const cache = await igniteClient.getCache(CACHE_NAME).
            setValueType(valueType);
        await cache.put(key, value);
        let result = await cache.get(key);
        if (!comparator) {
            comparator = (value1, value2) => { return value1 === value2; }
        }
        expect(result instanceof Map).toBe(true,
            `result is not Map: valueType=${valueType}, result=${result}`);
        expect(result.size).toBe(value.size,
            `unexpected Map size: mapType=${valueType}, put value=${value}, get value=${result}`);
        result.forEach((val, key) => {
            if (val instanceof Array) {
                expect(val.every((elem, i) => { return comparator(elem, value.get(key)[i]); })).toBe(true,
                    `Maps are not equal: valueType=${valueType.mapValueType}, put value=${val}, get value=${value.get(key)}`);
            }
            else {
                expect(comparator(val, value.get(key))).toBe(true,
                    `Maps are not equal: valueType=${valueType.mapValueType}, put value=${value}, get value=${result}`);
            }
        });
    }

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }
});
