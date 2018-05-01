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
const Enum = IgniteClient.Enum;
const Timestamp = IgniteClient.Timestamp;
const Decimal = IgniteClient.Decimal;

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
                    await putGetArrays(primitiveType, type, values[0], [], comparator);
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
                                MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, type1, type2), 
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
                            new MapObjectType(MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, type1, type2), 
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
    const floatComparator = (date1, date2) => { return Math.abs(date1 - date2) < 0.00001; };
    const defaultComparator = (value1, value2) => { return value1 === value2; };
    const UUIDComparator = (value1, value2) => {
        if (value1 === null && value2 === null) {
            return true;
        }
        if (value1 === null && value2 !== null || value1 !== null && value2 === null) {
            return false;
        }
        return value1 instanceof Array && value2 instanceof Array &&
            value1.length === value2.length &&
            value1.every((elem1, index) => defaultComparator(elem1, value2[index]));
    };
    const enumComparator = (value1, value2) => {
        return value1.getTypeId() === value2.getTypeId() &&
            value1.getOrdinal() === value2.getOrdinal(); };
    const decimalComparator = (value1, value2) => {
        return value1 === null && value2 === null ||
            value1.equals(value2);
    };
    const timestampComparator = (value1, value2) => {
        return value1 === null && value2 === null ||
            dateComparator(value1.getDate(), value2.getDate()) &&
            value1.getNanos() === value2.getNanos(); };

    const numericValueModificator = (data) => { return data > 0 ? data - 10 : data + 10; };
    const charValueModificator = (data) => { return String.fromCharCode(data.charCodeAt(0) + 5); };
    const booleanValueModificator = (data) => { return !data; };
    const stringValueModificator = (data) => { return data + 'xxx'; };
    const dateValueModificator = (data) => { return new Date(data.value + 12345); };
    const UUIDValueModificator = (data) => { return data.reverse(); };
    const enumValueModificator = (data) => { return new Enum(data.getTypeId() + 1, data.getOrdinal() + 1); };
    const decimalValueModificator = (data) => { return data.add(12345); };
    const timestampValueModificator = (data) => { return new Timestamp(new Date(data.getDate() + 12345), data.getNanos() + 123); };

    const primitiveValues = {
        [ObjectType.PRIMITIVE_TYPE.BYTE] : { 
            values : [-128, 0, 127],
            modificator : numericValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.SHORT] : {
            values : [-32768, 0, 32767],
            modificator : numericValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.INTEGER] : {
            values : [12345, 0, -54321],
            modificator : numericValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.LONG] : {
            values : [12345678912345, 0, -98765432112345],
            modificator : numericValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.FLOAT] : {
            values : [-1.155, 0, 123e-5],
            comparator : floatComparator,
            modificator : numericValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.DOUBLE] : {
            values : [-123e5, 0, 0.0001],
            typeOptional : true,
            comparator : floatComparator,
            modificator : numericValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.CHAR] : {
            values : ['a', String.fromCharCode(0x1234)],
            modificator : charValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.BOOLEAN] : {
            values : [true, false],
            typeOptional : true,
            modificator : booleanValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.STRING] : {
            values : ['abc', ''],
            typeOptional : true,
            modificator : stringValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.UUID] : {
            values : [
                [ 18, 70, 2, 119, 154, 254, 198, 254, 195, 146, 33, 60, 116, 230, 0, 146 ],
                [ 141, 77, 31, 194, 127, 36, 184, 255, 192, 4, 118, 57, 253, 209, 111, 147 ]
            ],
            comparator : UUIDComparator,
            modificator : UUIDValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.DATE] : {
            values : [new Date(), new Date('1995-12-17'), new Date(0)],
            typeOptional : true,
            comparator : dateComparator,
            modificator : dateValueModificator
        },
        // [ObjectType.PRIMITIVE_TYPE.ENUM] : {
        //     values : [new Enum(12345, 7), new Enum(0, 0)],
        //     typeOptional : true,
        //     comparator : enumComparator,
        //     modificator : enumValueModificator
        // },
        [ObjectType.PRIMITIVE_TYPE.DECIMAL] : {
            values : [new Decimal('123456789.6789345'), new Decimal(0), new Decimal('-98765.4321e15')],
            typeOptional : true,
            comparator : decimalComparator,
            modificator : decimalValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.TIMESTAMP] : {
            values : [new Timestamp(new Date(), 12345), new Timestamp(new Date('1995-12-17'), 543), new Timestamp(new Date(0), 0)],
            typeOptional : true,
            comparator : timestampComparator,
            modificator : timestampValueModificator
        },
        [ObjectType.PRIMITIVE_TYPE.TIME] : {
            values : [new Date(), new Date('1995-12-17'), new Date(0)],
            comparator : dateComparator,
            modificator : dateValueModificator
        }
    };

    async function putGetPrimitiveValues(keyType, valueType, key, value, modificator, comparator = null) {
        const cache = await igniteClient.getCache(CACHE_NAME).
            setKeyType(keyType).
            setValueType(valueType);
        try {
            if (!comparator) {
                comparator = defaultComparator;
            }
            await putGetPrimitive(cache, key, value, valueType, comparator);
            const newValue = modificator(value);
            await putGetPrimitive(cache, key, newValue, valueType, comparator);
        }
        finally {
            await cache.removeAll();
        }
    }

    async function putGetPrimitive(cache, key, value, valueType, comparator) {
        await cache.put(key, value);
        let result = await cache.get(key);
        expect(comparator(result, value)).toBe(true,
            `values are not equal: valueType=${valueType}, put value=${value}, get value=${result}`);
    }

    const arrayValues = {
        [ObjectType.PRIMITIVE_TYPE.BYTE_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.BYTE },
        [ObjectType.PRIMITIVE_TYPE.SHORT_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.SHORT },
        [ObjectType.PRIMITIVE_TYPE.INTEGER_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.INTEGER },
        [ObjectType.PRIMITIVE_TYPE.LONG_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.LONG },
        [ObjectType.PRIMITIVE_TYPE.FLOAT_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.FLOAT },
        [ObjectType.PRIMITIVE_TYPE.DOUBLE_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.DOUBLE, typeOptional : true },
        [ObjectType.PRIMITIVE_TYPE.CHAR_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.CHAR },
        [ObjectType.PRIMITIVE_TYPE.BOOLEAN_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.BOOLEAN, typeOptional : true },
        [ObjectType.PRIMITIVE_TYPE.STRING_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.STRING, typeOptional : true },
        [ObjectType.PRIMITIVE_TYPE.UUID_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.UUID },
        [ObjectType.PRIMITIVE_TYPE.DATE_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.DATE, typeOptional : true },
        //[ObjectType.PRIMITIVE_TYPE.ENUM_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.ENUM, typeOptional : true },
        [ObjectType.PRIMITIVE_TYPE.DECIMAL_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.DECIMAL, typeOptional : true },
        [ObjectType.PRIMITIVE_TYPE.TIMESTAMP_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.TIMESTAMP, typeOptional : true },
        [ObjectType.PRIMITIVE_TYPE.TIME_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.TIME }
    };

    async function putGetArrays(keyType, valueType, key, value, comparator = null) {
        const cache = await igniteClient.getCache(CACHE_NAME).
            setKeyType(keyType).
            setValueType(valueType);
        try {
            await cache.put(key, value);
            let result = await cache.get(key);
            if (!comparator) {
                comparator = defaultComparator;
            }
            await cache.clearKey(key);
            expect(result instanceof Array).toBe(true,
                `result is not Array: arrayType=${valueType}, result=${result}`);
            expect(result.length).toBe(value.length,
                `unexpected array length: arrayType=${valueType}, put array=${value}, get array=${result}`);
            expect(result.every((elem, i) => { return comparator(elem, value[i]); })).toBe(true,
                `arrays are different: arrayType=${valueType}, put array=${value}, get array=${result}`);
        }
        finally {
            await cache.removeAll();
        }
    }

    async function putGetMaps(mapType, value, comparator = null) {
        const key = new Date();
        const cache = await igniteClient.getCache(CACHE_NAME).
            setValueType(mapType);
        await cache.put(key, value);
        let result = await cache.get(key);
        if (!comparator) {
            comparator = defaultComparator;
        }
        expect(result instanceof Map).toBe(true,
            `result is not Map: mapType=${mapType}, result=${result}`);
        expect(result.size).toBe(value.size,
            `unexpected Map size: mapType=${mapType}, put value=${value}, get value=${result}`);
        result.forEach((val, key) => {
            if (val instanceof Array && mapType._valueType !== ObjectType.PRIMITIVE_TYPE.UUID) {
                expect(val.every((elem, i) => { return comparator(elem, value.get(key)[i]); })).toBe(true,
                    `Maps are not equal: valueType=${mapType._valueType}, put value=${val}, get value=${value.get(key)}`);
            }
            else {
                expect(comparator(val, value.get(key))).toBe(true,
                    `Maps are not equal: valueType=${mapType._valueType}, put value=${value}, get value=${result}`);
            }
        });
    }

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }
});
