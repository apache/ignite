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

class Class1 {
    constructor() {
        this.field_1_1 = null;
        this.field_1_2 = new Class2();
        this.field_1_3 = null;
    }
}

class SubClass1 extends Class1 {
    constructor() {
        super();
        this.field_1_4 = null;
        this.field_1_5 = new Class3();
        this.field_1_6 = null;
    }
}

class Class2 {
    constructor() {
        this.field_2_1 = null;
        this.field_2_2 = null;
    }
}

class Class3 {
    constructor() {
        this.field_3_1 = null;
        this.field_3_2 = null;
    }
}

const dateComparator = (date1, date2) => {
    const result = !date1 && !date2 || date1.value === date2.value;
    if (!result) {
        TestingHelper.logDebug(Util.format('dateComparator, values are different: %s, %s', date1, date2));
    }
    return result;
};
const floatComparator = (value1, value2) => {
    const result = Math.abs(value1 - value2) < 0.00001;
    if (!result) {
        TestingHelper.logDebug(Util.format('floatComparator, values are different: %d, %d', value1, value2));
    }
    return result;
};
const defaultComparator = (value1, value2) => {
    const result = value1 === value2;
    if (!result) {
        TestingHelper.logDebug(Util.format('defaultComparator, values are different: %s, %s', value1, value2));
    }
    return result;
};

function compare(value1, value2) {
    TestingHelper.logDebug(Util.format('compare: %s, %s', JSON.stringify(value1), JSON.stringify(value2)));
    if (value1 === undefined || value2 === undefined) {
        TestingHelper.logDebug(Util.format('compare: unexpected "undefined" value'));
        return false;
    }
    if (value1 === null && value2 === null) {
        return true;
    }
    if (typeof value1 !== typeof value2) {
        TestingHelper.logDebug(Util.format('compare: value types are different'));
        return false;
    }
    if (typeof value1 === 'number') {
        return floatComparator(value1, value2);
    }
    else if (typeof value1 !== 'object') {
        return defaultComparator(value1, value2);
    }
    else if (value1 instanceof Date && value2 instanceof Date) {
        return dateComparator(value1, value2);
    }
    else if (value1 instanceof Array && value2 instanceof Array) {
        if (value1.length !== value2.length) {
            TestingHelper.logDebug(Util.format('compare: array lengths are different'));
            return false;
        }
        return value1.every((elem, i) => { return compare(elem, value2[i]); });
    }
    else if (value1 instanceof Map && value2 instanceof Map) {
        if (value1.size !== value2.size) {
            TestingHelper.logDebug(Util.format('compare: map sizes are different'));
            return false;
        }
        for (var [key, val] of value1) {
            if (!compare(val, value2.get(key))) {
                return false;
            }
        }
        return true;
    }
    else if (value2 instanceof BinaryObject) {
        return Object.keys(value1).every((key) => {
            return compare(value1[key], value2.getField(key));
        });
    }
    else {
        return Object.keys(value1).every((key) => {
            return compare(value1[key], value2[key]);
        });
    }
}

describe('complex object test suite >', () => {
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

    it('put get complex objects', (done) => {
        Promise.resolve().
            then(async () => {
                const value1 = new Class1();
                value1.field_1_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.BYTE);
                value1.field_1_2.field_2_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.SHORT);
                value1.field_1_2.field_2_2 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.INTEGER);
                value1.field_1_3 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.LONG);

                const valueType1 = new ComplexObjectType(new Class1()).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.BYTE).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.SHORT).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.INTEGER)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.LONG);

                const value2 = new SubClass1();
                value2.field_1_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.FLOAT);
                value2.field_1_2.field_2_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.DOUBLE);
                value2.field_1_2.field_2_2 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.CHAR);
                value2.field_1_3 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.BOOLEAN);
                value2.field_1_4 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.STRING);
                value2.field_1_5.field_3_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.DATE);
                value2.field_1_5.field_3_2 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.SHORT);
                value2.field_1_6 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.INTEGER);

                const valueType2 = new ComplexObjectType(new SubClass1()).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.FLOAT).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.DOUBLE).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.CHAR)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.BOOLEAN).
                    setFieldType('field_1_4', ObjectType.PRIMITIVE_TYPE.STRING).
                    setFieldType('field_1_5', new ComplexObjectType(new Class3()).
                        setFieldType('field_3_1', ObjectType.PRIMITIVE_TYPE.DATE).
                        setFieldType('field_3_2', ObjectType.PRIMITIVE_TYPE.SHORT)).
                    setFieldType('field_1_6', ObjectType.PRIMITIVE_TYPE.INTEGER);

                await putGetComplexObjectsWithDifferentTypes(
                    value1, value2, valueType1, valueType2, Class1, SubClass1);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get unnamed complex objects', (done) => {
        Promise.resolve().
            then(async () => {
                const value1 = {};
                value1.field_1_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.BYTE);
                value1.field_1_2 = {};
                value1.field_1_2.field_2_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.SHORT);
                value1.field_1_2.field_2_2 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.INTEGER);
                value1.field_1_3 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.LONG);

                const valueType1 = new ComplexObjectType(value1).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.BYTE).
                    setFieldType('field_1_2', new ComplexObjectType(value1.field_1_2).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.SHORT).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.INTEGER)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.LONG);

                const value2 = {};
                value2.field_1_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.FLOAT);
                value2.field_1_2 = {};
                value2.field_1_2.field_2_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.DOUBLE);
                value2.field_1_2.field_2_2 = {};
                value2.field_1_2.field_2_2.field_3_1 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.CHAR);
                value2.field_1_2.field_2_2.field_3_2 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.BOOLEAN);
                value2.field_1_3 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.STRING);
                value2.field_1_4 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.DATE);

                const valueType2 = new ComplexObjectType(value2).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.FLOAT).
                    setFieldType('field_1_2', new ComplexObjectType(value2.field_1_2).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.DOUBLE).
                        setFieldType('field_2_2', new ComplexObjectType(value2.field_1_2.field_2_2).
                            setFieldType('field_3_1', ObjectType.PRIMITIVE_TYPE.CHAR).
                            setFieldType('field_3_2', ObjectType.PRIMITIVE_TYPE.BOOLEAN))).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.STRING).
                    setFieldType('field_1_4', ObjectType.PRIMITIVE_TYPE.DATE);

                await putGetComplexObjects(value1, value2,
                    valueType1, valueType2, value2);

                await putGetComplexObjects(value1, value2,
                    new ComplexObjectType(value1), new ComplexObjectType(value2), value2);

                await putGetComplexObjects({}, {},
                    new ComplexObjectType(), new ComplexObjectType(), {});

                let binaryKey = BinaryObject.fromObject(value1, valueType1);
                let binaryValue = BinaryObject.fromObject(value2, valueType2);
                await putGetComplexObjects(binaryKey, binaryValue,
                    null, null, value2);

                binaryKey = BinaryObject.fromObject({});
                binaryValue = BinaryObject.fromObject({});
                await putGetComplexObjects(binaryKey, binaryValue,
                    null, null, {});
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get complex objects with arrays', (done) => {
        Promise.resolve().
            then(async () => {
                const value1 = new Class1();
                value1.field_1_1 = getArrayValues(ObjectType.PRIMITIVE_TYPE.BYTE_ARRAY);
                value1.field_1_2.field_2_1 = getArrayValues(ObjectType.PRIMITIVE_TYPE.SHORT_ARRAY);
                value1.field_1_2.field_2_2 = getArrayValues(ObjectType.PRIMITIVE_TYPE.INTEGER_ARRAY);
                value1.field_1_3 = getArrayValues(ObjectType.PRIMITIVE_TYPE.LONG_ARRAY);

                const valueType1 = new ComplexObjectType(new Class1()).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.BYTE_ARRAY).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.SHORT_ARRAY).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.INTEGER_ARRAY)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.LONG_ARRAY);

                const value2 = new SubClass1();
                value2.field_1_1 = getArrayValues(ObjectType.PRIMITIVE_TYPE.FLOAT_ARRAY);
                value2.field_1_2.field_2_1 = getArrayValues(ObjectType.PRIMITIVE_TYPE.DOUBLE_ARRAY);
                value2.field_1_2.field_2_2 = getArrayValues(ObjectType.PRIMITIVE_TYPE.CHAR_ARRAY);
                value2.field_1_3 = getArrayValues(ObjectType.PRIMITIVE_TYPE.BOOLEAN_ARRAY);
                value2.field_1_4 = getArrayValues(ObjectType.PRIMITIVE_TYPE.STRING_ARRAY);
                value2.field_1_5.field_3_1 = getArrayValues(ObjectType.PRIMITIVE_TYPE.DATE_ARRAY);
                value2.field_1_5.field_3_2 = getArrayValues(ObjectType.PRIMITIVE_TYPE.SHORT_ARRAY);
                value2.field_1_6 = getArrayValues(ObjectType.PRIMITIVE_TYPE.INTEGER_ARRAY);

                const valueType2 = new ComplexObjectType(new SubClass1()).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.FLOAT_ARRAY).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.DOUBLE_ARRAY).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.CHAR_ARRAY)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.BOOLEAN_ARRAY).
                    setFieldType('field_1_4', ObjectType.PRIMITIVE_TYPE.STRING_ARRAY).
                    setFieldType('field_1_5', new ComplexObjectType(new Class3()).
                        setFieldType('field_3_1', ObjectType.PRIMITIVE_TYPE.DATE_ARRAY).
                        setFieldType('field_3_2', ObjectType.PRIMITIVE_TYPE.SHORT_ARRAY)).
                    setFieldType('field_1_6', ObjectType.PRIMITIVE_TYPE.INTEGER_ARRAY);

                await putGetComplexObjectsWithDifferentTypes(
                    value1, value2, valueType1, valueType2, Class1, SubClass1, true);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get complex objects with maps', (done) => {
        Promise.resolve().
            then(async () => {
                const value1 = new Class1();
                value1.field_1_1 = getMapValue(ObjectType.PRIMITIVE_TYPE.BYTE);
                value1.field_1_2.field_2_1 = getMapValue(ObjectType.PRIMITIVE_TYPE.SHORT);
                value1.field_1_2.field_2_2 = getMapValue(ObjectType.PRIMITIVE_TYPE.INTEGER);
                value1.field_1_3 = getMapValue(ObjectType.PRIMITIVE_TYPE.LONG);

                const valueType1 = new ComplexObjectType(new Class1()).
                    setFieldType('field_1_1', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.BYTE, ObjectType.PRIMITIVE_TYPE.BYTE)).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.SHORT, ObjectType.PRIMITIVE_TYPE.SHORT)).
                        setFieldType('field_2_2', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.INTEGER, ObjectType.PRIMITIVE_TYPE.INTEGER))).
                    setFieldType('field_1_3', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.LONG, ObjectType.PRIMITIVE_TYPE.LONG));

                const value2 = new SubClass1();
                value2.field_1_1 = getMapValue(ObjectType.PRIMITIVE_TYPE.FLOAT);
                value2.field_1_2.field_2_1 = getMapValue(ObjectType.PRIMITIVE_TYPE.DOUBLE);
                value2.field_1_2.field_2_2 = getMapValue(ObjectType.PRIMITIVE_TYPE.CHAR);
                value2.field_1_3 = getMapValue(ObjectType.PRIMITIVE_TYPE.BOOLEAN);
                value2.field_1_4 = getMapValue(ObjectType.PRIMITIVE_TYPE.STRING);
                value2.field_1_5.field_3_1 = getMapValue(ObjectType.PRIMITIVE_TYPE.DATE);
                value2.field_1_5.field_3_2 = getMapValue(ObjectType.PRIMITIVE_TYPE.SHORT);
                value2.field_1_6 = getMapValue(ObjectType.PRIMITIVE_TYPE.INTEGER);

                const valueType2 = new ComplexObjectType(new SubClass1()).
                    setFieldType('field_1_1', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.LONG, ObjectType.PRIMITIVE_TYPE.FLOAT)).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.LONG, ObjectType.PRIMITIVE_TYPE.DOUBLE)).
                        setFieldType('field_2_2', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.CHAR, ObjectType.PRIMITIVE_TYPE.CHAR))).
                    setFieldType('field_1_3', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.BOOLEAN, ObjectType.PRIMITIVE_TYPE.BOOLEAN)).
                    setFieldType('field_1_4', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.STRING)).
                    setFieldType('field_1_5', new ComplexObjectType(new Class3()).
                        setFieldType('field_3_1', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.LONG, ObjectType.PRIMITIVE_TYPE.DATE)).
                        setFieldType('field_3_2', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.SHORT, ObjectType.PRIMITIVE_TYPE.SHORT))).
                    setFieldType('field_1_6', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.INTEGER, ObjectType.PRIMITIVE_TYPE.INTEGER));

                await putGetComplexObjectsWithDifferentTypes(
                    value1, value2, valueType1, valueType2, Class1, SubClass1, true);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get binary objects', (done) => {
        Promise.resolve().
            then(async () => {
                const valueType = new ComplexObjectType(new Class1()).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.STRING).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2()).
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.SHORT).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.BOOLEAN)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.STRING_ARRAY);
                await putGetBinaryObjects(valueType);
                await putGetBinaryObjects(null);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    const primitiveValues = {
        [ObjectType.PRIMITIVE_TYPE.BYTE] : { 
            values : [-128, 0, 127],
        },
        [ObjectType.PRIMITIVE_TYPE.SHORT] : {
            values : [-32768, 0, 32767],
        },
        [ObjectType.PRIMITIVE_TYPE.INTEGER] : {
            values : [12345, 0, -54321],
        },
        [ObjectType.PRIMITIVE_TYPE.LONG] : {
            values : [12345678912345, 0, -98765432112345],
        },
        [ObjectType.PRIMITIVE_TYPE.FLOAT] : {
            values : [-1.155, 0, 123e-5],
            comparator : floatComparator,
        },
        [ObjectType.PRIMITIVE_TYPE.DOUBLE] : {
            values : [-123e5, 0, 0.0001],
            typeOptional : true,
            comparator : floatComparator,
        },
        [ObjectType.PRIMITIVE_TYPE.CHAR] : {
            values : ['a', String.fromCharCode(0x1234)],
        },
        [ObjectType.PRIMITIVE_TYPE.BOOLEAN] : {
            values : [true, false],
            typeOptional : true,
        },
        [ObjectType.PRIMITIVE_TYPE.STRING] : {
            values : ['abc', ''],
            typeOptional : true,
        },
        [ObjectType.PRIMITIVE_TYPE.DATE] : {
            values : [new Date(), new Date('1995-12-17'), new Date(0)],
            typeOptional : true,
            comparator : dateComparator,
        }
    };

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
        [ObjectType.PRIMITIVE_TYPE.DATE_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.DATE, typeOptional : true }
    };

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }

    async function putGetComplexObjects(key, value, keyType, valueType, valuePattern) {
        const cache = igniteClient.getCache(CACHE_NAME).setKeyType(keyType).setValueType(valueType);
        try {
            await cache.put(key, value);
            const result = await cache.get(key);
            expect(compare(valuePattern, result)).toBe(true,
                `values are not equal: put value=${JSON.stringify(valuePattern)}, get value=${JSON.stringify(result)}`);
        }
        finally {
            await cache.removeAll();
        }
    }

    function binaryObjectEquals(binaryObj, valuePattern, valueType) {
        expect(compare(valuePattern, binaryObj)).toBe(true,
            `binary values are not equal: put value=${JSON.stringify(valuePattern)}, get value=${JSON.stringify(binaryObj)}`);

        let value1, value2;
        for (let key of Object.keys(valuePattern)) {
            value1 = valuePattern[key];
            value2 = binaryObj.getField(key, valueType ? valueType._getFieldType(key) : null);
            expect(compare(value1, value2)).toBe(true,
                `values for key ${key} are not equal: put value=${value1}, get value=${value2}`);
        }

        if (valueType) {
            const toObject = binaryObj.toObject(valueType);
            expect(compare(valuePattern, toObject)).toBe(true,
                `values are not equal: put value=${JSON.stringify(valuePattern)}, get value=${JSON.stringify(toObject)}`);
        }
    }

    async function putGetComplexObjectsWithDifferentTypes(
        key, value, keyType, valueType, keyClass, valueClass, isNullable = false) {
        await putGetComplexObjects(key, value,
            new ComplexObjectType(new keyClass()), new ComplexObjectType(new valueClass()), value);
        await putGetComplexObjects(key, value,
           new ComplexObjectType(key), new ComplexObjectType(value), value);
        await putGetComplexObjects(key, value, keyType, valueType, value);

        await putGetComplexObjects(new keyClass(), new valueClass(),
            new ComplexObjectType(new keyClass()), new ComplexObjectType(new valueClass()), new valueClass());
        if (isNullable) {
            await putGetComplexObjects(new keyClass(), new valueClass(), keyType, valueType, new valueClass());
        }

        let binaryKey = BinaryObject.fromObject(key);
        let binaryValue = BinaryObject.fromObject(value);
        await putGetComplexObjects(binaryKey, binaryValue, null, null, value);

        binaryKey = BinaryObject.fromObject(key, new ComplexObjectType(new keyClass()));
        binaryValue = BinaryObject.fromObject(value, new ComplexObjectType(new valueClass()));
        await putGetComplexObjects(binaryKey, binaryValue, null, null, value);

        binaryKey = BinaryObject.fromObject(key, new ComplexObjectType(key));
        binaryValue = BinaryObject.fromObject(value, new ComplexObjectType(value));
        await putGetComplexObjects(binaryKey, binaryValue, null, null, value);

        binaryKey = BinaryObject.fromObject(key, keyType);
        binaryValue = BinaryObject.fromObject(value, valueType);
        await putGetComplexObjects(binaryKey, binaryValue, null, null, value);

        if (isNullable) {
            binaryKey = BinaryObject.fromObject(new keyClass());
            binaryValue = BinaryObject.fromObject(new valueClass());
            await putGetComplexObjects(binaryKey, binaryValue, null, null, new valueClass());
        }
    }

    async function putGetBinaryObjects(valueType) {
        const value1 = new Class1();
        value1.field_1_1 = 'abc';
        value1.field_1_2.field_2_1 = 1234;
        value1.field_1_2.field_2_2 = true;
        value1.field_1_3 = ['a', 'bb', 'ccc'];

        const value2 = new Class1();
        value2.field_1_1 = 'def';
        value2.field_1_2.field_2_1 = 5432;
        value2.field_1_2.field_2_2 = false;
        value2.field_1_3 = ['a', 'bb', 'ccc', 'dddd'];

        const value3 = new Class1();
        value3.field_1_1 = 'defdef';
        value3.field_1_2.field_2_1 = 543;
        value3.field_1_2.field_2_2 = false;
        value3.field_1_3 = ['a', 'bb', 'ccc', 'dddd', 'eeeee'];

        const binaryValue1 = BinaryObject.fromObject(value1, valueType);
        const binaryValue2 = BinaryObject.fromObject(value2, valueType);
        const binaryValue3 = BinaryObject.fromObject(value3);

        const cache = igniteClient.getCache(CACHE_NAME);
        try {
            await cache.put(binaryValue1, binaryValue2);
            let result = await cache.get(binaryValue1);
            binaryObjectEquals(result, value2, valueType);

            binaryValue1.setField('field_1_1', 'abcde');
            result = await cache.get(binaryValue1);
            expect(result === null).toBe(true);

            binaryValue2.setField('field_1_1', value3.field_1_1);
            binaryValue2.setField('field_1_2', value3.field_1_2);
            binaryValue2.setField('field_1_3', value3.field_1_3);
            await cache.put(binaryValue1, binaryValue2);
            result = await cache.get(binaryValue1);
            binaryObjectEquals(result, value3, valueType);

            binaryValue1.setField('field_1_1', 'abc');
            binaryValue1.setField('field_1_3', binaryValue1.getField('field_1_3'));
            result = await cache.get(binaryValue1);
            binaryObjectEquals(result, value2, valueType);

            result = await cache.get(binaryValue1);
            binaryObjectEquals(result, value2, valueType);

            binaryValue3.setField('field_1_1', result.getField('field_1_1'));
            binaryValue3.setField('field_1_2', result.getField('field_1_2'));
            binaryValue3.setField('field_1_3', result.getField('field_1_3'));
            await cache.put(binaryValue1, binaryValue3);
            result = await cache.get(binaryValue1);
            binaryObjectEquals(result, value2, valueType);
        }
        finally {
            await cache.removeAll();
        }
    }

    function getPrimitiveValue(typeCode) {
        return primitiveValues[typeCode].values[0];
    }

    function getArrayValues(typeCode) {
        return primitiveValues[arrayValues[typeCode].elemType].values;
    }

    function getMapValue(typeCode) {
        const map = new Map();
        const values = primitiveValues[typeCode].values;
        const length = values.length;
        values.forEach((value, index) => {
            if (typeCode === ObjectType.PRIMITIVE_TYPE.FLOAT || typeCode === ObjectType.PRIMITIVE_TYPE.DOUBLE) {
                value = Math.trunc(value);
            }
            else if (typeCode === ObjectType.PRIMITIVE_TYPE.DATE) {
                value = value ? value.getTime() : value;
            }
            map.set(value, values[length - index - 1]);
        });
        return map;
    }
});
