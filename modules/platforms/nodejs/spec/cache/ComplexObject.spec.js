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
        this.field_1_7 = null;
        this.field_1_8 = null;
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
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2ShortInteger').
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
                value2.field_1_5.field_3_2 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.UUID);
                value2.field_1_6 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.DECIMAL);
                value2.field_1_7 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.TIMESTAMP);
                value2.field_1_8 = getPrimitiveValue(ObjectType.PRIMITIVE_TYPE.TIME);

                const valueType2 = new ComplexObjectType(new SubClass1()).
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.FLOAT).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2DoubleChar').
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.DOUBLE).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.CHAR)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.BOOLEAN).
                    setFieldType('field_1_4', ObjectType.PRIMITIVE_TYPE.STRING).
                    setFieldType('field_1_5', new ComplexObjectType(new Class3()).
                        setFieldType('field_3_1', ObjectType.PRIMITIVE_TYPE.DATE).
                        setFieldType('field_3_2', ObjectType.PRIMITIVE_TYPE.UUID)).
                    setFieldType('field_1_6', ObjectType.PRIMITIVE_TYPE.DECIMAL).
                    setFieldType('field_1_7', ObjectType.PRIMITIVE_TYPE.TIMESTAMP).
                    setFieldType('field_1_8', ObjectType.PRIMITIVE_TYPE.TIME);

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

                const valueType1 = new ComplexObjectType(value1, 'ObjectWithByteObjLong').
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.BYTE).
                    setFieldType('field_1_2', new ComplexObjectType(value1.field_1_2, 'ObjectWithShortInteger').
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

                const valueType2 = new ComplexObjectType(value2, 'ObjectWithFloatObjStringDate').
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.FLOAT).
                    setFieldType('field_1_2', new ComplexObjectType(value2.field_1_2, 'ObjectWithDoubleObj').
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.DOUBLE).
                        setFieldType('field_2_2', new ComplexObjectType(value2.field_1_2.field_2_2, 'ObjectWithCharBoolean').
                            setFieldType('field_3_1', ObjectType.PRIMITIVE_TYPE.CHAR).
                            setFieldType('field_3_2', ObjectType.PRIMITIVE_TYPE.BOOLEAN))).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.STRING).
                    setFieldType('field_1_4', ObjectType.PRIMITIVE_TYPE.DATE);

                await putGetComplexObjects(value1, value2,
                    valueType1, valueType2, value2);

                await putGetComplexObjects({}, {},
                    new ComplexObjectType({}), new ComplexObjectType({}), {});

                let binaryKey = await BinaryObject.fromObject(value1, valueType1);
                let binaryValue = await BinaryObject.fromObject(value2, valueType2);
                await putGetComplexObjects(binaryKey, binaryValue,
                    null, null, value2);

                binaryKey = await BinaryObject.fromObject({});
                binaryValue = await BinaryObject.fromObject({});
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

                const valueType1 = new ComplexObjectType(new Class1(), 'Class1WithArrays').
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.BYTE_ARRAY).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2WithShortIntegerArrays').
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
                value2.field_1_5.field_3_2 = getArrayValues(ObjectType.PRIMITIVE_TYPE.UUID_ARRAY);
                value2.field_1_6 = getArrayValues(ObjectType.PRIMITIVE_TYPE.DECIMAL_ARRAY);
                value2.field_1_7 = getArrayValues(ObjectType.PRIMITIVE_TYPE.TIMESTAMP_ARRAY);
                value2.field_1_8 = getArrayValues(ObjectType.PRIMITIVE_TYPE.TIME_ARRAY);

                const valueType2 = new ComplexObjectType(new SubClass1(), 'SubClass1WithArrays').
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.FLOAT_ARRAY).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2WithDoubleCharArrays').
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.DOUBLE_ARRAY).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.CHAR_ARRAY)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.BOOLEAN_ARRAY).
                    setFieldType('field_1_4', ObjectType.PRIMITIVE_TYPE.STRING_ARRAY).
                    setFieldType('field_1_5', new ComplexObjectType(new Class3(), 'Class3WithArrays').
                        setFieldType('field_3_1', ObjectType.PRIMITIVE_TYPE.DATE_ARRAY).
                        setFieldType('field_3_2', ObjectType.PRIMITIVE_TYPE.UUID_ARRAY)).
                    setFieldType('field_1_6', ObjectType.PRIMITIVE_TYPE.DECIMAL_ARRAY).
                    setFieldType('field_1_7', ObjectType.PRIMITIVE_TYPE.TIMESTAMP_ARRAY).
                    setFieldType('field_1_8', ObjectType.PRIMITIVE_TYPE.TIME_ARRAY);

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

                const valueType1 = new ComplexObjectType(new Class1(), 'Class1WithMaps').
                    setFieldType('field_1_1', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.BYTE, ObjectType.PRIMITIVE_TYPE.BYTE)).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2WithShortIntegerMaps').
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
                value2.field_1_5.field_3_2 = getMapValue(ObjectType.PRIMITIVE_TYPE.UUID);
                value2.field_1_6 = getMapValue(ObjectType.PRIMITIVE_TYPE.DECIMAL);
                value2.field_1_7 = getMapValue(ObjectType.PRIMITIVE_TYPE.TIMESTAMP);
                value2.field_1_8 = getMapValue(ObjectType.PRIMITIVE_TYPE.TIME);

                const valueType2 = new ComplexObjectType(new SubClass1(), 'SubClass1WithMaps').
                    setFieldType('field_1_1', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.FLOAT)).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2WithDoubleCharMaps').
                        setFieldType('field_2_1', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.DOUBLE)).
                        setFieldType('field_2_2', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.CHAR, ObjectType.PRIMITIVE_TYPE.CHAR))).
                    setFieldType('field_1_3', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.BOOLEAN, ObjectType.PRIMITIVE_TYPE.BOOLEAN)).
                    setFieldType('field_1_4', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.STRING)).
                    setFieldType('field_1_5', new ComplexObjectType(new Class3(), 'Class3WithMaps').
                        setFieldType('field_3_1', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.DATE)).
                        setFieldType('field_3_2', new MapObjectType(
                            MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.UUID))).
                    setFieldType('field_1_6', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.DECIMAL)).
                    setFieldType('field_1_7', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.TIMESTAMP)).
                    setFieldType('field_1_8', new MapObjectType(
                        MapObjectType.MAP_SUBTYPE.HASH_MAP, ObjectType.PRIMITIVE_TYPE.STRING, ObjectType.PRIMITIVE_TYPE.TIME));

                await putGetComplexObjectsWithDifferentTypes(
                    value1, value2, valueType1, valueType2, Class1, SubClass1, true);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get binary objects from objects', (done) => {
        Promise.resolve().
            then(async () => {
                const valueType = new ComplexObjectType(new Class1(), 'Class1WithStringObjStringArray').
                    setFieldType('field_1_1', ObjectType.PRIMITIVE_TYPE.STRING).
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2WithShortBoolean').
                        setFieldType('field_2_1', ObjectType.PRIMITIVE_TYPE.SHORT).
                        setFieldType('field_2_2', ObjectType.PRIMITIVE_TYPE.BOOLEAN)).
                    setFieldType('field_1_3', ObjectType.PRIMITIVE_TYPE.STRING_ARRAY);
                await putGetBinaryObjects(valueType);
                const defaultValueType = new ComplexObjectType(new Class1(), 'Class1Default').
                    setFieldType('field_1_2', new ComplexObjectType(new Class2(), 'Class2Default'));
                await putGetBinaryObjects(defaultValueType);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }

    async function putGetComplexObjects(key, value, keyType, valueType, valuePattern) {
        const cache = igniteClient.getCache(CACHE_NAME).setKeyType(keyType).setValueType(valueType);
        try {
            await cache.put(key, value);
            const result = await cache.get(key);
            expect(await TestingHelper.compare(valuePattern, result)).toBe(true,
                `values are not equal: put value=${TestingHelper.printValue(valuePattern)}, get value=${TestingHelper.printValue(result)}`);
        }
        finally {
            await cache.removeAll();
        }
    }

    async function binaryObjectEquals(binaryObj, valuePattern, valueType) {
        expect(await TestingHelper.compare(valuePattern, binaryObj)).toBe(true,
            `binary values are not equal: put value=${TestingHelper.printValue(valuePattern)}, get value=${TestingHelper.printValue(binaryObj)}`);

        let value1, value2;
        for (let key of Object.keys(valuePattern)) {
            value1 = valuePattern[key];
            value2 = await binaryObj.getField(key, valueType ? valueType._getFieldType(key) : null);
            expect(await TestingHelper.compare(value1, value2)).toBe(true,
                `values for key ${key} are not equal: put value=${TestingHelper.printValue(value1)
                }, get value=${TestingHelper.printValue(value2)}`);
        }

        if (valueType) {
            const toObject = await binaryObj.toObject(valueType);
            expect(await TestingHelper.compare(valuePattern, toObject)).toBe(true,
                `values are not equal: put value=${TestingHelper.printValue(valuePattern)}, get value=${TestingHelper.printValue(toObject)}`);
        }
    }

    async function putGetComplexObjectsWithDifferentTypes(
        key, value, keyType, valueType, keyClass, valueClass, isNullable = false) {
        await putGetComplexObjects(key, value, keyType, valueType, value);

        if (isNullable) {
            await putGetComplexObjects(new keyClass(), new valueClass(), keyType, valueType, new valueClass());
        }

        let binaryKey = await BinaryObject.fromObject(key, keyType);
        let binaryValue = await BinaryObject.fromObject(value, valueType);
        await putGetComplexObjects(binaryKey, binaryValue, null, null, value);

        if (isNullable) {
            binaryKey = await BinaryObject.fromObject(new keyClass(), keyType);
            binaryValue = await BinaryObject.fromObject(new valueClass(), valueType);
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

        const field_1_2_Type = valueType ? valueType._getFieldType('field_1_2') : null;

        const binaryValue1 = await BinaryObject.fromObject(value1, valueType);
        const binaryValue2 = await BinaryObject.fromObject(value2, valueType);
        const binaryValue3 = await BinaryObject.fromObject(value3, valueType);

        const cache = igniteClient.getCache(CACHE_NAME);
        try {
            await cache.put(binaryValue1, binaryValue2);
            let result = await cache.get(binaryValue1);
            await binaryObjectEquals(result, value2, valueType);

            binaryValue1.setField('field_1_1', 'abcde');
            result = await cache.get(binaryValue1);
            expect(result === null).toBe(true);

            binaryValue2.setField('field_1_1', value3.field_1_1);
            binaryValue2.setField('field_1_2', value3.field_1_2, field_1_2_Type);
            binaryValue2.setField('field_1_3', value3.field_1_3);
            await cache.put(binaryValue1, binaryValue2);
            result = await cache.get(binaryValue1);
            await binaryObjectEquals(result, value3, valueType);

            binaryValue1.setField('field_1_1', 'abc');
            binaryValue1.setField('field_1_3', await binaryValue1.getField('field_1_3'));
            result = await cache.get(binaryValue1);
            await binaryObjectEquals(result, value2, valueType);

            result = await cache.get(binaryValue1);
            await binaryObjectEquals(result, value2, valueType);

            binaryValue3.setField('field_1_1', await result.getField('field_1_1'));
            binaryValue3.setField('field_1_2', await result.getField('field_1_2', field_1_2_Type), field_1_2_Type);
            binaryValue3.setField('field_1_3', await result.getField('field_1_3'));
            await cache.put(binaryValue1, binaryValue3);
            result = await cache.get(binaryValue1);
            await binaryObjectEquals(result, value2, valueType);
        }
        finally {
            await cache.removeAll();
        }
    }

    function getPrimitiveValue(typeCode) {
        return TestingHelper.primitiveValues[typeCode].values[0];
    }

    function getArrayValues(typeCode) {
        return TestingHelper.primitiveValues[TestingHelper.arrayValues[typeCode].elemType].values;
    }

    function getMapValue(typeCode) {
        const map = new Map();
        const values = TestingHelper.primitiveValues[typeCode].values;
        const length = values.length;
        values.forEach((value, index) => {
            if (!TestingHelper.primitiveValues[typeCode].isMapKey) {
                value = Util.format("%s", value);
            }
            map.set(value, values[length - index - 1]);
        });
        return map;
    }
});
