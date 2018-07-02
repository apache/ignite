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
const CollectionObjectType = IgniteClient.CollectionObjectType;
const ObjectArrayType = IgniteClient.ObjectArrayType;
const ComplexObjectType = IgniteClient.ComplexObjectType;
const EnumItem = IgniteClient.EnumItem;
const Timestamp = IgniteClient.Timestamp;
const Decimal = IgniteClient.Decimal;
const BinaryObject = IgniteClient.BinaryObject;

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
                for (let type1 of Object.keys(TestingHelper.primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = TestingHelper.primitiveValues[type1];
                    for (let type2 of Object.keys(TestingHelper.primitiveValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = TestingHelper.primitiveValues[type2];
                        const modificator = typeInfo2.modificator;
                        for (let value1 of typeInfo1.values) {
                            for (let value2 of typeInfo2.values) {
                                await putGetPrimitiveValues(type1, type2, value1, value2, modificator);
                                if (typeInfo1.typeOptional) {
                                    await putGetPrimitiveValues(null, type2, value1, value2, modificator);
                                }
                                if (typeInfo2.typeOptional) {
                                    await putGetPrimitiveValues(type1, null, value1, value2, modificator);
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
                for (let type of Object.keys(TestingHelper.arrayValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.arrayValues[type];
                    const primitiveType = typeInfo.elemType;
                    const values = TestingHelper.primitiveValues[primitiveType].values;
                    await putGetArrays(primitiveType, type, values[0], values);
                    await putGetArrays(primitiveType, type, values[0], []);
                    if (typeInfo.typeOptional) {
                        await putGetArrays(primitiveType, null, values[0], values);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get maps of different key/value types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type1 of Object.keys(TestingHelper.primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = TestingHelper.primitiveValues[type1];
                    if (!typeInfo1.isMapKey) {
                        continue;
                    }
                    for (let type2 of Object.keys(TestingHelper.primitiveValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = TestingHelper.primitiveValues[type2];
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
                            map);
                        await putGetMaps(
                            new MapObjectType(
                                MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, type1, type2), 
                            map);
                        if (typeInfo1.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, null, type2),
                            map);
                        }
                        if (typeInfo2.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, null),
                            map);
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
                for (let type1 of Object.keys(TestingHelper.primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = TestingHelper.primitiveValues[type1];
                    if (!typeInfo1.isMapKey) {
                        continue;
                    }
                    for (let type2 of Object.keys(TestingHelper.arrayValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = TestingHelper.arrayValues[type2];
                        const primitiveType2 = typeInfo2.elemType;
                        const values2 = TestingHelper.primitiveValues[primitiveType2].values;
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
                            map);
                        await putGetMaps(
                            new MapObjectType(MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, type1, type2), 
                            map);
                        if (typeInfo1.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, null, type2),
                            map);
                        }
                        if (typeInfo2.typeOptional) {
                            await putGetMaps(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, null),
                            map);
                        }
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get sets of different key/value types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    const set = new Set();
                    for (let value of typeInfo.values) {
                        set.add(value);
                    }
                    await putGetSets(
                        new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.USER_SET, type),
                            set);
                    await putGetSets(
                        new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.HASH_SET, type),
                            set);
                    await putGetSets(
                        new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.LINKED_HASH_SET, type),
                            set);
                    if (typeInfo.typeOptional) {
                        await putGetSets(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.LINKED_HASH_SET),
                            set);
                        await putGetSets(null, set);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get lists of different key/value types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    const list = new Array();
                    for (let value of typeInfo.values) {
                        list.push(value);
                    }
                    await putGetLists(
                        new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.USER_COL, type),
                            list);
                    await putGetLists(
                        new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.ARRAY_LIST, type),
                            list);
                    await putGetLists(
                        new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.LINKED_LIST, type),
                            list);
                    if (typeInfo.typeOptional) {
                        await putGetLists(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.ARRAY_LIST),
                            list);
                    }
                    // const singletonList = [typeInfo.values[0]];
                    // await putGetLists(
                    //     new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.SINGLETON_LIST, type),
                    //         singletonList);
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of maps', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type1 of Object.keys(TestingHelper.primitiveValues)) {
                    type1 = parseInt(type1);
                    const typeInfo1 = TestingHelper.primitiveValues[type1];
                    if (!typeInfo1.isMapKey) {
                        continue;
                    }
                    for (let type2 of Object.keys(TestingHelper.primitiveValues)) {
                        type2 = parseInt(type2);
                        const typeInfo2 = TestingHelper.primitiveValues[type2];
                        let map = new Map();
                        let index2 = 0;
                        for (let value1 of typeInfo1.values) {
                            let value2 = typeInfo2.values[index2];
                            index2++;
                            if (index2 >= typeInfo2.values.length) {
                                index2 = 0;
                            }
                            map.set(value1, value2);
                        }
                        const array = new Array();
                        for (let i = 0; i < 10; i++) {
                            map = new Map([...map.entries()].map(([key, value]) => [typeInfo1.modificator(key), typeInfo2.modificator(value)]));
                            array.push(map);
                        }
                        await putGetObjectArrays(new ObjectArrayType(new MapObjectType(MapObjectType.MAP_SUBTYPE.HASH_MAP, type1, type2)), 
                            array);
                        if (typeInfo1.typeOptional) {
                            await putGetObjectArrays(new ObjectArrayType(new MapObjectType(MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, null, type2)), 
                                array);
                        }
                        if (typeInfo2.typeOptional) {
                            await putGetObjectArrays(new ObjectArrayType(new MapObjectType(MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP, type1)), 
                                array);
                        }
                        if (typeInfo1.typeOptional && typeInfo2.typeOptional) {
                            await putGetObjectArrays(new ObjectArrayType(), array);
                            await putGetObjectArrays(null, array);
                        }
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of primitive types', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    let value = typeInfo.values[0];
                    const array = new Array();
                    for (let i = 0; i < 10; i++) {
                        value = typeInfo.modificator(value);
                        array.push(value);
                    }
                    await putGetObjectArrays(
                        new ObjectArrayType(type), 
                        array);
                    if (typeInfo.typeOptional) {
                        await putGetObjectArrays(new ObjectArrayType(), array);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of primitive arrays', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(TestingHelper.arrayValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.arrayValues[type];
                    const primitiveType = typeInfo.elemType;
                    const primitiveTypeInfo = TestingHelper.primitiveValues[primitiveType];
                    let values = primitiveTypeInfo.values;
                    
                    const array = new Array();
                    for (let i = 0; i < 10; i++) {
                        values = values.map((value) => primitiveTypeInfo.modificator(value));
                        array.push(values);
                    }
                    await putGetObjectArrays(
                        new ObjectArrayType(type), 
                        array);
                    if (typeInfo.typeOptional) {
                        await putGetObjectArrays(new ObjectArrayType(), array);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of sets', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    let set = new Set();
                    for (let value of typeInfo.values) {
                        set.add(value);
                    }
                    const array = new Array();
                    for (let i = 0; i < 10; i++) {
                        set = new Set([...set].map((value) => typeInfo.modificator(value)));
                        array.push(set);
                    }
                    await putGetObjectArrays(
                        new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.USER_SET, type)), 
                        array);
                    await putGetObjectArrays(
                        new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.HASH_SET, type)), 
                        array);
                    await putGetObjectArrays(
                        new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.LINKED_HASH_SET, type)), 
                        array);
                    if (typeInfo.typeOptional) {
                        await putGetObjectArrays(new ObjectArrayType(), array);
                        await putGetObjectArrays(null, array);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of lists', (done) => {
        Promise.resolve().
            then(async () => {
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    let list = new Array();
                    for (let value of typeInfo.values) {
                        list.push(value);
                    }
                    const array = new Array();
                    for (let i = 0; i < 10; i++) {
                        list = list.map((value) => typeInfo.modificator(value));
                        array.push(list);
                    }
                    await putGetObjectArrays(
                        new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.USER_COL, type)), 
                        array);
                    await putGetObjectArrays(
                        new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.ARRAY_LIST, type)), 
                        array);
                    await putGetObjectArrays(
                        new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.LINKED_LIST, type)), 
                        array);
                    if (typeInfo.typeOptional) {
                        await putGetObjectArrays(
                            new ObjectArrayType(new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.ARRAY_LIST)), 
                            array);
                    }
                }
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of complex objects', (done) => {
        Promise.resolve().
            then(async () => {
                let object = {};
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    object['field' + type] = typeInfo.values[0];
                }
                const objectType = new ComplexObjectType(object);
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    objectType.setFieldType('field' + type, type);
                }

                let array = new Array();
                for (let i = 0; i < 5; i++) {
                    for (let field in object) {
                        const type = parseInt(field.substring(5));
                        object[field] = TestingHelper.primitiveValues[type].modificator(object[field]);
                    }
                    array.push(object);
                }
                await putGetObjectArrays(
                    new ObjectArrayType(objectType), array);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of complex objects with default field types', (done) => {
        Promise.resolve().
            then(async () => {
                let object = {};
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    if (typeInfo.typeOptional) {
                        object['field' + type] = typeInfo.values[0];
                    }
                }
                const objectType = new ComplexObjectType(object, 'tstComplObjectWithDefaultFieldTypes');
                let array = new Array();
                for (let i = 0; i < 5; i++) {
                    for (let field in object) {
                        const type = parseInt(field.substring(5));
                        object[field] = TestingHelper.primitiveValues[type].modificator(object[field]);
                    }
                    array.push(object);
                }
                await putGetObjectArrays(
                    new ObjectArrayType(objectType), array);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of binary objects', (done) => {
        Promise.resolve().
            then(async () => {
                let binObject = new BinaryObject('tstBinaryObj');
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    binObject.setField('field' + type, typeInfo.values[0], type);
                }
                let array = new Array();
                for (let i = 0; i < 5; i++) {
                    for (let field of binObject.getFieldNames()) {
                        const type = parseInt(field.substring(5));
                        binObject.setField(
                            'field' + type,
                            TestingHelper.primitiveValues[type].modificator(await binObject.getField('field' + type)),
                            type);
                    }
                    array.push(binObject);
                }
                await putGetObjectArrays(new ObjectArrayType(), array);
                await putGetObjectArrays(null, array);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    it('put get object array of object arrays', (done) => {
        Promise.resolve().
            then(async () => {
                let object = {};
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    const typeInfo = TestingHelper.primitiveValues[type];
                    object['field' + type] = typeInfo.values[0];
                }
                const objectType = new ComplexObjectType(object);
                for (let type of Object.keys(TestingHelper.primitiveValues)) {
                    type = parseInt(type);
                    objectType.setFieldType('field' + type, type);
                }

                let array = new Array();
                for (let i = 0; i < 2; i++) {
                    let innerArray = new Array();
                    for (let j = 0; j < 2; j++) {
                        for (let field in object) {
                            const type = parseInt(field.substring(5));
                            object[field] = TestingHelper.primitiveValues[type].modificator(object[field]);
                        }
                        innerArray.push(object);
                    }
                    array.push(innerArray);
                }
                await putGetObjectArrays(
                    new ObjectArrayType(new ObjectArrayType(objectType)), array);
            }).
            then(done).
            catch(error => done.fail(error));
    });

    async function putGetPrimitiveValues(keyType, valueType, key, value, modificator) {
        const cache = await igniteClient.getCache(CACHE_NAME).
            setKeyType(keyType).
            setValueType(valueType);
        try {
            await putGetPrimitive(cache, key, keyType, value, valueType);
            const newValue = modificator(value);
            await putGetPrimitive(cache, key, keyType, newValue, valueType);
        }
        finally {
            await cache.removeAll();
        }
    }

    async function putGetPrimitive(cache, key, keyType, value, valueType) {
        await cache.put(key, value);
        let result = await cache.get(key);
        expect(await TestingHelper.compare(value, result)).toBe(true,
            `values are not equal: keyType=${keyType}, key=${key}, valueType=${valueType}, put value=${value}, get value=${result}`);
    }

    async function putGetArrays(keyType, valueType, key, value) {
        const cache = await igniteClient.getCache(CACHE_NAME).
            setKeyType(keyType).
            setValueType(valueType);
        try {
            await cache.put(key, value);
            let result = await cache.get(key);
            await cache.clearKey(key);
            expect(result instanceof Array).toBe(true,
                `result is not Array: arrayType=${valueType}, result=${result}`);
            expect(await TestingHelper.compare(value, result)).toBe(true, 
                `Arrays are not equal: arrayType=${valueType}, put array=${TestingHelper.printValue(value)
                }, get array=${TestingHelper.printValue(result)}`);
        }
        finally {
            await cache.removeAll();
        }
    }

    async function putGetMaps(mapType, value) {
        const key = new Date();
        const cache = await igniteClient.getCache(CACHE_NAME).
            setValueType(mapType);
        await cache.put(key, value);
        let result = await cache.get(key);
        expect(result instanceof Map).toBe(true,
            `result is not Map: mapType=${mapType}, result=${result}`);
        expect(await TestingHelper.compare(value, result)).toBe(true, 
            `Maps are not equal: valueType=${mapType._valueType}, put value=${TestingHelper.printValue(value)
            }, get value=${TestingHelper.printValue(result)}`);
    }

    async function putGetSets(setType, value) {
        const key = new Date();
        const cache = await igniteClient.getCache(CACHE_NAME).
            setValueType(setType);
        await cache.put(key, value);
        let result = await cache.get(key);

        expect(result instanceof Set).toBe(true,
            `result is not Set: setType=${setType}, result=${result}`);
        if (!setType || setType._subType !== CollectionObjectType.COLLECTION_SUBTYPE.LINKED_HASH_SET) {
            const valueArr = [...value].sort();
            const resultArr = [...result].sort();
            expect(await TestingHelper.compare(valueArr, resultArr)).toBe(true, `Sets are not equal: valueType=${setType ? setType._elementType : 
                            null}, put value=${TestingHelper.printValue(valueArr)}, get value=${TestingHelper.printValue(resultArr)}`);
        }
        else {
            expect(await TestingHelper.compare(value, result)).toBe(true, `Sets are not equal: valueType=${setType ? setType._elementType : 
                            null}, put value=${TestingHelper.printValue(value)}, get value=${TestingHelper.printValue(result)}`);
        }
    }

    async function putGetLists(listType, value) {
        const key = new Date();
        const cache = await igniteClient.getCache(CACHE_NAME).
            setValueType(listType);
        await cache.put(key, value);
        let result = await cache.get(key);
        expect(result instanceof Array).toBe(true,
            `result is not Array: listType=${listType}, result=${result}`);
        expect(await TestingHelper.compare(value, result)).toBe(true, `Lists are not equal: valueType=${listType ? listType._elementType : 
                        null}, put value=${TestingHelper.printValue(value)}, get value=${TestingHelper.printValue(result)}`);
    }

    async function putGetObjectArrays(arrayType, value) {
        const key = new Date();
        const cache = await igniteClient.getCache(CACHE_NAME).
            setValueType(arrayType);
        await cache.put(key, value);
        let result = await cache.get(key);
        expect(result instanceof Array).toBe(true,
            `result is not Array: arrayType=${arrayType}, result=${result}`);
        expect(await TestingHelper.compare(value, result)).toBe(true, `Arrays are not equal: valueType=${arrayType ? arrayType._elementType : 
                        null}, put value=${TestingHelper.printValue(value)}, get value=${TestingHelper.printValue(result)}`);
    }

    async function testSuiteCleanup(done) {
        await TestingHelper.destroyCache(CACHE_NAME, done);
    }
});
