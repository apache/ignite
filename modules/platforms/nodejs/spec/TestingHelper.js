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
const JasmineReporters = require('jasmine-reporters');

const Util = require('util');
const exec = require('child_process').exec;
const config = require('./config');
const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const Errors = IgniteClient.Errors;
const EnumItem = IgniteClient.EnumItem;
const Timestamp = IgniteClient.Timestamp;
const Decimal = IgniteClient.Decimal;
const BinaryObject = IgniteClient.BinaryObject;
const ObjectType = IgniteClient.ObjectType;

const TIMEOUT_MS = 60000;

jasmine.getEnv().addReporter(new JasmineReporters.TeamCityReporter());

const dateComparator = (date1, date2) => { return !date1 && !date2 || date1.value === date2.value; };
const floatComparator = (date1, date2) => { return Math.abs(date1 - date2) < 0.00001; };
const defaultComparator = (value1, value2) => { return value1 === value2; };
const enumComparator = (value1, value2) => {
    return value1.getTypeId() === value2.getTypeId() &&
        value1.getOrdinal() === value2.getOrdinal(); };
const decimalComparator = (value1, value2) => {
    return value1 === null && value2 === null ||
        value1.equals(value2);
};
const timestampComparator = (value1, value2) => {
    return value1 === null && value2 === null ||
        dateComparator(value1.getTime(), value2.getTime()) &&
        value1.getNanos() === value2.getNanos(); };

const numericValueModificator = (data) => { return data > 0 ? data - 10 : data + 10; };
const charValueModificator = (data) => { return String.fromCharCode(data.charCodeAt(0) + 5); };
const booleanValueModificator = (data) => { return !data; };
const stringValueModificator = (data) => { return data + 'xxx'; };
const dateValueModificator = (data) => { return new Date(data.getTime() + 12345); };
const UUIDValueModificator = (data) => { return data.reverse(); };
const enumValueModificator = (data) => { return new EnumItem(data.getTypeId(), data.getOrdinal() + 1); };
const decimalValueModificator = (data) => { return data.add(12345); };
const timestampValueModificator = (data) => { return new Timestamp(new Date(data.getTime() + 12345), data.getNanos() + 123); };

const primitiveValues = {
    [ObjectType.PRIMITIVE_TYPE.BYTE] : { 
        values : [-128, 0, 127],
        isMapKey : true,
        modificator : numericValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.SHORT] : {
        values : [-32768, 0, 32767],
        isMapKey : true,
        modificator : numericValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.INTEGER] : {
        values : [12345, 0, -54321],
        isMapKey : true,
        modificator : numericValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.LONG] : {
        values : [12345678912345, 0, -98765432112345],
        isMapKey : true,
        modificator : numericValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.FLOAT] : {
        values : [-1.155, 0, 123e-5],
        isMapKey : false,
        modificator : numericValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.DOUBLE] : {
        values : [-123e5, 0, 0.0001],
        typeOptional : true,
        isMapKey : false,
        modificator : numericValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.CHAR] : {
        values : ['a', String.fromCharCode(0x1234)],
        isMapKey : true,
        modificator : charValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.BOOLEAN] : {
        values : [true, false],
        isMapKey : true,
        typeOptional : true,
        modificator : booleanValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.STRING] : {
        values : ['abc', ''],
        isMapKey : true,
        typeOptional : true,
        modificator : stringValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.UUID] : {
        values : [
            [ 18, 70, 2, 119, 154, 254, 198, 254, 195, 146, 33, 60, 116, 230, 0, 146 ],
            [ 141, 77, 31, 194, 127, 36, 184, 255, 192, 4, 118, 57, 253, 209, 111, 147 ]
        ],
        isMapKey : false,
        modificator : UUIDValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.DATE] : {
        values : [new Date(), new Date('1995-12-17T03:24:00'), new Date(0)],
        typeOptional : true,
        isMapKey : false,
        modificator : dateValueModificator
    },
    // [ObjectType.PRIMITIVE_TYPE.ENUM] : {
    //     values : [new EnumItem(12345, 7), new EnumItem(0, 0)],
    //     typeOptional : true,
    //     isMapKey : false,
    //     modificator : enumValueModificator
    // },
    [ObjectType.PRIMITIVE_TYPE.DECIMAL] : {
        values : [new Decimal('123456789.6789345'), new Decimal(0), new Decimal('-98765.4321e15')],
        typeOptional : true,
        isMapKey : false,
        modificator : decimalValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.TIMESTAMP] : {
        values : [new Timestamp(new Date().getTime(), 12345), new Timestamp(new Date('1995-12-17T03:24:00').getTime(), 543), new Timestamp(0, 0)],
        typeOptional : true,
        isMapKey : false,
        modificator : timestampValueModificator
    },
    [ObjectType.PRIMITIVE_TYPE.TIME] : {
        values : [new Date(), new Date('1995-12-17T03:24:00'), new Date(123)],
        isMapKey : false,
        modificator : dateValueModificator
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
    [ObjectType.PRIMITIVE_TYPE.UUID_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.UUID },
    [ObjectType.PRIMITIVE_TYPE.DATE_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.DATE, typeOptional : true },
    //[ObjectType.PRIMITIVE_TYPE.ENUM_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.ENUM, typeOptional : true },
    [ObjectType.PRIMITIVE_TYPE.DECIMAL_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.DECIMAL, typeOptional : true },
    [ObjectType.PRIMITIVE_TYPE.TIMESTAMP_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.TIMESTAMP, typeOptional : true },
    [ObjectType.PRIMITIVE_TYPE.TIME_ARRAY] : { elemType : ObjectType.PRIMITIVE_TYPE.TIME }
};

// Helper class for testing apache-ignite-client library.
// Contains common methods for testing environment initialization and cleanup.
class TestingHelper {
    static get TIMEOUT() {
        return TIMEOUT_MS;
    }

    static get primitiveValues() {
        return primitiveValues;
    }

    static get arrayValues() {
        return arrayValues;
    }

    // Initializes testing environment: creates and starts the library client, sets default jasmine test timeout.
    // Should be called from any test suite beforeAll method.
    static async init() {
        jasmine.DEFAULT_TIMEOUT_INTERVAL = TIMEOUT_MS;

        TestingHelper._igniteClient = new IgniteClient();
        TestingHelper._igniteClient.setDebug(config.debug);
        await TestingHelper._igniteClient.connect(new IgniteClientConfiguration(...config.endpoints));
    }

    // Cleans up testing environment.
    // Should be called from any test suite afterAll method.
    static async cleanUp() {
        await TestingHelper.igniteClient.disconnect();
    }

    static get igniteClient() {
        return TestingHelper._igniteClient;
    }

    static async destroyCache(cacheName, done) {
        try {
            await TestingHelper.igniteClient.destroyCache(cacheName);
        }
        catch (err) {
            TestingHelper.checkOperationError(err, done);
        }
    }

    static executeExample(name, outputChecker) {
        return new Promise((resolve, reject) => {
                exec('node ' + name, (error, stdout, stderr) => {
                    TestingHelper.logDebug(stdout);
                    resolve(stdout);
                })
            }).
            then(output => {
                expect(output).not.toMatch('ERROR:');
                expect(output).toMatch('Client is started');
            });
    }

    static checkOperationError(error, done) {
        TestingHelper.checkError(error, Errors.OperationError, done)
    }

    static checkIllegalArgumentError(error, done) {
        TestingHelper.checkError(error, Errors.IgniteClientError, done)
    }

    static checkError(error, errorType, done) {
        if (!(error instanceof errorType)) {
            done.fail('unexpected error: ' + error);
        }
    }

    static logDebug(message) {
        if (config.debug) {
            console.log(message);
        }
    }

    static printValue(value) {
        const val = Util.inspect(value, false, null);
        const length = 500;
        return val.length > length ? val.substr(0, length) + '...' : val;
    }

    static async compare(value1, value2) {
        TestingHelper.logDebug(Util.format('compare: %s and %s', TestingHelper.printValue(value1), TestingHelper.printValue(value2)));
        if (value1 === undefined || value2 === undefined) {
            TestingHelper.logDebug(Util.format('compare: unexpected "undefined" value'));
            return false;
        }
        if (value1 === null && value2 === null) {
            return true;
        }
        if (value1 === null && value2 !== null || value1 !== null && value2 === null) {
            return false;
        }
        if (typeof value1 !== typeof value2) {
            TestingHelper.logDebug(Util.format('compare: value types are different: %s and %s',
                typeof value1, typeof value2));
            return false;
        }
        if (typeof value1 === 'number') {
            return floatComparator(value1, value2);
        }
        else if (typeof value1 !== 'object') {
            return defaultComparator(value1, value2);
        }
        else if (value1.constructor.name !== value2.constructor.name && !value2 instanceof BinaryObject) {
            TestingHelper.logDebug(Util.format('compare: value types are different: %s and %s',
                value1.constructor.name, value2.constructor.name));
            return false;
        }
        else if (value1 instanceof Date && value2 instanceof Date) {
            return dateComparator(value1, value2);
        }
        else if (value1 instanceof EnumItem && value2 instanceof EnumItem) {
            return enumComparator(value1, value2);
        }
        else if (value1 instanceof Decimal && value2 instanceof Decimal) {
            return decimalComparator(value1, value2);
        }
        else if (value1 instanceof Timestamp && value2 instanceof Timestamp) {
            return timestampComparator(value1, value2);
        }
        else if (value1 instanceof Array && value2 instanceof Array) {
            if (value1.length !== value2.length) {
                TestingHelper.logDebug(Util.format('compare: array lengths are different'));
                return false;
            }
            for (var i = 0; i < value1.length; i++) {
                if (!await TestingHelper.compare(value1[i], value2[i])) {
                    TestingHelper.logDebug(Util.format('compare: array elements are different: %s, %s',
                        TestingHelper.printValue(value1[i]), TestingHelper.printValue(value2[i])));
                    return false;
                }
            }
            return true;
        }
        else if (value1 instanceof Map && value2 instanceof Map) {
            if (value1.size !== value2.size) {
                TestingHelper.logDebug(Util.format('compare: map sizes are different'));
                return false;
            }
            for (var [key, val] of value1) {
                if (!value2.has(key)) {
                    TestingHelper.logDebug(Util.format('compare: maps are different: %s key is absent', TestingHelper.printValue(key)));
                    return false;
                }
                if (!(await TestingHelper.compare(val, value2.get(key)))) {
                    TestingHelper.logDebug(Util.format('compare: map values are different: %s, %s',
                        TestingHelper.printValue(val), TestingHelper.printValue(value2.get(key))));
                    return false;
                }
            }
            return true;
        }
        else if (value1 instanceof Set && value2 instanceof Set) {
            if (value1.size !== value2.size) {
                TestingHelper.logDebug(Util.format('compare: set sizes are different'));
                return false;
            }
            const value1Arr = [...value1].sort();
            const value2Arr = [...value2].sort();
            if (!await TestingHelper.compare(value1Arr, value2Arr)) {
                TestingHelper.logDebug(Util.format('compare: sets are different: %s and %s',
                    TestingHelper.printValue(value1Arr), TestingHelper.printValue(value2Arr)));
                return false;
            }
            return true;
        }
        else if (value2 instanceof BinaryObject) {
            if (value1 instanceof BinaryObject) {
                if (value1.getTypeName() !== value2.getTypeName()) {
                    TestingHelper.logDebug(Util.format('compare: binary object type names are different'));
                    return false;
                }
                if (!await TestingHelper.compare(value1.getFieldNames(), value2.getFieldNames())) {
                    TestingHelper.logDebug(Util.format('compare: binary object field names are different'));
                    return false;
                }
                for (let fieldName of value1.getFieldNames()) {
                    if (!value1.hasField(fieldName) || !value2.hasField(fieldName) ||
                        !await TestingHelper.compare(await value1.getField(fieldName), await value1.getField(fieldName))) {
                        TestingHelper.logDebug(Util.format('compare: binary objects field "%s" values are different', fieldName));
                        return false;
                    }
                }
                return true;
            }
            else {
                let value;
                for (let key of Object.keys(value1)) {
                    value = await value2.getField(key);
                    if (!(await TestingHelper.compare(value1[key], value))) {
                        TestingHelper.logDebug(Util.format('compare: binary object values for key %s are different: %s and %s',
                            TestingHelper.printValue(key), TestingHelper.printValue(value1[key]), TestingHelper.printValue(value)));
                        return false;
                    }
                }
                return true;
            }
        }
        else {
            for (let key of Object.keys(value1)) {
                if (!(await TestingHelper.compare(value1[key], value2[key]))) {
                    TestingHelper.logDebug(Util.format('compare: object values for key %s are different: %s and %s',
                        TestingHelper.printValue(key), TestingHelper.printValue(value1[key]), TestingHelper.printValue(value2[key])));
                    return false;
                }
            }
            return true;
        }
    }    
}

module.exports = TestingHelper;
