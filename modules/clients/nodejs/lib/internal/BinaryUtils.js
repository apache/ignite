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

const ObjectType = require('../ObjectType').ObjectType;
const CompositeType = require('../ObjectType').CompositeType;
const MapObjectType = require('../ObjectType').MapObjectType;
const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const Errors = require('../Errors');
const ArgumentChecker = require('./ArgumentChecker');

// Operation codes
const OPERATION = Object.freeze({
    CACHE_GET : 1000,
    CACHE_PUT : 1001,
    CACHE_PUT_IF_ABSENT : 1002,
    CACHE_GET_ALL : 1003,
    CACHE_PUT_ALL : 1004,
    CACHE_GET_AND_PUT : 1005,
    CACHE_GET_AND_REPLACE : 1006,
    CACHE_GET_AND_REMOVE : 1007,
    CACHE_GET_AND_PUT_IF_ABSENT : 1008,
    CACHE_REPLACE : 1009,
    CACHE_REPLACE_IF_EQUALS : 1010,
    CACHE_CONTAINS_KEY : 1011,
    CACHE_CONTAINS_KEYS : 1012,
    CACHE_CLEAR : 1013,
    CACHE_CLEAR_KEY : 1014,
    CACHE_CLEAR_KEYS : 1015,
    CACHE_REMOVE_KEY : 1016,
    CACHE_REMOVE_IF_EQUALS : 1017,
    CACHE_REMOVE_KEYS : 1018,
    CACHE_REMOVE_ALL : 1019,
    CACHE_GET_SIZE : 1020,
    CACHE_GET_NAMES : 1050,
    CACHE_CREATE_WITH_NAME : 1051,
    CACHE_GET_OR_CREATE_WITH_NAME : 1052,
    CACHE_CREATE_WITH_CONFIGURATION : 1053,
    CACHE_GET_OR_CREATE_WITH_CONFIGURATION : 1054,
    CACHE_GET_CONFIGURATION : 1055,
    CACHE_DESTROY : 1056
});

const TYPE_CODE = Object.assign({
        BINARY_OBJECT : 27
    },
    ObjectType.PRIMITIVE_TYPE,
    ObjectType.COMPOSITE_TYPE);


const TYPE_INFO = Object.freeze({
    [TYPE_CODE.BYTE] : {
        NAME : 'byte',
        SIZE : 1
    },
    [TYPE_CODE.SHORT] : {
        NAME : 'short',
        SIZE : 2
    },
    [TYPE_CODE.INTEGER] : {
        NAME : 'integer',
        SIZE : 4
    },
    [TYPE_CODE.LONG] : {
        NAME : 'long',
        SIZE : 8
    },
    [TYPE_CODE.FLOAT] : {
        NAME : 'float',
        SIZE : 4
    },
    [TYPE_CODE.DOUBLE] : {
        NAME : 'double',
        SIZE : 8
    },
    [TYPE_CODE.CHAR] : {
        NAME : 'char',
        SIZE : 2
    },
    [TYPE_CODE.BOOLEAN] : {
        NAME : 'boolean',
        SIZE : 1
    },
    [TYPE_CODE.STRING] : {
        NAME : 'string',
        NULLABLE : true
    },
    [TYPE_CODE.DATE] : {
        NAME : 'date',
        SIZE : 8,
        NULLABLE : true
    },
    [TYPE_CODE.BYTE_ARRAY] : {
        NAME : 'byte array',
        ELEMENT_TYPE : TYPE_CODE.BYTE,
        NULLABLE : true
    },
    [TYPE_CODE.SHORT_ARRAY] : {
        NAME : 'short array',
        ELEMENT_TYPE : TYPE_CODE.SHORT,
        NULLABLE : true
    },
    [TYPE_CODE.INTEGER_ARRAY] : {
        NAME : 'integer array',
        ELEMENT_TYPE : TYPE_CODE.INTEGER,
        NULLABLE : true
    },
    [TYPE_CODE.LONG_ARRAY] : {
        NAME : 'long array',
        ELEMENT_TYPE : TYPE_CODE.LONG,
        NULLABLE : true
    },
    [TYPE_CODE.FLOAT_ARRAY] : {
        NAME : 'float array',
        ELEMENT_TYPE : TYPE_CODE.FLOAT,
        NULLABLE : true
    },
    [TYPE_CODE.DOUBLE_ARRAY] : {
        NAME : 'double array',
        ELEMENT_TYPE : TYPE_CODE.DOUBLE,
        NULLABLE : true
    },
    [TYPE_CODE.CHAR_ARRAY] :  {
        NAME : 'char array',
        ELEMENT_TYPE : TYPE_CODE.CHAR,
        NULLABLE : true
    },
    [TYPE_CODE.BOOLEAN_ARRAY] :  {
        NAME : 'boolean array',
        ELEMENT_TYPE : TYPE_CODE.BOOLEAN,
        NULLABLE : true
    },
    [TYPE_CODE.STRING_ARRAY] :  {
        NAME : 'string array',
        ELEMENT_TYPE : TYPE_CODE.STRING,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.DATE_ARRAY] :  {
        NAME : 'date array',
        ELEMENT_TYPE : TYPE_CODE.DATE,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.MAP] : {
        NAME : 'map',
        NULLABLE : true
    },
    [TYPE_CODE.BINARY_OBJECT] : {
        NAME : 'BinaryObject',
        NULLABLE : true
    },
    [TYPE_CODE.NULL] : {
        NAME : 'null',
        NULLABLE : true
    },
    [TYPE_CODE.COMPLEX_OBJECT] : {
        NAME : 'complex object',
        NULLABLE : true
    }
});

const UTF8_ENCODING = 'utf8';

class BinaryUtils {
    static get OPERATION() {
        return OPERATION;
    }

    static get TYPE_CODE() {
        return TYPE_CODE;
    }

    static get TYPE_INFO() {
        return TYPE_INFO;
    }

    static getSize(typeCode) {
        const size = TYPE_INFO[typeCode].SIZE;
        return size ? size : 0;
    }

    static get ENCODING() {
        return UTF8_ENCODING;
    }

    static getTypeName(type) {
        if (typeof type === 'string') {
            return type;
        }
        const typeCode = BinaryUtils.getTypeCode(type);
        return TYPE_INFO[typeCode] ? TYPE_INFO[typeCode].NAME : 'code ' + typeCode;
    }

    static isNullable(type) {
        return TYPE_INFO[BinaryUtils.getTypeCode(type)].NULLABLE === true;
    }

    static getTypeCode(type) {
        return type instanceof CompositeType ? type._typeCode : type;
    }

    static getObjectType(type, argName, checkType = true) {
        if (type === null || type instanceof CompositeType) {
            return type;
        }
        else {
            if (checkType) {
                ArgumentChecker.hasValueFrom(type, argName, ObjectType.PRIMITIVE_TYPE);
            }
            switch (type) {
                case BinaryUtils.TYPE_CODE.MAP:
                    return new MapObjectType();
                case BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                    return new ComplexObjectType();
                default:
                    return type;
            }
        }
    }

    static calcObjectTypeCode(object) {
        const BinaryObject = require('../BinaryObject');
        const objectType = typeof object;
        if (object === null) {
            return BinaryUtils.TYPE_CODE.NULL;
        }
        else if (objectType === 'number') {
            return BinaryUtils.TYPE_CODE.DOUBLE;
        }
        else if (objectType === 'string') {
            return BinaryUtils.TYPE_CODE.STRING;
        }
        else if (objectType === 'boolean') {
            return BinaryUtils.TYPE_CODE.BOOLEAN;
        }
        else if (object instanceof Date) {
            return BinaryUtils.TYPE_CODE.DATE;
        }
        else if (object instanceof Array) {
            if (object.length > 0 && object[0] !== null) {
                return BinaryUtils.getArrayTypeCode(BinaryUtils.calcObjectTypeCode(object[0]));
            }
        }
        else if (object instanceof Map) {
            return BinaryUtils.TYPE_CODE.MAP;
        }
        else if (object instanceof BinaryObject) {
            return BinaryUtils.TYPE_CODE.BINARY_OBJECT;
        }
        else if (objectType === 'object') {
            return BinaryUtils.TYPE_CODE.COMPLEX_OBJECT;
        }
        throw Errors.IgniteClientError.unsupportedTypeError(objectType);
    }

    static checkCompatibility(value, type) {
        if (!type) {
            return;
        }
        const typeCode = BinaryUtils.getTypeCode(type);
        if (value === null && !BinaryUtils.isNullable(typeCode)) {
            throw Errors.IgniteClientError.typeCastError(BinaryUtils.TYPE_CODE.NULL, typeCode);
        }
        else if (BinaryUtils.isStandardType(typeCode)) {
            BinaryUtils.checkStandardTypeCompatibility(value, typeCode);
            return;
        }
        const valueTypeCode = BinaryUtils.calcObjectTypeCode(value);
        if (typeCode === BinaryUtils.TYPE_CODE.BINARY_OBJECT &&
            valueTypeCode === BinaryUtils.TYPE_CODE.COMPLEX_OBJECT ||
            typeCode !== BinaryUtils.TYPE_CODE.BINARY_OBJECT &&
            valueTypeCode === BinaryUtils.TYPE_CODE.BINARY_OBJECT) {
            throw Errors.IgniteClientError.typeCastError(valueTypeCode, typeCode);
        }
    }

    static isStandardType(typeCode) {
        return typeCode !== BinaryUtils.TYPE_CODE.BINARY_OBJECT &&
            typeCode !== BinaryUtils.TYPE_CODE.COMPLEX_OBJECT;
    }

    static checkStandardTypeCompatibility(value, typeCode) {
        switch (typeCode) {
            case BinaryUtils.TYPE_CODE.BYTE_ARRAY:
            case BinaryUtils.TYPE_CODE.SHORT_ARRAY:
            case BinaryUtils.TYPE_CODE.INTEGER_ARRAY:
            case BinaryUtils.TYPE_CODE.LONG_ARRAY:
            case BinaryUtils.TYPE_CODE.FLOAT_ARRAY:
            case BinaryUtils.TYPE_CODE.DOUBLE_ARRAY:
            case BinaryUtils.TYPE_CODE.CHAR_ARRAY:
            case BinaryUtils.TYPE_CODE.BOOLEAN_ARRAY:
            case BinaryUtils.TYPE_CODE.STRING_ARRAY:
            case BinaryUtils.TYPE_CODE.DATE_ARRAY:
                if (!value instanceof Array) {
                    throw Errors.IgniteClientError.typeCastError('not Array', typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.MAP:
                if (!value instanceof Map) {
                    throw Errors.IgniteClientError.typeCastError('not Map', typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.NULL:
                if (value !== null) {
                    throw Errors.IgniteClientError.typeCastError('not null', typeCode);
                }
                return;
            default:
                const valueTypeCode = BinaryUtils.calcObjectTypeCode(value);
                if (valueTypeCode === BinaryUtils.TYPE_CODE.BINARY_OBJECT) {
                    throw Errors.IgniteClientError.typeCastError(valueTypeCode, typeCode);
                }
                return;
        }
    }

    static checkTypesComatibility(expectedType, actualTypeCode) {
        if (expectedType === null) {
            return;
        }
        const expectedTypeCode = BinaryUtils.getTypeCode(expectedType);
        if (actualTypeCode === BinaryUtils.TYPE_CODE.NULL) {
            if (!BinaryUtils.isNullable(expectedTypeCode)) {
                throw Errors.IgniteClientError.typeCastError(BinaryUtils.TYPE_CODE.NULL, expectedTypeCode);
            }
        }
        else if (expectedTypeCode === BinaryUtils.TYPE_CODE.BINARY_OBJECT ||
            actualTypeCode === BinaryUtils.TYPE_CODE.BINARY_OBJECT &&
            expectedTypeCode === BinaryUtils.TYPE_CODE.COMPLEX_OBJECT) {
            return;
        }
        else if (actualTypeCode !== expectedTypeCode) {
            throw Errors.IgniteClientError.typeCastError(actualTypeCode, expectedTypeCode);
        }
    }

    static getArrayElementTypeCode(arrayTypeCode) {
        const elementTypeCode = TYPE_INFO[arrayTypeCode].ELEMENT_TYPE;
        if (!elementTypeCode) {
            throw Errors.IgniteClientError.internalError();
        }
        return elementTypeCode;
    }

    static getArrayTypeCode(elementTypeCode) {
        switch (elementTypeCode) {
            case BinaryUtils.TYPE_CODE.BYTE:
                return BinaryUtils.TYPE_CODE.BYTE_ARRAY;
            case BinaryUtils.TYPE_CODE.SHORT:
                return BinaryUtils.TYPE_CODE.SHORT_ARRAY;
            case BinaryUtils.TYPE_CODE.INTEGER:
                return BinaryUtils.TYPE_CODE.INTEGER_ARRAY;
            case BinaryUtils.TYPE_CODE.LONG:
                return BinaryUtils.TYPE_CODE.LONG_ARRAY;
            case BinaryUtils.TYPE_CODE.FLOAT:
                return BinaryUtils.TYPE_CODE.FLOAT_ARRAY;
            case BinaryUtils.TYPE_CODE.DOUBLE:
                return BinaryUtils.TYPE_CODE.DOUBLE_ARRAY;
            case BinaryUtils.TYPE_CODE.CHAR:
                return BinaryUtils.TYPE_CODE.CHAR_ARRAY;
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                return BinaryUtils.TYPE_CODE.BOOLEAN_ARRAY;
            case BinaryUtils.TYPE_CODE.STRING:
                return BinaryUtils.TYPE_CODE.STRING_ARRAY;
            case BinaryUtils.TYPE_CODE.DATE:
                return BinaryUtils.TYPE_CODE.DATE_ARRAY;
            default:
                throw Errors.IgniteClientError.internalError();
        }
    }

    static keepArrayElementType(arrayTypeCode) {
        return TYPE_INFO[arrayTypeCode].KEEP_ELEMENT_TYPE === true;
    }

    static hashCode(str) {
        let hash = 0, char;
        if (str && str.length > 0) {
            for (let i = 0; i < str.length; i++) {
                char = str.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash |= 0; // Convert to 32bit integer
            }
        }
        return hash;
    }

    static contentHashCode(buffer, startPos, endPos) {
        let hash = 1;
        for (let i = startPos; i <= endPos; i++) {
            hash = 31 * hash + buffer._buffer[i];
            hash |= 0; // Convert to 32bit integer
        }
        return hash;
    }
}

class BinaryObjectType extends CompositeType {
    constructor(innerType = null) {
        super(BinaryUtils.TYPE_CODE.BINARY_OBJECT);
        this._innerType = innerType;
    }

    get innerType() {
        return this._innerType;
    }
}

module.exports = BinaryUtils;
module.exports.BinaryObjectType = BinaryObjectType;
