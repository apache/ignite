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
        MAP : 25,
        BINARY_OBJECT : 27,
        NULL : 101,
        COMPLEX_OBJECT : 103
    },
    ObjectType.TYPE_CODE);


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
    [TYPE_CODE.NULL] : {
        NAME : 'null',
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
        return TYPE_INFO[BinaryUtils.getTypeCode(type)].NAME;
    }

    static isNullable(type) {
        return TYPE_INFO[BinaryUtils.getTypeCode(type)].NULLABLE === true;
    }

    static getTypeCode(type) {
        return type instanceof ObjectType ? type.typeCode : type;
    }

    static getObjectType(type, argName = null) {
        if (type === null || type instanceof ObjectType) {
            return type;
        }
        else {
            ArgumentChecker.hasValueFrom(type, argName, ObjectType.TYPE_CODE);
            return new ObjectType(type);
        }
    }

    static checkCompatibility(value, type) {
        if (!type) {
            return;
        }
        if (value === null && !BinaryUtils.isNullable(type)) {
            throw Errors.IgniteClientError.typeCastError(BinaryUtils.TYPE_CODE.NULL, type);
        }
        else if (value !== null && type.typeCode === BinaryUtils.TYPE_CODE.NULL) {
            throw Errors.IgniteClientError.typeCastError('not null', BinaryUtils.TYPE_CODE.NULL);
        }
    }

    static checkTypesComatibility(expectedType, actualTypeCode) {
        if (expectedType === null) {
            return;
        }
        if (actualTypeCode === BinaryUtils.TYPE_CODE.NULL) {
            if (!BinaryUtils.isNullable(expectedType)) {
                throw Errors.IgniteClientError.typeCastError(BinaryUtils.TYPE_CODE.NULL, expectedType);
            }
        }
        else if (actualTypeCode !== expectedType.typeCode) {
            throw Errors.IgniteClientError.typeCastError(actualTypeCode, expectedType);
        }
    }

    static getArrayElementType(arrayTypeCode) {
        const elementType = TYPE_INFO[arrayTypeCode].ELEMENT_TYPE;
        if (!elementType) {
            throw Errors.IgniteClientError.internalError();
        }
        return BinaryUtils.getObjectType(elementType);
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
            case BinaryUtils.TYPE_CODE.UUID:
                return BinaryUtils.TYPE_CODE.UUID_ARRAY;
            case BinaryUtils.TYPE_CODE.DATE:
                return BinaryUtils.TYPE_CODE.DATE_ARRAY;
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                return BinaryUtils.TYPE_CODE.BINARY_OBJECT_ARRAY;
            default:
                throw Errors.IgniteClientError.internalError();
        }
    }

    static keepArrayElementType(arrayTypeCode) {
        return TYPE_INFO[arrayTypeCode].KEEP_ELEMENT_TYPE === true;
    }

    static hashCode(str) {
        let hash = 0, char;
        if (str.length > 0) {
            for (let i = 0; i < str.length; i++) {
                char = str.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash |= 0; // Convert to 32bit integer
            }
        }
        return hash;
    }
}

module.exports = BinaryUtils;
