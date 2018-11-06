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

const Decimal = require('decimal.js');
const ObjectType = require('../ObjectType').ObjectType;
const CompositeType = require('../ObjectType').CompositeType;
const MapObjectType = require('../ObjectType').MapObjectType;
const CollectionObjectType = require('../ObjectType').CollectionObjectType;
const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const ObjectArrayType = require('../ObjectType').ObjectArrayType;
const Timestamp = require('../Timestamp');
const EnumItem = require('../EnumItem');
const Errors = require('../Errors');
const ArgumentChecker = require('./ArgumentChecker');

// Operation codes
const OPERATION = Object.freeze({
    // Key-Value Queries
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
    // Cache Configuration
    CACHE_GET_NAMES : 1050,
    CACHE_CREATE_WITH_NAME : 1051,
    CACHE_GET_OR_CREATE_WITH_NAME : 1052,
    CACHE_CREATE_WITH_CONFIGURATION : 1053,
    CACHE_GET_OR_CREATE_WITH_CONFIGURATION : 1054,
    CACHE_GET_CONFIGURATION : 1055,
    CACHE_DESTROY : 1056,
    // SQL and Scan Queries
    QUERY_SCAN : 2000,
    QUERY_SCAN_CURSOR_GET_PAGE : 2001,
    QUERY_SQL : 2002,
    QUERY_SQL_CURSOR_GET_PAGE : 2003,
    QUERY_SQL_FIELDS : 2004,
    QUERY_SQL_FIELDS_CURSOR_GET_PAGE : 2005,
    RESOURCE_CLOSE : 0,
    // Binary Types
    GET_BINARY_TYPE : 3002,
    PUT_BINARY_TYPE : 3003
});

const TYPE_CODE = Object.assign({
        BINARY_OBJECT : 27,
        BINARY_ENUM : 38
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
    [TYPE_CODE.UUID] : {
        NAME : 'UUID',
        SIZE : 16,
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
    [TYPE_CODE.UUID_ARRAY] :  {
        NAME : 'UUID array',
        ELEMENT_TYPE : TYPE_CODE.UUID,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.OBJECT_ARRAY] : {
        NAME : 'object array',
        ELEMENT_TYPE : TYPE_CODE.COMPLEX_OBJECT,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.COLLECTION] : {
        NAME : 'collection',
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
    [TYPE_CODE.ENUM] : {
        NAME : 'enum',
        NULLABLE : true
    },
    [TYPE_CODE.ENUM_ARRAY] :  {
        NAME : 'enum array',
        ELEMENT_TYPE : TYPE_CODE.ENUM,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.DECIMAL] : {
        NAME : 'decimal',
        NULLABLE : true
    },
    [TYPE_CODE.DECIMAL_ARRAY] :  {
        NAME : 'decimal array',
        ELEMENT_TYPE : TYPE_CODE.DECIMAL,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.TIMESTAMP] : {
        NAME : 'timestamp',
        NULLABLE : true
    },
    [TYPE_CODE.TIMESTAMP_ARRAY] :  {
        NAME : 'timestamp array',
        ELEMENT_TYPE : TYPE_CODE.TIMESTAMP,
        KEEP_ELEMENT_TYPE : true,
        NULLABLE : true
    },
    [TYPE_CODE.TIME] : {
        NAME : 'time',
        NULLABLE : true
    },
    [TYPE_CODE.TIME_ARRAY] :  {
        NAME : 'time array',
        ELEMENT_TYPE : TYPE_CODE.TIME,
        KEEP_ELEMENT_TYPE : true,
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
        return TYPE_INFO[typeCode] ? TYPE_INFO[typeCode].NAME : 'type code ' + typeCode;
    }

    static isNullable(type) {
        return TYPE_INFO[BinaryUtils.getTypeCode(type)].NULLABLE === true;
    }

    static getTypeCode(type) {
        return type instanceof CompositeType ? type._typeCode : type;
    }

    static checkObjectType(type, argName) {
        if (type === null || type instanceof CompositeType) {
            return;
        }
        ArgumentChecker.hasValueFrom(type, argName, false, ObjectType.PRIMITIVE_TYPE);
    }

    static calcObjectType(object) {
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
        else if (object instanceof Timestamp) {
            return BinaryUtils.TYPE_CODE.TIMESTAMP;
        }
        else if (object instanceof Date) {
            return BinaryUtils.TYPE_CODE.DATE;
        }
        else if (object instanceof EnumItem) {
            return BinaryUtils.TYPE_CODE.ENUM;
        }
        else if (object instanceof Decimal) {
            return BinaryUtils.TYPE_CODE.DECIMAL;
        }
        else if (object instanceof Array) {
            if (object.length > 0 && object[0] !== null) {
                return BinaryUtils.getArrayType(BinaryUtils.calcObjectType(object[0]));
            }
        }
        else if (object instanceof Set) {
            return new CollectionObjectType(CollectionObjectType.COLLECTION_SUBTYPE.HASH_SET);
        }
        else if (object instanceof Map) {
            return new MapObjectType();
        }
        else if (object instanceof BinaryObject) {
            return BinaryUtils.TYPE_CODE.BINARY_OBJECT;
        }
        else if (objectType === 'object') {
            return new ComplexObjectType(object);
        }
        throw Errors.IgniteClientError.unsupportedTypeError(objectType);
    }

    static checkCompatibility(value, type) {
        if (!type) {
            return;
        }
        const typeCode = BinaryUtils.getTypeCode(type);
        if (value === null) {
            if (!BinaryUtils.isNullable(typeCode)) {
                throw Errors.IgniteClientError.typeCastError(BinaryUtils.TYPE_CODE.NULL, typeCode);
            }
            return;
        }
        else if (BinaryUtils.isStandardType(typeCode)) {
            BinaryUtils.checkStandardTypeCompatibility(value, typeCode, type);
            return;
        }
        const valueTypeCode = BinaryUtils.getTypeCode(BinaryUtils.calcObjectType(value));
        if (typeCode !== valueTypeCode) {
            throw Errors.IgniteClientError.typeCastError(valueTypeCode, typeCode);
        }
    }

    static isStandardType(typeCode) {
        return typeCode !== BinaryUtils.TYPE_CODE.BINARY_OBJECT &&
            typeCode !== BinaryUtils.TYPE_CODE.COMPLEX_OBJECT;
    }

    static checkStandardTypeCompatibility(value, typeCode, type = null) {
        const valueType = typeof value;
        switch (typeCode) {
            case BinaryUtils.TYPE_CODE.BYTE:
            case BinaryUtils.TYPE_CODE.SHORT:
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.LONG:
                if (!Number.isInteger(value)) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.FLOAT:
            case BinaryUtils.TYPE_CODE.DOUBLE:
                if (valueType !== 'number') {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.CHAR:
                if (valueType !== 'string' || value.length !== 1) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                if (valueType !== 'boolean') {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.STRING:
                if (valueType !== 'string') {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.UUID:
                if (!value instanceof Array ||
                    value.length !== BinaryUtils.getSize(BinaryUtils.TYPE_CODE.UUID)) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                value.forEach(element =>
                    BinaryUtils.checkStandardTypeCompatibility(element, BinaryUtils.TYPE_CODE.BYTE));
                return;
            case BinaryUtils.TYPE_CODE.DATE:
                if (!value instanceof Date) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.ENUM:
                if (!value instanceof EnumItem) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.DECIMAL:
                if (!value instanceof Decimal) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.TIMESTAMP:
                if (!value instanceof Timestamp) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.TIME:
                if (!value instanceof Date) {
                    throw Errors.IgniteClientError.valueCastError(value, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.BYTE_ARRAY:
            case BinaryUtils.TYPE_CODE.SHORT_ARRAY:
            case BinaryUtils.TYPE_CODE.INTEGER_ARRAY:
            case BinaryUtils.TYPE_CODE.LONG_ARRAY:
            case BinaryUtils.TYPE_CODE.FLOAT_ARRAY:
            case BinaryUtils.TYPE_CODE.DOUBLE_ARRAY:
            case BinaryUtils.TYPE_CODE.CHAR_ARRAY:
            case BinaryUtils.TYPE_CODE.BOOLEAN_ARRAY:
            case BinaryUtils.TYPE_CODE.STRING_ARRAY:
            case BinaryUtils.TYPE_CODE.UUID_ARRAY:
            case BinaryUtils.TYPE_CODE.DATE_ARRAY:
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
            case BinaryUtils.TYPE_CODE.ENUM_ARRAY:
            case BinaryUtils.TYPE_CODE.DECIMAL_ARRAY:
            case BinaryUtils.TYPE_CODE.TIMESTAMP_ARRAY:
            case BinaryUtils.TYPE_CODE.TIME_ARRAY:
                if (!value instanceof Array) {
                    throw Errors.IgniteClientError.typeCastError(valueType, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.MAP:
                if (!value instanceof Map) {
                    throw Errors.IgniteClientError.typeCastError(valueType, typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.COLLECTION:
                if (!(type && type._isSet() && value instanceof Set || value instanceof Array)) {
                    throw Errors.IgniteClientError.typeCastError(valueType, type && type._isSet() ? 'set' : typeCode);
                }
                return;
            case BinaryUtils.TYPE_CODE.NULL:
                if (value !== null) {
                    throw Errors.IgniteClientError.typeCastError('not null', typeCode);
                }
                return;
            default:
                const valueTypeCode = BinaryUtils.getTypeCode(BinaryUtils.calcObjectType(value));
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
            return;
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

    static getArrayElementType(arrayType) {
        if (arrayType instanceof ObjectArrayType) {
            return arrayType._elementType;
        }
        else if (arrayType === BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
            return null;
        }
        const elementTypeCode = TYPE_INFO[arrayType].ELEMENT_TYPE;
        if (!elementTypeCode) {
            throw Errors.IgniteClientError.internalError();
        }
        return elementTypeCode;
    }

    static getArrayType(elementType) {
        switch (BinaryUtils.getTypeCode(elementType)) {
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
            case BinaryUtils.TYPE_CODE.ENUM:
                return BinaryUtils.TYPE_CODE.ENUM_ARRAY;
            case BinaryUtils.TYPE_CODE.DECIMAL:
                return BinaryUtils.TYPE_CODE.DECIMAL_ARRAY;
            case BinaryUtils.TYPE_CODE.TIMESTAMP:
                return BinaryUtils.TYPE_CODE.TIMESTAMP_ARRAY;
            case BinaryUtils.TYPE_CODE.TIME:
                return BinaryUtils.TYPE_CODE.TIME_ARRAY;
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                return new ObjectArrayType();
            default:
                return new ObjectArrayType(elementType);
        }
    }

    static keepArrayElementType(arrayTypeCode) {
        return TYPE_INFO[arrayTypeCode].KEEP_ELEMENT_TYPE === true;
    }

    static getJsObjectFieldNames(jsObject) {
        var fields = new Array();
        for (let field in jsObject) {
            if (typeof jsObject[field] !== 'function') {
                fields.push(field);
            }
        }
        return fields;
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

    static hashCodeLowerCase(str) {
        return BinaryUtils.hashCode(str ? str.toLowerCase() : str);
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

module.exports = BinaryUtils;
