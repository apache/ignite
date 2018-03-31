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

const ObjectType = require('../ObjectType');
const Errors = require('../Errors');
const BinaryUtils = require('./BinaryUtils');

class BinaryReader {

    static readObject(buffer, expectedType = null) {
        const type = buffer.readByte();
        BinaryUtils.checkTypesComatibility(expectedType, type);
        return BinaryReader._readTypedObject(buffer, type, expectedType);
    }

    static readStringArray(buffer) {
        return BinaryReader._readTypedObject(buffer, ObjectType.TYPE_CODE.STRING_ARRAY);
    }

    static _readTypedObject(buffer, objectTypeCode, expectedType = null) {
        switch (objectTypeCode) {
            case ObjectType.TYPE_CODE.BYTE:
            case ObjectType.TYPE_CODE.SHORT:
            case ObjectType.TYPE_CODE.INTEGER:
            case ObjectType.TYPE_CODE.FLOAT:
            case ObjectType.TYPE_CODE.DOUBLE:
                return buffer.readNumber(objectTypeCode);
            case ObjectType.TYPE_CODE.LONG:
                return buffer.readLong().toNumber();
            case ObjectType.TYPE_CODE.CHAR:
                return buffer.readChar();
            case ObjectType.TYPE_CODE.BOOLEAN:
                return buffer.readBoolean();
            case ObjectType.TYPE_CODE.STRING:
                return buffer.readString();
            case ObjectType.TYPE_CODE.DATE:
                return buffer.readDate();
            case ObjectType.TYPE_CODE.BYTE_ARRAY:
            case ObjectType.TYPE_CODE.SHORT_ARRAY:
            case ObjectType.TYPE_CODE.INTEGER_ARRAY:
            case ObjectType.TYPE_CODE.LONG_ARRAY:
            case ObjectType.TYPE_CODE.FLOAT_ARRAY:
            case ObjectType.TYPE_CODE.DOUBLE_ARRAY:
            case ObjectType.TYPE_CODE.CHAR_ARRAY:
            case ObjectType.TYPE_CODE.BOOLEAN_ARRAY:
            case ObjectType.TYPE_CODE.STRING_ARRAY:
            case ObjectType.TYPE_CODE.UUID_ARRAY:
            case ObjectType.TYPE_CODE.DATE_ARRAY:
            case ObjectType.TYPE_CODE.BINARY_OBJECT_ARRAY:
                return BinaryReader._readArray(buffer, objectTypeCode);
            case ObjectType.TYPE_CODE.MAP:
                return BinaryReader._readMap(buffer, expectedType);
            case ObjectType.TYPE_CODE.NULL:
                return null;
            default:
                throw Errors.IgniteClientError.unsupportedTypeError(objectTypeCode);
        }
    }

    static _readArray(buffer, arrayTypeCode) {
        const length = buffer.readInteger();
        const elementType = BinaryUtils.getArrayElementType(arrayTypeCode);
        const keepElementType = BinaryUtils.keepArrayElementType(arrayTypeCode);
        const result = new Array(length);
        for (let i = 0; i < length; i++) {
            result[i] = keepElementType ?
                BinaryReader.readObject(buffer, elementType) :
                BinaryReader._readTypedObject(buffer, elementType.typeCode);
        }
        return result;
    }

    static _readMap(buffer, expectedMapType) {
        const result = new Map();
        const size = buffer.readInteger();
        const mapType = buffer.readByte();
        let key, value;
        for (let i = 0; i < size; i++) {
            key = BinaryReader.readObject(buffer, expectedMapType ? expectedMapType.mapKeyType : null);
            value = BinaryReader.readObject(buffer, expectedMapType ? expectedMapType.mapValueType : null);
            result.set(key, value);
        }
        return result;
    }
}

module.exports = BinaryReader;
