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

const BinaryObject = require('./BinaryObject');
const Errors = require('../Errors');
const BinaryUtils = require('./BinaryUtils');

class BinaryReader {

    static readObject(buffer, expectedType = null) {
        const typeCode = buffer.readByte();
        BinaryUtils.checkTypesComatibility(expectedType, typeCode);
        return BinaryReader._readTypedObject(buffer, typeCode, expectedType);
    }

    static readString(buffer) {
        return BinaryReader._readTypedObject(buffer, BinaryUtils.TYPE_CODE.STRING);
    }

    static readStringArray(buffer) {
        return BinaryReader._readTypedObject(buffer, BinaryUtils.TYPE_CODE.STRING_ARRAY);
    }

    static _readTypedObject(buffer, objectTypeCode, expectedType = null) {
        switch (objectTypeCode) {
            case BinaryUtils.TYPE_CODE.BYTE:
            case BinaryUtils.TYPE_CODE.SHORT:
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.FLOAT:
            case BinaryUtils.TYPE_CODE.DOUBLE:
                return buffer.readNumber(objectTypeCode);
            case BinaryUtils.TYPE_CODE.LONG:
                return buffer.readLong().toNumber();
            case BinaryUtils.TYPE_CODE.CHAR:
                return buffer.readChar();
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                return buffer.readBoolean();
            case BinaryUtils.TYPE_CODE.STRING:
                return buffer.readString();
            case BinaryUtils.TYPE_CODE.DATE:
                return buffer.readDate();
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
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                return BinaryReader._readArray(buffer, objectTypeCode, expectedType);
            case BinaryUtils.TYPE_CODE.MAP:
                return BinaryReader._readMap(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                return BinaryReader._readBinaryObject(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.NULL:
                return null;
            case BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                return BinaryReader._readComplexObject(buffer, expectedType);
            default:
                throw Errors.IgniteClientError.unsupportedTypeError(objectTypeCode);
        }
    }

    static _readArray(buffer, arrayTypeCode, arrayType) {
        if (arrayTypeCode === BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
            buffer.readInteger();
        }
        const length = buffer.readInteger();
        const elementType = BinaryUtils.getArrayElementType(arrayType ? arrayType : arrayTypeCode);
        const keepElementType = BinaryUtils.keepArrayElementType(arrayTypeCode);
        const result = new Array(length);
        for (let i = 0; i < length; i++) {
            result[i] = keepElementType ?
                BinaryReader.readObject(buffer, elementType) :
                BinaryReader._readTypedObject(buffer, elementType);
        }
        return result;
    }

    static _readMap(buffer, expectedMapType) {
        const result = new Map();
        const size = buffer.readInteger();
        const mapType = buffer.readByte();
        let key, value;
        for (let i = 0; i < size; i++) {
            key = BinaryReader.readObject(buffer, expectedMapType ? expectedMapType._keyType : null);
            value = BinaryReader.readObject(buffer, expectedMapType ? expectedMapType._valueType : null);
            result.set(key, value);
        }
        return result;
    }

    static _readBinaryObject(buffer, expectedType) {
        const size = buffer.readInteger();
        const startPos = buffer.position;
        buffer.position = startPos + size;
        const offset = buffer.readInteger();
        const endPos = buffer.position;
        buffer.position = startPos + offset;
        const result = BinaryReader.readObject(buffer, expectedType);
        buffer.position = endPos;
        return result;
    }

    static _readComplexObject(buffer, expectedType) {
        buffer.position = buffer.position - 1;
        const isBinaryMode = expectedType && expectedType instanceof BinaryUtils.BinaryObjectType;
        const binaryObject = BinaryObject._fromBuffer(buffer, isBinaryMode ? expectedType.innerType : expectedType);
        if (isBinaryMode) {
            return binaryObject;
        }
        return binaryObject._toObject();
    }
}

module.exports = BinaryReader;
