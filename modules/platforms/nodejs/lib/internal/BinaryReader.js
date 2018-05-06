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
const BinaryObject = require('../BinaryObject');
const CollectionObjectType = require('../ObjectType').CollectionObjectType;
const Errors = require('../Errors');
const Timestamp = require('../Timestamp');
const EnumItem = require('../EnumItem');
const BinaryUtils = require('./BinaryUtils');

class BinaryReader {

    static async readObject(buffer, expectedType = null) {
        const typeCode = buffer.readByte();
        BinaryUtils.checkTypesComatibility(expectedType, typeCode);
        return await BinaryReader._readTypedObject(buffer, typeCode, expectedType);
    }

    static async readStringArray(buffer) {
        return await BinaryReader._readTypedObject(buffer, BinaryUtils.TYPE_CODE.STRING_ARRAY);
    }

    static async _readTypedObject(buffer, objectTypeCode, expectedType = null) {
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
            case BinaryUtils.TYPE_CODE.UUID:
                return BinaryReader._readUUID(buffer);
            case BinaryUtils.TYPE_CODE.DATE:
                return buffer.readDate();
            case BinaryUtils.TYPE_CODE.ENUM:
            case BinaryUtils.TYPE_CODE.BINARY_ENUM:
                return await BinaryReader._readEnum(buffer);
            case BinaryUtils.TYPE_CODE.DECIMAL:
                return BinaryReader._readDecimal(buffer);
            case BinaryUtils.TYPE_CODE.TIMESTAMP:
                return BinaryReader._readTimestamp(buffer);
            case BinaryUtils.TYPE_CODE.TIME:
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
            case BinaryUtils.TYPE_CODE.UUID_ARRAY:
            case BinaryUtils.TYPE_CODE.DATE_ARRAY:
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
            case BinaryUtils.TYPE_CODE.ENUM_ARRAY:
            case BinaryUtils.TYPE_CODE.DECIMAL_ARRAY:
            case BinaryUtils.TYPE_CODE.TIMESTAMP_ARRAY:
            case BinaryUtils.TYPE_CODE.TIME_ARRAY:
                return await BinaryReader._readArray(buffer, objectTypeCode, expectedType);
            case BinaryUtils.TYPE_CODE.COLLECTION:
                return await BinaryReader._readCollection(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.MAP:
                return await BinaryReader._readMap(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                return await BinaryReader._readBinaryObject(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.NULL:
                return null;
            case BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                return await BinaryReader._readComplexObject(buffer, expectedType);
            default:
                throw Errors.IgniteClientError.unsupportedTypeError(objectTypeCode);
        }
    }

    static _readUUID(buffer) {
        return [...buffer.readBuffer(BinaryUtils.getSize(BinaryUtils.TYPE_CODE.UUID))];
    }

    static async _readEnum(buffer) {
        const enumItem = new EnumItem(0);
        await enumItem._read(buffer);
        return enumItem;
    }

    static _readDecimal(buffer) {
        const scale = buffer.readInteger();
        const dataLength = buffer.readInteger();
        const data = buffer.readBuffer(dataLength);
        const isNegative = (data[0] & 0x80) !== 0;
        if (isNegative) {
            data[0] &= 0x7F;
        }
        let result = new Decimal('0x' + data.toString('hex'));
        if (isNegative) {
            result = result.negated();
        }
        return result.mul(Decimal.pow(10, -scale));
    }

    static _readTimestamp(buffer) {
        return new Timestamp(buffer.readLong().toNumber(), buffer.readInteger());
    }

    static async _readArray(buffer, arrayTypeCode, arrayType) {
        if (arrayTypeCode === BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
            buffer.readInteger();
        }
        const length = buffer.readInteger();
        const elementType = BinaryUtils.getArrayElementType(arrayType ? arrayType : arrayTypeCode);
        const keepElementType = elementType === null ? true : BinaryUtils.keepArrayElementType(arrayTypeCode);
        const result = new Array(length);
        for (let i = 0; i < length; i++) {
            result[i] = keepElementType ?
                await BinaryReader.readObject(buffer, elementType) :
                await BinaryReader._readTypedObject(buffer, elementType);
        }
        return result;
    }

    static async _readMap(buffer, expectedMapType) {
        const result = new Map();
        const size = buffer.readInteger();
        const subType = buffer.readByte();
        let key, value;
        for (let i = 0; i < size; i++) {
            key = await BinaryReader.readObject(buffer, expectedMapType ? expectedMapType._keyType : null);
            value = await BinaryReader.readObject(buffer, expectedMapType ? expectedMapType._valueType : null);
            result.set(key, value);
        }
        return result;
    }

    static async _readCollection(buffer, expectedColType) {
        const size = buffer.readInteger();
        const subType = buffer.readByte();
        const isSet = CollectionObjectType._isSet(subType);
        const result = isSet ? new Set() : new Array(size);
        let element;
        for (let i = 0; i < size; i++) {
            element = await BinaryReader.readObject(buffer, expectedColType ? expectedColType._elementType : null);
            if (isSet) {
                result.add(element);
            }
            else {
                result[i] = element;
            }
        }
        return result;
    }

    static async _readBinaryObject(buffer, expectedType) {
        const size = buffer.readInteger();
        const startPos = buffer.position;
        buffer.position = startPos + size;
        const offset = buffer.readInteger();
        const endPos = buffer.position;
        buffer.position = startPos + offset;
        const result = await BinaryReader.readObject(buffer, expectedType);
        buffer.position = endPos;
        return result;
    }

    static async _readComplexObject(buffer, expectedType) {
        buffer.position = buffer.position - 1;
        const binaryObject = await BinaryObject._fromBuffer(buffer);
        return expectedType ?
            await binaryObject.toObject(expectedType) : binaryObject;
    }
}

module.exports = BinaryReader;
