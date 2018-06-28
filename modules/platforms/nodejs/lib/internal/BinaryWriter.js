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
const Errors = require('../Errors');
const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const BinaryUtils = require('./BinaryUtils');

class BinaryWriter {

    static async writeString(buffer, value) {
        await BinaryWriter.writeObject(buffer, value, BinaryUtils.TYPE_CODE.STRING);
    }

    static async writeObject(buffer, object, objectType = null, writeObjectType = true) {
        BinaryUtils.checkCompatibility(object, objectType);
        if (object === null) {
            buffer.writeByte(BinaryUtils.TYPE_CODE.NULL);
            return;
        }

        objectType =  objectType ? objectType : BinaryUtils.calcObjectType(object);
        const objectTypeCode = BinaryUtils.getTypeCode(objectType);

        if (writeObjectType) {
            buffer.writeByte(objectTypeCode);
        }
        switch (objectTypeCode) {
            case BinaryUtils.TYPE_CODE.BYTE:
            case BinaryUtils.TYPE_CODE.SHORT:
            case BinaryUtils.TYPE_CODE.INTEGER:
            case BinaryUtils.TYPE_CODE.FLOAT:
            case BinaryUtils.TYPE_CODE.DOUBLE:
                buffer.writeNumber(object, objectTypeCode);
                break;
            case BinaryUtils.TYPE_CODE.LONG:
                buffer.writeLong(object);
                break;
            case BinaryUtils.TYPE_CODE.CHAR:
                buffer.writeChar(object);
                break;
            case BinaryUtils.TYPE_CODE.BOOLEAN:
                buffer.writeBoolean(object);
                break;
            case BinaryUtils.TYPE_CODE.STRING:
                buffer.writeString(object);
                break;
            case BinaryUtils.TYPE_CODE.UUID:
                BinaryWriter._writeUUID(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.DATE:
                buffer.writeDate(object);
                break;
            case BinaryUtils.TYPE_CODE.ENUM:
                await BinaryWriter._writeEnum(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.DECIMAL:
                BinaryWriter._writeDecimal(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.TIMESTAMP:
                BinaryWriter._writeTimestamp(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.TIME:
                BinaryWriter._writeTime(buffer, object);
                break;
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
                await BinaryWriter._writeArray(buffer, object, objectType, objectTypeCode);
                break;
            case BinaryUtils.TYPE_CODE.COLLECTION:
                await BinaryWriter._writeCollection(buffer, object, objectType);
                break;
            case BinaryUtils.TYPE_CODE.MAP:
                await BinaryWriter._writeMap(buffer, object, objectType);
                break;
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                await BinaryWriter._writeBinaryObject(buffer, object, objectType);
                break;
            case BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                await BinaryWriter._writeComplexObject(buffer, object, objectType);
                break;
            default:
                throw Errors.IgniteClientError.unsupportedTypeError(objectType);
        }
    }

    static _writeUUID(buffer, value) {
        buffer.writeBuffer(Buffer.from(value));
    }

    static async _writeEnum(buffer, enumValue) {
        await enumValue._write(buffer);
    }

    static _writeDecimal(buffer, decimal) {
        let strValue = decimal.toExponential();
        let expIndex = strValue.indexOf('e');
        if (expIndex < 0) {
            expIndex = strValue.indexOf('E');
        }
        let scale = 0;
        if (expIndex >= 0) {
            scale = parseInt(strValue.substring(expIndex + 1));
            strValue = strValue.substring(0, expIndex);
        }
        const isNegative = strValue.startsWith('-');
        if (isNegative) {
            strValue = strValue.substring(1);
        }
        const dotIndex = strValue.indexOf('.');
        if (dotIndex >= 0) {
            scale -= strValue.length - dotIndex - 1;
            strValue = strValue.substring(0, dotIndex) + strValue.substring(dotIndex + 1);
        }
        scale = -scale;
        let hexValue = new Decimal(strValue).toHexadecimal().substring(2);
        hexValue = ((hexValue.length % 2 !== 0) ? '000' : '00') + hexValue;
        const valueBuffer = Buffer.from(hexValue, 'hex');
        if (isNegative) {
            valueBuffer[0] |= 0x80;
        }
        buffer.writeInteger(scale);
        buffer.writeInteger(valueBuffer.length);
        buffer.writeBuffer(valueBuffer);
    }

    static _writeTimestamp(buffer, timestamp) {
        buffer.writeDate(timestamp);
        buffer.writeInteger(timestamp.getNanos());
    }

    static _writeTime(buffer, time) {
        const midnight = new Date(time);
        midnight.setHours(0, 0, 0, 0);
        buffer.writeLong(time.getTime() - midnight.getTime());
    }

    static async _writeArray(buffer, array, arrayType, arrayTypeCode) {
        const BinaryType = require('./BinaryType');
        const elementType = BinaryUtils.getArrayElementType(arrayType);
        const keepElementType = BinaryUtils.keepArrayElementType(arrayTypeCode);
        if (arrayTypeCode === BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
            buffer.writeInteger(elementType instanceof ComplexObjectType ?
                BinaryType._calculateId(elementType._typeName) : -1);
        }
        buffer.writeInteger(array.length);
        for (let elem of array) {
            await BinaryWriter.writeObject(buffer, elem, elementType, keepElementType);
        }
    }

    static async _writeCollection(buffer, collection, collectionType) {
        buffer.writeInteger(collection instanceof Set ? collection.size : collection.length);
        buffer.writeByte(collectionType._subType);
        for (let element of collection) {
            await BinaryWriter.writeObject(buffer, element, collectionType._elementType);
        }
    }

    static async _writeMap(buffer, map, mapType) {
        buffer.writeInteger(map.size);
        buffer.writeByte(mapType._subType);
        for (let [key, value] of map.entries()) {
            await BinaryWriter.writeObject(buffer, key, mapType._keyType);
            await BinaryWriter.writeObject(buffer, value, mapType._valueType);
        }
    }

    static async _writeBinaryObject(buffer, binaryObject) {
        buffer.position = buffer.position - 1;
        await binaryObject._write(buffer);
    }

    static async _writeComplexObject(buffer, object, objectType) {
        const BinaryObject = require('../BinaryObject');
        await BinaryWriter._writeBinaryObject(buffer, await BinaryObject.fromObject(object, objectType));
    }
}

module.exports = BinaryWriter;
