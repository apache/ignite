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
const CollectionObjectType = require('../ObjectType').CollectionObjectType;
const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const Errors = require('../Errors');
const Timestamp = require('../Timestamp');
const EnumItem = require('../EnumItem');
const BinaryUtils = require('./BinaryUtils');
const BinaryTypeStorage = require('./BinaryTypeStorage');

class BinaryCommunicator {

    constructor(socket) {
        this._socket = socket;
        this._typeStorage = new BinaryTypeStorage(this);
    }

    static readString(buffer) {
        const typeCode = buffer.readByte();
        BinaryUtils.checkTypesComatibility(BinaryUtils.TYPE_CODE.STRING, typeCode);
        if (typeCode === BinaryUtils.TYPE_CODE.NULL) {
            return null;
        }
        return buffer.readString();
    }

    static writeString(buffer, value) {
        if (value === null) {
            buffer.writeByte(BinaryUtils.TYPE_CODE.NULL);
        }
        else {
            buffer.writeByte(BinaryUtils.TYPE_CODE.STRING);
            buffer.writeString(value);
        }
    }
    
    async send(opCode, payloadWriter, payloadReader = null) {
        await this._socket.send(opCode, payloadWriter, payloadReader);
    }

    get typeStorage() {
        return this._typeStorage;
    }

    async readObject(buffer, expectedType = null) {
        const typeCode = buffer.readByte();
        BinaryUtils.checkTypesComatibility(expectedType, typeCode);
        return await this._readTypedObject(buffer, typeCode, expectedType);
    }

    async readStringArray(buffer) {
        return await this._readTypedObject(buffer, BinaryUtils.TYPE_CODE.STRING_ARRAY);
    }

    async writeObject(buffer, object, objectType = null, writeObjectType = true) {
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
                this._writeUUID(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.DATE:
                buffer.writeDate(object);
                break;
            case BinaryUtils.TYPE_CODE.ENUM:
                await this._writeEnum(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.DECIMAL:
                this._writeDecimal(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.TIMESTAMP:
                this._writeTimestamp(buffer, object);
                break;
            case BinaryUtils.TYPE_CODE.TIME:
                this._writeTime(buffer, object);
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
                await this._writeArray(buffer, object, objectType, objectTypeCode);
                break;
            case BinaryUtils.TYPE_CODE.COLLECTION:
                await this._writeCollection(buffer, object, objectType);
                break;
            case BinaryUtils.TYPE_CODE.MAP:
                await this._writeMap(buffer, object, objectType);
                break;
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                await this._writeBinaryObject(buffer, object, objectType);
                break;
            case BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                await this._writeComplexObject(buffer, object, objectType);
                break;
            default:
                throw Errors.IgniteClientError.unsupportedTypeError(objectType);
        }
    }

    async _readTypedObject(buffer, objectTypeCode, expectedType = null) {
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
                return this._readUUID(buffer);
            case BinaryUtils.TYPE_CODE.DATE:
                return buffer.readDate();
            case BinaryUtils.TYPE_CODE.ENUM:
            case BinaryUtils.TYPE_CODE.BINARY_ENUM:
                return await this._readEnum(buffer);
            case BinaryUtils.TYPE_CODE.DECIMAL:
                return this._readDecimal(buffer);
            case BinaryUtils.TYPE_CODE.TIMESTAMP:
                return this._readTimestamp(buffer);
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
                return await this._readArray(buffer, objectTypeCode, expectedType);
            case BinaryUtils.TYPE_CODE.COLLECTION:
                return await this._readCollection(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.MAP:
                return await this._readMap(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                return await this._readBinaryObject(buffer, expectedType);
            case BinaryUtils.TYPE_CODE.NULL:
                return null;
            case BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                return await this._readComplexObject(buffer, expectedType);
            default:
                throw Errors.IgniteClientError.unsupportedTypeError(objectTypeCode);
        }
    }

    _readUUID(buffer) {
        return [...buffer.readBuffer(BinaryUtils.getSize(BinaryUtils.TYPE_CODE.UUID))];
    }

    async _readEnum(buffer) {
        const enumItem = new EnumItem(0);
        await enumItem._read(this, buffer);
        return enumItem;
    }

    _readDecimal(buffer) {
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

    _readTimestamp(buffer) {
        return new Timestamp(buffer.readLong().toNumber(), buffer.readInteger());
    }

    async _readArray(buffer, arrayTypeCode, arrayType) {
        if (arrayTypeCode === BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
            buffer.readInteger();
        }
        const length = buffer.readInteger();
        const elementType = BinaryUtils.getArrayElementType(arrayType ? arrayType : arrayTypeCode);
        const keepElementType = elementType === null ? true : BinaryUtils.keepArrayElementType(arrayTypeCode);
        const result = new Array(length);
        for (let i = 0; i < length; i++) {
            result[i] = keepElementType ?
                await this.readObject(buffer, elementType) :
                await this._readTypedObject(buffer, elementType);
        }
        return result;
    }

    async _readMap(buffer, expectedMapType) {
        const result = new Map();
        const size = buffer.readInteger();
        const subType = buffer.readByte();
        let key, value;
        for (let i = 0; i < size; i++) {
            key = await this.readObject(buffer, expectedMapType ? expectedMapType._keyType : null);
            value = await this.readObject(buffer, expectedMapType ? expectedMapType._valueType : null);
            result.set(key, value);
        }
        return result;
    }

    async _readCollection(buffer, expectedColType) {
        const size = buffer.readInteger();
        const subType = buffer.readByte();
        const isSet = CollectionObjectType._isSet(subType);
        const result = isSet ? new Set() : new Array(size);
        let element;
        for (let i = 0; i < size; i++) {
            element = await this.readObject(buffer, expectedColType ? expectedColType._elementType : null);
            if (isSet) {
                result.add(element);
            }
            else {
                result[i] = element;
            }
        }
        return result;
    }

    async _readBinaryObject(buffer, expectedType) {
        const size = buffer.readInteger();
        const startPos = buffer.position;
        buffer.position = startPos + size;
        const offset = buffer.readInteger();
        const endPos = buffer.position;
        buffer.position = startPos + offset;
        const result = await this.readObject(buffer, expectedType);
        buffer.position = endPos;
        return result;
    }

    async _readComplexObject(buffer, expectedType) {
        buffer.position = buffer.position - 1;
        const BinaryObject = require('../BinaryObject');
        const binaryObject = await BinaryObject._fromBuffer(this, buffer);
        return expectedType ?
            await binaryObject.toObject(expectedType) : binaryObject;
    }

    _writeUUID(buffer, value) {
        buffer.writeBuffer(Buffer.from(value));
    }

    async _writeEnum(buffer, enumValue) {
        await enumValue._write(this, buffer);
    }

    _writeDecimal(buffer, decimal) {
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

    _writeTimestamp(buffer, timestamp) {
        buffer.writeDate(timestamp);
        buffer.writeInteger(timestamp.getNanos());
    }

    _writeTime(buffer, time) {
        const midnight = new Date(time);
        midnight.setHours(0, 0, 0, 0);
        buffer.writeLong(time.getTime() - midnight.getTime());
    }

    async _writeArray(buffer, array, arrayType, arrayTypeCode) {
        const BinaryType = require('./BinaryType');
        const elementType = BinaryUtils.getArrayElementType(arrayType);
        const keepElementType = BinaryUtils.keepArrayElementType(arrayTypeCode);
        if (arrayTypeCode === BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
            buffer.writeInteger(elementType instanceof ComplexObjectType ?
                BinaryType._calculateId(elementType._typeName) : -1);
        }
        buffer.writeInteger(array.length);
        for (let elem of array) {
            await this.writeObject(buffer, elem, elementType, keepElementType);
        }
    }

    async _writeCollection(buffer, collection, collectionType) {
        buffer.writeInteger(collection instanceof Set ? collection.size : collection.length);
        buffer.writeByte(collectionType._subType);
        for (let element of collection) {
            await this.writeObject(buffer, element, collectionType._elementType);
        }
    }

    async _writeMap(buffer, map, mapType) {
        buffer.writeInteger(map.size);
        buffer.writeByte(mapType._subType);
        for (let [key, value] of map.entries()) {
            await this.writeObject(buffer, key, mapType._keyType);
            await this.writeObject(buffer, value, mapType._valueType);
        }
    }

    async _writeBinaryObject(buffer, binaryObject) {
        buffer.position = buffer.position - 1;
        await binaryObject._write(this, buffer);
    }

    async _writeComplexObject(buffer, object, objectType) {
        const BinaryObject = require('../BinaryObject');
        await this._writeBinaryObject(buffer, await BinaryObject.fromObject(object, objectType));
    }
}

module.exports = BinaryCommunicator;
