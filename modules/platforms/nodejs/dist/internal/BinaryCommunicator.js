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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const decimal_js_1 = require("decimal.js");
/*
import { CollectionObjectType, ComplexObjectType } from '../ObjectType';
import * as Errors from '../Errors';
import { Timestamp } from '../Timestamp';
import { EnumItem } from '../EnumItem';
import { BinaryUtils } from './BinaryUtils';
import { BinaryTypeStorage } from './BinaryTypeStorage';
import { BinaryObject } from '../BinaryObject';
import { BinaryType } from './BinaryType';
*/
const internal_1 = require("../internal");
class BinaryCommunicator {
    constructor(socket) {
        this._socket = socket;
        this._typeStorage = new internal_1.BinaryTypeStorage(this);
    }
    static readString(buffer) {
        const typeCode = buffer.readByte();
        internal_1.BinaryUtils.checkTypesComatibility(internal_1.BinaryUtils.TYPE_CODE.STRING, typeCode);
        if (typeCode === internal_1.BinaryUtils.TYPE_CODE.NULL) {
            return null;
        }
        return buffer.readString();
    }
    static writeString(buffer, value) {
        if (value === null) {
            buffer.writeByte(internal_1.BinaryUtils.TYPE_CODE.NULL);
        }
        else {
            buffer.writeByte(internal_1.BinaryUtils.TYPE_CODE.STRING);
            buffer.writeString(value);
        }
    }
    send(opCode, payloadWriter, payloadReader = null) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._socket.send(opCode, payloadWriter, payloadReader);
        });
    }
    get typeStorage() {
        return this._typeStorage;
    }
    readObject(buffer, expectedType = null) {
        return __awaiter(this, void 0, void 0, function* () {
            const typeCode = buffer.readByte();
            internal_1.BinaryUtils.checkTypesComatibility(expectedType, typeCode);
            return yield this._readTypedObject(buffer, typeCode, expectedType);
        });
    }
    readStringArray(buffer) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._readTypedObject(buffer, internal_1.BinaryUtils.TYPE_CODE.STRING_ARRAY);
        });
    }
    writeObject(buffer, object, objectType = null, writeObjectType = true) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.BinaryUtils.checkCompatibility(object, objectType);
            if (object === null) {
                buffer.writeByte(internal_1.BinaryUtils.TYPE_CODE.NULL);
                return;
            }
            objectType = objectType ? objectType : internal_1.BinaryUtils.calcObjectType(object);
            const objectTypeCode = internal_1.BinaryUtils.getTypeCode(objectType);
            if (writeObjectType) {
                buffer.writeByte(objectTypeCode);
            }
            switch (objectTypeCode) {
                case internal_1.BinaryUtils.TYPE_CODE.BYTE:
                case internal_1.BinaryUtils.TYPE_CODE.SHORT:
                case internal_1.BinaryUtils.TYPE_CODE.INTEGER:
                case internal_1.BinaryUtils.TYPE_CODE.FLOAT:
                case internal_1.BinaryUtils.TYPE_CODE.DOUBLE:
                    buffer.writeNumber(object, objectTypeCode);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.LONG:
                    buffer.writeLong(object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.CHAR:
                    buffer.writeChar(object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.BOOLEAN:
                    buffer.writeBoolean(object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.STRING:
                    buffer.writeString(object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.UUID:
                    this._writeUUID(buffer, object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.DATE:
                    buffer.writeDate(object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.ENUM:
                    yield this._writeEnum(buffer, object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.DECIMAL:
                    this._writeDecimal(buffer, object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.TIMESTAMP:
                    this._writeTimestamp(buffer, object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.TIME:
                    this._writeTime(buffer, object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.BYTE_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.SHORT_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.INTEGER_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.LONG_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.FLOAT_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.DOUBLE_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.CHAR_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.BOOLEAN_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.STRING_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.UUID_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.DATE_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.ENUM_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.DECIMAL_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.TIMESTAMP_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.TIME_ARRAY:
                    yield this._writeArray(buffer, object, objectType, objectTypeCode);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.COLLECTION:
                    yield this._writeCollection(buffer, object, objectType);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.MAP:
                    yield this._writeMap(buffer, object, objectType);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                    yield this._writeBinaryObject(buffer, object);
                    break;
                case internal_1.BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                    yield this._writeComplexObject(buffer, object, objectType);
                    break;
                default:
                    throw internal_1.Errors.IgniteClientError.unsupportedTypeError(objectType);
            }
        });
    }
    _readTypedObject(buffer, objectTypeCode, expectedType = null) {
        return __awaiter(this, void 0, void 0, function* () {
            switch (objectTypeCode) {
                case internal_1.BinaryUtils.TYPE_CODE.BYTE:
                case internal_1.BinaryUtils.TYPE_CODE.SHORT:
                case internal_1.BinaryUtils.TYPE_CODE.INTEGER:
                case internal_1.BinaryUtils.TYPE_CODE.FLOAT:
                case internal_1.BinaryUtils.TYPE_CODE.DOUBLE:
                    return buffer.readNumber(objectTypeCode);
                case internal_1.BinaryUtils.TYPE_CODE.LONG:
                    return buffer.readLong().toNumber();
                case internal_1.BinaryUtils.TYPE_CODE.CHAR:
                    return buffer.readChar();
                case internal_1.BinaryUtils.TYPE_CODE.BOOLEAN:
                    return buffer.readBoolean();
                case internal_1.BinaryUtils.TYPE_CODE.STRING:
                    return buffer.readString();
                case internal_1.BinaryUtils.TYPE_CODE.UUID:
                    return this._readUUID(buffer);
                case internal_1.BinaryUtils.TYPE_CODE.DATE:
                    return buffer.readDate();
                case internal_1.BinaryUtils.TYPE_CODE.ENUM:
                case internal_1.BinaryUtils.TYPE_CODE.BINARY_ENUM:
                    return yield this._readEnum(buffer);
                case internal_1.BinaryUtils.TYPE_CODE.DECIMAL:
                    return this._readDecimal(buffer);
                case internal_1.BinaryUtils.TYPE_CODE.TIMESTAMP:
                    return this._readTimestamp(buffer);
                case internal_1.BinaryUtils.TYPE_CODE.TIME:
                    return buffer.readDate();
                case internal_1.BinaryUtils.TYPE_CODE.BYTE_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.SHORT_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.INTEGER_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.LONG_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.FLOAT_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.DOUBLE_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.CHAR_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.BOOLEAN_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.STRING_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.UUID_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.DATE_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.ENUM_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.DECIMAL_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.TIMESTAMP_ARRAY:
                case internal_1.BinaryUtils.TYPE_CODE.TIME_ARRAY:
                    return yield this._readArray(buffer, objectTypeCode, expectedType);
                case internal_1.BinaryUtils.TYPE_CODE.COLLECTION:
                    return yield this._readCollection(buffer, expectedType);
                case internal_1.BinaryUtils.TYPE_CODE.MAP:
                    return yield this._readMap(buffer, expectedType);
                case internal_1.BinaryUtils.TYPE_CODE.BINARY_OBJECT:
                    return yield this._readBinaryObject(buffer, expectedType);
                case internal_1.BinaryUtils.TYPE_CODE.NULL:
                    return null;
                case internal_1.BinaryUtils.TYPE_CODE.COMPLEX_OBJECT:
                    return yield this._readComplexObject(buffer, expectedType);
                default:
                    throw internal_1.Errors.IgniteClientError.unsupportedTypeError(objectTypeCode);
            }
        });
    }
    _readUUID(buffer) {
        return [...buffer.readBuffer(internal_1.BinaryUtils.getSize(internal_1.BinaryUtils.TYPE_CODE.UUID))];
    }
    _readEnum(buffer) {
        return __awaiter(this, void 0, void 0, function* () {
            const enumItem = new internal_1.EnumItem(0);
            yield enumItem._read(this, buffer);
            return enumItem;
        });
    }
    _readDecimal(buffer) {
        const scale = buffer.readInteger();
        const dataLength = buffer.readInteger();
        const data = buffer.readBuffer(dataLength);
        const isNegative = (data[0] & 0x80) !== 0;
        if (isNegative) {
            data[0] &= 0x7F;
        }
        let result = new decimal_js_1.Decimal('0x' + data.toString('hex'));
        if (isNegative) {
            result = result.negated();
        }
        return result.mul(decimal_js_1.Decimal.pow(10, -scale));
    }
    _readTimestamp(buffer) {
        return new internal_1.Timestamp(buffer.readLong().toNumber(), buffer.readInteger());
    }
    _readArray(buffer, arrayTypeCode, arrayType) {
        return __awaiter(this, void 0, void 0, function* () {
            if (arrayTypeCode === internal_1.BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
                buffer.readInteger();
            }
            const length = buffer.readInteger();
            const elementType = internal_1.BinaryUtils.getArrayElementType(arrayType ? arrayType : arrayTypeCode);
            const keepElementType = elementType === null ? true : internal_1.BinaryUtils.keepArrayElementType(arrayTypeCode);
            const result = new Array(length);
            for (let i = 0; i < length; i++) {
                result[i] = keepElementType ?
                    yield this.readObject(buffer, elementType) :
                    yield this._readTypedObject(buffer, elementType);
            }
            return result;
        });
    }
    _readMap(buffer, expectedMapType) {
        return __awaiter(this, void 0, void 0, function* () {
            const result = new Map();
            const size = buffer.readInteger();
            const subType = buffer.readByte();
            let key, value;
            for (let i = 0; i < size; i++) {
                key = yield this.readObject(buffer, expectedMapType ? expectedMapType._keyType : null);
                value = yield this.readObject(buffer, expectedMapType ? expectedMapType._valueType : null);
                result.set(key, value);
            }
            return result;
        });
    }
    _readCollection(buffer, expectedColType) {
        return __awaiter(this, void 0, void 0, function* () {
            const size = buffer.readInteger();
            const subType = buffer.readByte();
            const isSet = internal_1.CollectionObjectType._isSet(subType);
            const result = isSet ? new Set() : new Array(size);
            let element;
            for (let i = 0; i < size; i++) {
                element = yield this.readObject(buffer, expectedColType ? expectedColType._elementType : null);
                if (isSet) {
                    result.add(element);
                }
                else {
                    result[i] = element;
                }
            }
            return result;
        });
    }
    _readBinaryObject(buffer, expectedType) {
        return __awaiter(this, void 0, void 0, function* () {
            const size = buffer.readInteger();
            const startPos = buffer.position;
            buffer.position = startPos + size;
            const offset = buffer.readInteger();
            const endPos = buffer.position;
            buffer.position = startPos + offset;
            const result = yield this.readObject(buffer, expectedType);
            buffer.position = endPos;
            return result;
        });
    }
    _readComplexObject(buffer, expectedType) {
        return __awaiter(this, void 0, void 0, function* () {
            buffer.position = buffer.position - 1;
            const binaryObject = yield internal_1.BinaryObject._fromBuffer(this, buffer);
            return expectedType ?
                yield binaryObject.toObject(expectedType) : binaryObject;
        });
    }
    _writeUUID(buffer, value) {
        buffer.writeBuffer(Buffer.from(value));
    }
    _writeEnum(buffer, enumValue) {
        return __awaiter(this, void 0, void 0, function* () {
            yield enumValue._write(this, buffer);
        });
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
        let hexValue = new decimal_js_1.Decimal(strValue).toHexadecimal().substring(2);
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
    _writeArray(buffer, array, arrayType, arrayTypeCode) {
        return __awaiter(this, void 0, void 0, function* () {
            const elementType = internal_1.BinaryUtils.getArrayElementType(arrayType);
            const keepElementType = internal_1.BinaryUtils.keepArrayElementType(arrayTypeCode);
            if (arrayTypeCode === internal_1.BinaryUtils.TYPE_CODE.OBJECT_ARRAY) {
                buffer.writeInteger(elementType instanceof internal_1.ComplexObjectType ?
                    internal_1.BinaryType._calculateId(elementType.typeName) : -1);
            }
            buffer.writeInteger(array.length);
            for (let elem of array) {
                yield this.writeObject(buffer, elem, elementType, keepElementType);
            }
        });
    }
    _writeCollection(buffer, collection, collectionType) {
        return __awaiter(this, void 0, void 0, function* () {
            buffer.writeInteger(collection instanceof Set ? collection.size : collection.length);
            buffer.writeByte(collectionType._subType);
            for (let element of collection) {
                yield this.writeObject(buffer, element, collectionType._elementType);
            }
        });
    }
    _writeMap(buffer, map, mapType) {
        return __awaiter(this, void 0, void 0, function* () {
            buffer.writeInteger(map.size);
            buffer.writeByte(mapType._subType);
            for (let [key, value] of map.entries()) {
                yield this.writeObject(buffer, key, mapType._keyType);
                yield this.writeObject(buffer, value, mapType._valueType);
            }
        });
    }
    _writeBinaryObject(buffer, binaryObject) {
        return __awaiter(this, void 0, void 0, function* () {
            buffer.position = buffer.position - 1;
            yield binaryObject._write(this, buffer);
        });
    }
    _writeComplexObject(buffer, object, objectType) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._writeBinaryObject(buffer, yield internal_1.BinaryObject.fromObject(object, objectType));
        });
    }
}
exports.BinaryCommunicator = BinaryCommunicator;
//# sourceMappingURL=BinaryCommunicator.js.map