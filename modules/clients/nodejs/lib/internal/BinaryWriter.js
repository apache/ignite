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

const Errors = require('../Errors');
const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const BinaryUtils = require('./BinaryUtils');
const BinaryType = require('./BinaryType');

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
            case BinaryUtils.TYPE_CODE.DATE:
                buffer.writeDate(object);
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
            case BinaryUtils.TYPE_CODE.DATE_ARRAY:
            case BinaryUtils.TYPE_CODE.OBJECT_ARRAY:
                await BinaryWriter._writeArray(buffer, object, objectType, objectTypeCode);
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

    static async _writeArray(buffer, array, arrayType, arrayTypeCode) {
        const elementType = BinaryUtils.getArrayElementType(arrayType);
        const keepElementType = BinaryUtils.keepArrayElementType(arrayTypeCode);
        if (arrayTypeCode === BinaryUtils.TYPE_CODE.OBJECT_ARRAY && 
            elementType instanceof ComplexObjectType) {
            buffer.writeInteger(BinaryType._calculateId(elementType._typeName));
        }
        buffer.writeInteger(array.length);
        for (let elem of array) {
            await BinaryWriter.writeObject(buffer, elem, elementType, keepElementType);
        }
    }

    static async _writeMap(buffer, map, mapType) {
        buffer.writeInteger(map.size);
        buffer.writeByte(mapType.mapType);
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
