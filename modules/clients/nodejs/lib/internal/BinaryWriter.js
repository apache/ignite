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

class BinaryWriter {

    static writeString(buffer, value) {
        BinaryWriter.writeObject(buffer, value, ObjectType.TYPE_CODE.STRING);
    }

    static writeObject(buffer, object, objectType = null, writeObjectType = true) {
        BinaryUtils.checkCompatibility(object, objectType);
        if (object === null) {
            buffer.writeByte(ObjectType.TYPE_CODE.NULL);
            return;
        }

        objectType = BinaryWriter._getObjectType(object, objectType);

        if (writeObjectType) {
            buffer.writeByte(objectType.typeCode);
        }
        switch (objectType.typeCode) {
            case ObjectType.TYPE_CODE.BYTE:
            case ObjectType.TYPE_CODE.SHORT:
            case ObjectType.TYPE_CODE.INTEGER:
            case ObjectType.TYPE_CODE.FLOAT:
            case ObjectType.TYPE_CODE.DOUBLE:
                buffer.writeNumber(object, objectType.typeCode);
                break;
            case ObjectType.TYPE_CODE.LONG:
                buffer.writeLong(object);
                break;
            case ObjectType.TYPE_CODE.CHAR:
                buffer.writeChar(object);
                break;
            case ObjectType.TYPE_CODE.BOOLEAN:
                buffer.writeBoolean(object);
                break;
            case ObjectType.TYPE_CODE.STRING:
                buffer.writeString(object);
                break;
            case ObjectType.TYPE_CODE.DATE:
                buffer.writeDate(object);
                break;
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
                BinaryWriter._writeArray(buffer, object, objectType);
                break;
            case ObjectType.TYPE_CODE.MAP:
                BinaryWriter._writeMap(buffer, object, objectType);
                break;
            default:
                throw new Errors.UnsupportedTypeError(objectType);
        }
    }

    static _getObjectType(object, objectType = null) {
        if (objectType === null) {
            objectType = BinaryWriter._getObjectTypeCode(object);
        }
        return BinaryUtils.getObjectType(objectType);
    }

    static _getObjectTypeCode(object) {
        const objectType = typeof object;
        if (objectType === 'number') {
            return ObjectType.TYPE_CODE.DOUBLE;
        }
        else if (objectType === 'string') {
            return ObjectType.TYPE_CODE.STRING;
        }
        else if (objectType === 'boolean') {
            return ObjectType.TYPE_CODE.BOOLEAN;
        }
        else if (object instanceof Date) {
            return ObjectType.TYPE_CODE.DATE;
        }
        else if (object instanceof Array) {
            if (object.length > 0) {
                return BinaryUtils.getArrayTypeCode(BinaryWriter._getObjectTypeCode(object[0]));
            }
        }
        else if (object instanceof Map) {
            return ObjectType.TYPE_CODE.MAP;
        }
        throw new Errors.UnsupportedTypeError(objectType);
    }

    static _writeArray(buffer, array, arrayType) {
        const elementType = BinaryUtils.getArrayElementType(arrayType.typeCode);
        const keepElementType = BinaryUtils.keepArrayElementType(arrayType.typeCode);
        buffer.writeInteger(array.length);
        for (let elem of array) {
            BinaryWriter.writeObject(buffer, elem, elementType, keepElementType);
        }
    }

    static _writeMap(buffer, map, mapType) {
        buffer.writeInteger(map.size);
        buffer.writeByte(mapType.mapType);
        map.forEach((value, key) => {
            BinaryWriter.writeObject(buffer, key, mapType.mapKeyType);
            BinaryWriter.writeObject(buffer, value, mapType.mapValueType);
        });
    }
}

module.exports = BinaryWriter;
