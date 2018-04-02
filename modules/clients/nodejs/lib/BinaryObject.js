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

const Util = require('util');
const ObjectType = require('./ObjectType').ObjectType;
const ComplexObjectType = require('./ObjectType').ComplexObjectType;
const Errors = require('./Errors');
const BinaryUtils = require('./internal/BinaryUtils');
const BinaryType = require('./internal/BinaryType');
const BinaryField = require('./internal/BinaryField');
const BinarySchema = require('./internal/BinarySchema');
const ArgumentChecker = require('./internal/ArgumentChecker');
const Logger = require('./internal/Logger');

const HEADER_LENGTH = 24;
const VERSION = 1;
const FLAG_USER_TYPE = 1;
const FLAG_HAS_SCHEMA = 2;

/**
 * ???
 */
class BinaryObject {

    /**
     * ???
     *
     * @param {ComplexObjectType} complexObjectType - 
     *
     * @return {BinaryObject} - 
     */
    constructor(complexObjectType) {
        ArgumentChecker.hasType(complexObjectType, 'complexObjectType', ComplexObjectType);
        this._buffer = null;
        this._fields = new Map();
        this._setType(BinaryType._fromObjectType(complexObjectType));
        this._modified = false;
        this._schemaOffset = null;
    }

    /**
     * ???
     *
     * @param {string} fieldName - 
     *
     * @return {boolean} - 
     */
    hasField(fieldName) {
        return this._fields.has(BinaryField._calculateId(fieldName));
    }

    /**
     * ???
     *
     * @param {string} fieldName - 
     *
     * @return {*} - ??? or undefined if field does not exist
     */
    getField(fieldName) {
        const field = this._fields.get(BinaryField._calculateId(fieldName));
        return field ? field.value : field;
    }

    /**
     * ???
     *
     * @param {string} fieldName - 
     */
    removeField(fieldName) {
        this._modified = true;
        this._fields.delete(BinaryField._calculateId(fieldName));
    }

    /**
     * ???
     *
     * @param {string} fieldName - 
     * @param {*} fieldValue - 
     * @param {ObjectType} [fieldType] - 
     *
     * @return {BinaryObject} - the same instance of BinaryObject
     */
    setField(fieldName, fieldValue, fieldType = null) {
        this._modified = true;
        const field = new BinaryObjectField(fieldName, fieldType, fieldValue);
        this._fields.set(field.id, field);
        this._type._addField(this._schema, fieldName, fieldType);
        return this;
    }

    /**
     * ???
     *
     * @param {*} object - 
     * @param {ComplexObjectType} [complexObjectType] - 
     *
     * @return {BinaryObject} - 
     */
    static fromObject(object, complexObjectType = null) {
        const result = new BinaryObject(complexObjectType);
        const binaryType = BinaryType._fromObject(object, complexObjectType);
        result._setType(binaryType);
        for (let field of binaryType.fields) {
            if (object && object[field.name] !== undefined) {
                result.setField(field.name, object[field.name], field.type);
            }
        }
        return result;
    }

    /** Private methods */

    static _fromBuffer(buffer, complexObjectType) {
        const result = new BinaryObject(complexObjectType);
        result._buffer = buffer;
        result._startPos = buffer.position;
        result._read();
        return result;
    }

    _toObject() {
        const result = new (this._type._getObjectConstructor());
        for (let field of this._fields.values()) {
            const binaryField = this._type.getField(field.id);
            if (!binaryField) {
                throw new Errors.IgniteClientError(
                    Util.format('??? Complex object field with id "%d" can not be deserialized', field.id));
            }
            result[binaryField.name] = field.value;
        }
        return result;
    }

    _setType(binaryType) {
        this._type = binaryType;
        this._schema = binaryType._getSchemas()[0];
    }

    _write(buffer) {
        if (this._buffer && !this._modified) {
            buffer._writeBuffer(this._buffer.getSlice(this._startPos, this._startPos + this._length));
        }
        else {
            this._startPos = buffer.position;
            buffer.position = this._startPos + HEADER_LENGTH;
            // write fields
            for (let field of this._fields.values()) {
                field._writeValue(buffer);
            }
            this._schemaOffset = buffer.position - this._startPos;
            // write schema
            for (let field of this._fields.values()) {
                field._writeSchema(buffer, this._startPos);
            }
            this._length = buffer.position - this._startPos;
            this._buffer = buffer;
            // write header
            this._writeHeader();
            this._buffer.position = this._startPos + this._length;
            this._modified = false;
        }

        if (Logger.debug) {
            Logger.logDebug('BinaryObject._write [' + [...this._buffer.getSlice(this._startPos, this._startPos + this._length)] + ']');
        }
    }

    _writeHeader() {
        this._buffer.position = this._startPos;
        // type code
        this._buffer.writeByte(BinaryUtils.TYPE_CODE.COMPLEX_OBJECT);
        // version
        this._buffer.writeByte(VERSION);
        // flags
        this._buffer.writeShort(FLAG_HAS_SCHEMA);
        // type id
        this._buffer.writeInteger(this._type.id);
        // hash code
        this._buffer.writeInteger(BinaryUtils.contentHashCode(
            this._buffer, this._startPos + HEADER_LENGTH, this._schemaOffset - 1));
        // length
        this._buffer.writeInteger(this._length);
        // schema id
        this._buffer.writeInteger(this._schema.id);
        // schema offset
        this._buffer.writeInteger(this._schemaOffset);
    }

    _read() {
        this._readHeader();
        this._buffer.position = this._startPos + this._schemaOffset;
        this._schema = new BinarySchema();
        const fieldOffsets = new Array();
        let fieldId;
        while (this._buffer.position < this._startPos + this._length) {
            fieldId = this._buffer.readInteger();
            this._schema._addFieldId(fieldId);
            fieldOffsets.push([fieldId, this._buffer.readInteger()]);
        }
        fieldOffsets.sort((val1, val2) => val1[1] - val2[1]);
        let offset;
        let nextOffset;
        let binaryField;
        let field;
        for (let i = 0; i < fieldOffsets.length; i++) {
            fieldId = fieldOffsets[i][0];
            offset = fieldOffsets[i][1];
            nextOffset = i + 1 < fieldOffsets.length ? fieldOffsets[i + 1][1] : this._schemaOffset;
            binaryField = this._type.getField(fieldId);
            field = BinaryObjectField._fromBuffer(
                this._buffer, this._startPos + offset, nextOffset - offset, fieldId, binaryField);
            this._fields.set(field.id, field);
        }
        this._buffer.position = this._startPos + this._length;
    }

    _readHeader() {
        // type code
        this._buffer.readByte();
        // version
        const version = this._buffer.readByte();
        if (version !== VERSION) {
            throw Errors.IgniteClientError.internalError();
        }
        // flags
        this._buffer.readShort();
        // type id
        const typeId = this._buffer.readInteger();
        // hash code
        this._buffer.readInteger();
        // length
        this._length = this._buffer.readInteger();
        // schema id
        const schemaId = this._buffer.readInteger();
        // schema offset
        this._schemaOffset = this._buffer.readInteger();
    }
}

/**
 * @ignore
 */
class BinaryObjectField {
    constructor(name, type = null, value = undefined) {
        this._name = name;
        this._id = BinaryField._calculateId(name);
        this._type = type;
        this._value = value;
    }

    get id() {
        return this._id;
    }

    get name() {
        return this._name;
    }

    get value() {
        if (this._value === undefined) {
            this._buffer.position = this._offset;
            const BinaryReader = require('./internal/BinaryReader');
            this._value = BinaryReader.readObject(this._buffer, this._type);
        }
        return this._value;
    }

    get type() {
        return this._type;
    }

    static _fromBuffer(buffer, offset, length, id, binaryField) {
        let name = null;
        let type = null;
        if (binaryField) {
            name = binaryField.name;
            type = binaryField.type;
        }
        const result = new BinaryObjectField(name, type);
        result._id = id;
        result._buffer = buffer;
        result._offset = offset;
        result._length = length;
        return result;
    }

    _writeValue(buffer) {
        const offset = buffer.position;
        if (this._buffer) {
            buffer._writeBuffer(this._buffer.getSlice(this._offset, this._offset + this._length));
        }
        else {
            const BinaryWriter = require('./internal/BinaryWriter');
            BinaryWriter.writeObject(buffer, this._value, this._type ? this._type : null);
        }
        this._buffer = buffer;
        this._length = buffer.position - offset;
        this._offset = offset;
    }

    _writeSchema(buffer, headerStartPos) {
        buffer.writeInteger(this._id);
        buffer.writeInteger(this._offset - headerStartPos);
    }
}

module.exports = BinaryObject;
