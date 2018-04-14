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
 * 
 */
class BinaryObject {

    /**
     * 
     *
     * @param {string} typeName - 
     *
     * @return {BinaryObject} - 
     */
    constructor(typeName) {
        this._buffer = null;
        this._fields = new Map();
        this._setType(new BinaryType(typeName));
        this._modified = false;
        this._schemaOffset = null;
    }

    /**
     * 
     *
     * @param {object} object - 
     * @param {ComplexObjectType} [complexObjectType] - 
     *
     * @return {BinaryObject} - 
     */
    static fromObject(object, complexObjectType = null) {
        ArgumentChecker.hasType(complexObjectType, 'complexObjectType', false, ComplexObjectType);
        const result = new BinaryObject(null);
        const binaryType = BinaryType._fromObjectType(complexObjectType, object);
        result._setType(binaryType);
        for (let field of binaryType.fields) {
            if (object && object[field.name] !== undefined) {
                result.setField(field.name, object[field.name], field.type);
            }
        }
        return result;
    }

    /**
     * 
     *
     * @return {BinaryObject} - 
     */
    clone() {
        // TODO
    }

    /**
     * 
     *
     * @param {string} fieldName - 
     *
     * @return {boolean} - 
     */
    hasField(fieldName) {
        return this._fields.has(BinaryField._calculateId(fieldName));
    }

    /**
     * 
     *
     * @param {string} fieldName - 
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - 
     *
     * @return {*} - ??? or undefined if field does not exist
     */
    getField(fieldName, fieldType = null) {
        const field = this._fields.get(BinaryField._calculateId(fieldName));
        return field ? field.value : field;
    }

    /**
     * 
     *
     * @param {string} fieldName - 
     */
    removeField(fieldName) {
        this._modified = true;
        this._fields.delete(BinaryField._calculateId(fieldName));
    }

    /**
     * 
     *
     * @param {string} fieldName - 
     * @param {*} fieldValue - 
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - 
     *
     * @return {BinaryObject} - the same instance of BinaryObject
     */
    setField(fieldName, fieldValue, fieldType = null) {
        this._modified = true;
        const field = new BinaryObjectField(fieldName, fieldValue);
        this._fields.set(field.id, field);
        this._type._addField(this._schema, fieldName, fieldType);
        return this;
    }

    /**
     * 
     *
     * @param {ComplexObjectType} complexObjectType - 
     *
     * @return {object} - 
     */
    toObject(complexObjectType) {
        ArgumentChecker.hasType(complexObjectType, 'complexObjectType', false, ComplexObjectType);
        this._setType(BinaryType._fromObjectType(complexObjectType));
        const result = new (this._type._objectConstructor);
        for (let field of this._fields.values()) {
            const binaryField = this._type.getField(field.id);
            if (!binaryField) {
                throw new Errors.IgniteClientError(
                    Util.format('Complex object field with id "%d" can not be deserialized', field.id));
            }
            result[binaryField.name] = field.getValue(binaryField.type);
        }
        return result;
    }

    /**
     * 
     *
     * @return {integer} - 
     */
    getTypeId() {
        return this._type.id;
    }

    /** Private methods */

    /**
     * @ignore
     */
    static _fromBuffer(buffer) {
        const result = new BinaryObject(null);
        result._buffer = buffer;
        result._startPos = buffer.position;
        result._read();
        return result;
    }

    /**
     * @ignore
     */
    _setType(binaryType) {
        this._type = binaryType;
        this._schema = binaryType._getSchemas()[0];
    }

    /**
     * @ignore
     */
    _write(buffer) {
        if (this._buffer && !this._modified) {
            buffer._writeBuffer(this._buffer.getSlice(this._startPos, this._startPos + this._length));
        }
        else {
            this._startPos = buffer.position;
            buffer.position = this._startPos + HEADER_LENGTH;
            // write fields
            let binaryField;
            for (let field of this._fields.values()) {
                binaryField = this._type.getField(field.id);
                field._writeValue(buffer, binaryField.type);
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

    /**
     * @ignore
     */
    _writeHeader() {
        this._buffer.position = this._startPos;
        // type code
        this._buffer.writeByte(BinaryUtils.TYPE_CODE.COMPLEX_OBJECT);
        // version
        this._buffer.writeByte(VERSION);
        // flags
        this._buffer.writeShort(FLAG_HAS_SCHEMA | FLAG_USER_TYPE);
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

    /**
     * @ignore
     */
    _read() {
        this._readHeader();
        this._buffer.position = this._startPos + this._schemaOffset;
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
        let field;
        for (let i = 0; i < fieldOffsets.length; i++) {
            fieldId = fieldOffsets[i][0];
            offset = fieldOffsets[i][1];
            nextOffset = i + 1 < fieldOffsets.length ? fieldOffsets[i + 1][1] : this._schemaOffset;
            field = BinaryObjectField._fromBuffer(
                this._buffer, this._startPos + offset, nextOffset - offset, fieldId);
            this._fields.set(field.id, field);
        }
        this._buffer.position = this._startPos + this._length;
    }

    /**
     * @ignore
     */
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
        this._type._id = this._buffer.readInteger();
        // hash code
        this._buffer.readInteger();
        // length
        this._length = this._buffer.readInteger();
        // schema id
        this._schema._id = this._buffer.readInteger();
        // schema offset
        this._schemaOffset = this._buffer.readInteger();
    }
}

/**
 * @ignore
 */
class BinaryObjectField {
    constructor(name, value = undefined) {
        this._name = name;
        this._id = BinaryField._calculateId(name);
        this._value = value;
    }

    get id() {
        return this._id;
    }

    getValue(type = null) {
        if (this._value === undefined) {
            this._buffer.position = this._offset;
            const BinaryReader = require('./internal/BinaryReader');
            this._value = BinaryReader.readObject(this._buffer, type);
        }
        return this._value;
    }

    static _fromBuffer(buffer, offset, length, id) {
        const result = new BinaryObjectField(null);
        result._id = id;
        result._buffer = buffer;
        result._offset = offset;
        result._length = length;
        return result;
    }

    _writeValue(buffer, type = null) {
        const offset = buffer.position;
        if (this._buffer) {
            buffer._writeBuffer(this._buffer.getSlice(this._offset, this._offset + this._length));
        }
        else {
            const BinaryWriter = require('./internal/BinaryWriter');
            BinaryWriter.writeObject(buffer, this._value, type);
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
