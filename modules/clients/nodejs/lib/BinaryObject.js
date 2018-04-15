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
 * Class representing a complex Ignite object in the binary form.
 *
 * It corresponds to COMPOSITE_TYPE.COMPLEX_OBJECT {@link ObjectType.COMPOSITE_TYPE},
 * has mandatory type Id, which corresponds to a name of the complex type,
 * and includes optional fields.
 *
 * An instance of the BinaryObject can be obtained/created by the following ways:
 *   - returned by the client when a complex object is received from Ignite cache
 * and is not deserialized to another JavaScript object.
 *   - created using the public constructor. Fields may be added to such an instance using setField() method.
 *   - created from a JavaScript object using static fromObject() method.
 */
class BinaryObject {

    /**
     * Creates an instance of the BinaryObject without any fields.
     *
     * Fields may be added later using setField() method.
     *
     * @param {string | number} typeNameOrId - name of the complex type to generate the type Id
     *   or the type Id itself.
     *
     * @return {BinaryObject} - new BinaryObject instance.
     */
    constructor(typeName) {
        this._buffer = null;
        this._fields = new Map();
        this._setType(new BinaryType(typeName));
        this._modified = false;
        this._schemaOffset = null;
    }

    /**
     * Creates an instance of the BinaryObject from the specified instance of JavaScript Object.
     *
     * All fields of the JavaScript Object instance with their values are added to the BinaryObject.
     * Fields may be added or removed later using setField() and removeField() methods.
     *
     * If complexObjectType parameter is specified, then the type Id is taken from it.
     * Otherwise, the type Id is generated from the name of the JavaScript Object.
     *
     * @param {object} jsObject - instance of JavaScript Object
     *   which adds and initializes the fields of the BinaryObject instance.
     * @param {ComplexObjectType} [complexObjectType] - instance of complex type definition
     *   which specifies non-standard mapping of the fields of the BinaryObject instance
     *   to/from the Ignite types.
     *
     * @return {BinaryObject} - new BinaryObject instance.
     */
    static fromObject(jsObject, complexObjectType = null) {
        ArgumentChecker.hasType(complexObjectType, 'complexObjectType', false, ComplexObjectType);
        const result = new BinaryObject(null);
        const binaryType = BinaryType._fromObjectType(complexObjectType, jsObject);
        result._setType(binaryType);
        for (let field of binaryType.fields) {
            if (jsObject && jsObject[field.name] !== undefined) {
                result.setField(field.name, jsObject[field.name], field.type);
            }
        }
        return result;
    }

    /**
     * Sets the new value of the specified field.
     * Adds the specified field, if it did not exist before.
     *
     * Optionally, specifies a type of the field.
     * If the type is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {string} fieldName - name of the field.
     * @param {*} fieldValue - new value of the field.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - type of the field:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified.
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
     * Removes the specified field.
     *
     * @param {string} fieldName - name of the field.
     *
     * @return {BinaryObject} - the same instance of BinaryObject
     */
    removeField(fieldName) {
        this._modified = true;
        this._fields.delete(BinaryField._calculateId(fieldName));
    }

    /**
     * Clones this BinaryObject instance.
     *
     * @return {BinaryObject} - new BinaryObject instance with the same type Id and the fields as the current one has.
     */
    clone() {
        // TODO
    }

    /**
     * Checks if the specified field exists in this BinaryObject instance.
     *
     * @param {string} fieldName - name of the field.
     *
     * @return {boolean} - true if exists, false otherwise.
     */
    hasField(fieldName) {
        return this._fields.has(BinaryField._calculateId(fieldName));
    }

    /**
     * Returns a value of the specified field.
     *
     * Optionally, specifies a type of the field.
     * If the type is not specified then during operations the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {string} fieldName - name of the field.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - type of the field:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified.
     *
     * @return {*} - value of the field or {@link undefined} if the field does not exist.
     */
    getField(fieldName, fieldType = null) {
        const field = this._fields.get(BinaryField._calculateId(fieldName));
        return field ? field.getValue(fieldType) : field;
    }

    /**
     * Deserializes this BinaryObject instance into an instance of the specified complex object type.
     *
     * @param {ComplexObjectType} complexObjectType - instance of class representing complex object type.
     *
     * @return {object} - instance of the JavaScript object
     *   which corresponds to the specified complex object type.
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
     * Returns type Id of this BinaryObject instance.
     *
     * @return {integer} - type Id.
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
