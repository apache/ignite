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
const BinaryField = require('./internal/BinaryType').BinaryField;
const BinaryTypeBuilder = require('./internal/BinaryType').BinaryTypeBuilder;
const ArgumentChecker = require('./internal/ArgumentChecker');
const Logger = require('./internal/Logger');

const HEADER_LENGTH = 24;
const VERSION = 1;

// user type
const FLAG_USER_TYPE = 0x0001;
// schema exists
const FLAG_HAS_SCHEMA = 0x0002;
// object contains raw data
const FLAG_HAS_RAW_DATA = 0x0004;
// offsets take 1 byte
const FLAG_OFFSET_ONE_BYTE = 0x0008;
// offsets take 2 bytes
const FLAG_OFFSET_TWO_BYTES = 0x0010;
// compact footer, no field IDs
const FLAG_COMPACT_FOOTER = 0x0020;

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
     * @param {string} typeName - name of the complex type to generate the type Id.
     *
     * @return {BinaryObject} - new BinaryObject instance.
     *
     * @throws {IgniteClientError} if error.
     */
    constructor(typeName) {
        ArgumentChecker.notEmpty(typeName, 'typeName');
        this._buffer = null;
        this._fields = new Map();
        this._typeBuilder = BinaryTypeBuilder.fromTypeName(typeName);
        this._modified = false;
        this._schemaOffset = null;
        this._hasSchema = false;
        this._compactFooter = false;
        this._hasRawData = false;
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
     * @async
     *
     * @param {object} jsObject - instance of JavaScript Object
     *   which adds and initializes the fields of the BinaryObject instance.
     * @param {ComplexObjectType} [complexObjectType] - instance of complex type definition
     *   which specifies non-standard mapping of the fields of the BinaryObject instance
     *   to/from the Ignite types.
     *
     * @return {BinaryObject} - new BinaryObject instance.
     *
     * @throws {IgniteClientError} if error.
     */
    static async fromObject(jsObject, complexObjectType = null) {
        ArgumentChecker.notEmpty(jsObject, 'jsObject');
        ArgumentChecker.hasType(complexObjectType, 'complexObjectType', false, ComplexObjectType);
        const typeBuilder = BinaryTypeBuilder.fromObject(jsObject, complexObjectType);
        const result = new BinaryObject(typeBuilder.getTypeName());
        result._typeBuilder = typeBuilder;
        let fieldName;
        for (let field of result._typeBuilder.getFields()) {
            fieldName = field.name;
            if (jsObject && jsObject[fieldName] !== undefined) {
                result.setField(
                    fieldName,
                    jsObject[fieldName],
                    complexObjectType ? complexObjectType._getFieldType(fieldName) : null);
            }
            else {
                throw Errors.IgniteClientError.serializationError(
                    true, Util.format('field "%s" is undefined', fieldName));
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
     *
     * @throws {IgniteClientError} if error.
     */
    setField(fieldName, fieldValue, fieldType = null) {
        ArgumentChecker.notEmpty(fieldName, 'fieldName');
        this._modified = true;
        const field = new BinaryObjectField(fieldName, fieldValue, fieldType);
        this._fields.set(field.id, field);
        this._typeBuilder.setField(fieldName, field.typeCode);
        return this;
    }

    /**
     * Removes the specified field.
     * Does nothing if the field does not exist.
     *
     * @param {string} fieldName - name of the field.
     *
     * @return {BinaryObject} - the same instance of BinaryObject
     *
     * @throws {IgniteClientError} if error.
     */
    removeField(fieldName) {
        ArgumentChecker.notEmpty(fieldName, 'fieldName');
        this._modified = true;
        this._fields.delete(BinaryField._calculateId(fieldName));
        this._typeBuilder.removeField(fieldName);
        return this;
    }

    /**
     * Checks if the specified field exists in this BinaryObject instance.
     *
     * @param {string} fieldName - name of the field.
     *
     * @return {boolean} - true if exists, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    hasField(fieldName) {
        ArgumentChecker.notEmpty(fieldName, 'fieldName');
        return this._fields.has(BinaryField._calculateId(fieldName));
    }

    /**
     * Returns a value of the specified field.
     *
     * Optionally, specifies a type of the field.
     * If the type is not specified then the Ignite client
     * will try to make automatic mapping between JavaScript types and Ignite object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @async
     *
     * @param {string} fieldName - name of the field.
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} [fieldType] - type of the field:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified.
     *
     * @return {*} - value of the field or JavaScript undefined if the field does not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    async getField(fieldName, fieldType = null) {
        ArgumentChecker.notEmpty(fieldName, 'fieldName');
        const field = this._fields.get(BinaryField._calculateId(fieldName));
        return field ? await field.getValue(fieldType) : undefined;
    }

    /**
     * Deserializes this BinaryObject instance into an instance of the specified complex object type.
     *
     * @async
     *
     * @param {ComplexObjectType} complexObjectType - instance of class representing complex object type.
     *
     * @return {object} - instance of the JavaScript object
     *   which corresponds to the specified complex object type.
     *
     * @throws {IgniteClientError} if error.
     */
    async toObject(complexObjectType) {
        ArgumentChecker.notNull(complexObjectType, 'complexObjectType');
        ArgumentChecker.hasType(complexObjectType, 'complexObjectType', false, ComplexObjectType);
        const result = new (complexObjectType._objectConstructor);
        let binaryField;
        let fieldName;
        for (let field of this._fields.values()) {
            binaryField = this._typeBuilder.getField(field.id);
            if (!binaryField) {
                throw Errors.IgniteClientError.serializationError(
                    false, Util.format('field with id "%s" can not be deserialized', field.id));
            }
            fieldName = binaryField.name;
            result[fieldName] = await field.getValue(complexObjectType._getFieldType(fieldName));
        }
        return result;
    }

    /**
     * Returns type name of this BinaryObject instance.
     *
     * @return {string} - type name.
     */
    getTypeName() {
        return this._typeBuilder.getTypeName();
    }

    /**
     * Returns names of all fields of this BinaryObject instance.
     *
     * @return {Array<string>} - names of all fields.
     *
     * @throws {IgniteClientError} if error.
     */
    getFieldNames() {
        return this._typeBuilder._schema.fieldIds.map(fieldId => {
            const field = this._typeBuilder.getField(fieldId);
            if (field) {
                return field.name;
            }
            else {
                throw Errors.IgniteClientError.internalError(
                    Util.format('Field "%s" is absent in binary type fields', fieldId));
            }
        });
    }

    /** Private methods */

    /**
     * @ignore
     */
    static _isFlagSet(flags, flag) {
        return (flags & flag) === flag;
    }

    /**
     * @ignore
     */
    static async _fromBuffer(communicator, buffer) {
        const result = new BinaryObject(new ComplexObjectType({})._typeName);
        result._buffer = buffer;
        result._startPos = buffer.position;
        await result._read(communicator);
        return result;
    }

    /**
     * @ignore
     */
    async _write(communicator, buffer) {
        if (this._buffer && !this._modified) {
            buffer.writeBuffer(this._buffer.buffer, this._startPos, this._startPos + this._length);
        }
        else {
            await this._typeBuilder.finalize(communicator);
            this._startPos = buffer.position;
            buffer.position = this._startPos + HEADER_LENGTH;
            this._hasSchema = (this._fields.size > 0);
            if (this._hasSchema) {
                let field;
                // write fields
                for (field of this._fields.values()) {
                    await field._writeValue(communicator, buffer, this._typeBuilder.getField(field.id).typeCode);
                }
                this._schemaOffset = buffer.position - this._startPos;
                this._offsetType = field.getOffsetType(this._startPos);
                // write schema
                for (let field of this._fields.values()) {
                    field._writeOffset(buffer, this._startPos, this._offsetType);
                }
            }
            else {
                this._schemaOffset = 0;
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
        let flags = FLAG_USER_TYPE;
        if (this._hasSchema) {
            flags = flags | FLAG_HAS_SCHEMA | FLAG_COMPACT_FOOTER;
        }
        if (this._offsetType === BinaryUtils.TYPE_CODE.BYTE) {
            flags = flags | FLAG_OFFSET_ONE_BYTE;
        }
        else if (this._offsetType === BinaryUtils.TYPE_CODE.SHORT) {
            flags = flags | FLAG_OFFSET_TWO_BYTES;
        }
        this._buffer.writeShort(flags);
        // type id
        this._buffer.writeInteger(this._typeBuilder.getTypeId());
        // hash code
        this._buffer.writeInteger(BinaryUtils.contentHashCode(
            this._buffer, this._startPos + HEADER_LENGTH, this._schemaOffset - 1));
        // length
        this._buffer.writeInteger(this._length);
        // schema id
        this._buffer.writeInteger(this._hasSchema ? this._typeBuilder.getSchemaId() : 0);
        // schema offset
        this._buffer.writeInteger(this._schemaOffset);
    }

    /**
     * @ignore
     */
    async _read(communicator) {
        await this._readHeader(communicator);
        if (this._hasSchema) {
            this._buffer.position = this._startPos + this._schemaOffset;
            const fieldOffsets = new Array();
            const fieldIds = this._typeBuilder._schema.fieldIds;
            let index = 0;
            let fieldId;
            let schemaEndOffset = this._startPos + this._length;
            if (this._hasRawData) {
                schemaEndOffset -= BinaryUtils.getSize(BinaryUtils.TYPE_CODE.INTEGER);
            }
            while (this._buffer.position < schemaEndOffset) {
                if (!this._compactFooter) {
                    fieldId = this._buffer.readInteger();
                    this._typeBuilder._schema.addField(fieldId);
                }
                else {
                    if (index >= fieldIds.length) {
                        throw Errors.IgniteClientError.serializationError(
                            false, 'wrong number of fields in schema');
                    }
                    fieldId = fieldIds[index];
                    index++;
                }
                fieldOffsets.push([fieldId, this._buffer.readNumber(this._offsetType, false)]);
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
                    communicator,this._buffer, this._startPos + offset, nextOffset - offset, fieldId);
                this._fields.set(field.id, field);
            }
        }
        this._buffer.position = this._startPos + this._length;
    }

    /**
     * @ignore
     */
    async _readHeader(communicator) {
        // type code
        this._buffer.readByte();
        // version
        const version = this._buffer.readByte();
        if (version !== VERSION) {
            throw Errors.IgniteClientError.internalError();
        }
        // flags
        const flags = this._buffer.readShort();
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
        this._hasSchema = BinaryObject._isFlagSet(flags, FLAG_HAS_SCHEMA);
        this._compactFooter = BinaryObject._isFlagSet(flags, FLAG_COMPACT_FOOTER);
        this._hasRawData = BinaryObject._isFlagSet(flags, FLAG_HAS_RAW_DATA);
        this._offsetType = BinaryObject._isFlagSet(flags, FLAG_OFFSET_ONE_BYTE) ?
            BinaryUtils.TYPE_CODE.BYTE :
            BinaryObject._isFlagSet(flags, FLAG_OFFSET_TWO_BYTES) ?
                BinaryUtils.TYPE_CODE.SHORT :
                BinaryUtils.TYPE_CODE.INTEGER;
        this._typeBuilder = await BinaryTypeBuilder.fromTypeId(communicator, typeId, this._compactFooter ? schemaId : null);
    }
}

/**
 * @ignore
 */
class BinaryObjectField {
    constructor(name, value = undefined, type = null) {
        this._name = name;
        this._id = BinaryField._calculateId(name);
        this._value = value;
        this._type = type;
        if (!type && value !== undefined && value !== null) {
            this._type = BinaryUtils.calcObjectType(value);
        }
        this._typeCode = null;
        if (this._type) {
            this._typeCode = BinaryUtils.getTypeCode(this._type);
        }
    }

    get id() {
        return this._id;
    }

    get typeCode() {
        return this._typeCode;
    }

    async getValue(type = null) {
        if (this._value === undefined || this._buffer && this._type !== type) {
            this._buffer.position = this._offset;
            this._value = await this._communicator.readObject(this._buffer, type);
            this._type = type;
        }
        return this._value;
    }

    getOffsetType(headerStartPos) {
        let offset = this._offset - headerStartPos;
        if (offset < 0x100) {
            return BinaryUtils.TYPE_CODE.BYTE;
        }
        else if (offset < 0x10000) {
            return BinaryUtils.TYPE_CODE.SHORT;
        }
        return BinaryUtils.TYPE_CODE.INTEGER;
    }

    static _fromBuffer(communicator, buffer, offset, length, id) {
        const result = new BinaryObjectField(null);
        result._id = id;
        result._communicator = communicator;
        result._buffer = buffer;
        result._offset = offset;
        result._length = length;
        return result;
    }

    async _writeValue(communicator, buffer, expectedTypeCode) {
        const offset = buffer.position;
        if (this._buffer && this._communicator === communicator) {
            buffer.writeBuffer(this._buffer.buffer, this._offset, this._offset + this._length);
        }
        else {
            if (this._value === undefined) {
                await this.getValue();
            }
            BinaryUtils.checkCompatibility(this._value, expectedTypeCode);
            await communicator.writeObject(buffer, this._value, this._type);
        }
        this._communicator = communicator;
        this._buffer = buffer;
        this._length = buffer.position - offset;
        this._offset = offset;
    }

    _writeOffset(buffer, headerStartPos, offsetType) {
        buffer.writeNumber(this._offset - headerStartPos, offsetType, false);
    }
}

module.exports = BinaryObject;
