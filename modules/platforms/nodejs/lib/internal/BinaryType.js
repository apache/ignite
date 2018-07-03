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
const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const BinaryTypeStorage = require('./BinaryTypeStorage');
const BinaryUtils = require('./BinaryUtils');
const BinaryWriter = require('./BinaryWriter');
const Errors = require('../Errors');

class BinaryType {
    constructor(name) {
        this._name = name;
        this._id = BinaryType._calculateId(name);
        this._fields = new Map();
        this._schemas = new Map();
        this._isEnum = false;
        this._enumValues = null;
    }

    get id() {
        return this._id;
    }

    get name() {
        return this._name;
    }

    get fields() {
        return [...this._fields.values()];
    }

    getField(fieldId) {
        return this._fields.get(fieldId);
    }

    hasField(fieldId) {
        return this._fields.has(fieldId);
    }

    removeField(fieldId) {
        return this._fields.delete(fieldId);
    }

    setField(field) {
        this._fields.set(field.id, field);
    }

    hasSchema(schemaId) {
        return this._schemas.has(schemaId);
    }

    addSchema(schema) {
        if (!this.hasSchema(schema.id)) {
            this._schemas.set(schema.id, schema);
        }
    }

    getSchema(schemaId) {
        return this._schemas.get(schemaId);
    }

    merge(binaryType, binarySchema) {
        let fieldId;
        for (let field of binaryType.fields) {
            fieldId = field.id;
            if (this.hasField(fieldId)) {
                if (this.getField(fieldId).typeCode !== field.typeCode) {
                    throw Errors.IgniteClientError.serializationError(
                        true, Util.format('type conflict for field "%s" of complex object type "%s"'),
                        field.name, this._name);
                }
            }
            else {
                this.setField(field);
            }
        }
        this.addSchema(binarySchema);
    }

    clone() {
        const result = new BinaryType();
        result._name = this._name;
        result._id = this._id;
        result._fields = new Map(this._fields.entries());
        result._schemas = new Map(this._schemas.entries());
        result._isEnum = this._isEnum;
        return result;
    }

    static _calculateId(name) {
        return BinaryUtils.hashCodeLowerCase(name);
    }

    async _write(buffer) {
        // type id
        buffer.writeInteger(this._id);
        // type name
        await BinaryWriter.writeString(buffer, this._name);
        // affinity key field name
        await BinaryWriter.writeString(buffer, null);
        // fields count
        buffer.writeInteger(this._fields.size);
        // fields
        for (let field of this._fields.values()) {
            await field._write(buffer);
        }
        await this._writeEnum(buffer);
        // schemas count
        buffer.writeInteger(this._schemas.size);
        for (let schema of this._schemas.values()) {
            await schema._write(buffer);
        }
    }

    async _writeEnum(buffer) {
        buffer.writeBoolean(this._isEnum);
        if (this._isEnum) {
            const length = this._enumValues ? this._enumValues.length : 0;
            buffer.writeInteger(length);
            if (length > 0) {
                for (let [key, value] of this._enumValues) {
                    await BinaryWriter.writeString(buffer, key);
                    buffer.writeInteger(value);
                }
            }
        }
    }

    async _read(buffer) {
        // type id
        this._id = buffer.readInteger();
        // type name
        const BinaryReader = require('./BinaryReader');
        this._name = await BinaryReader.readObject(buffer);
        // affinity key field name
        await BinaryReader.readObject(buffer);
        // fields count
        const fieldsCount = buffer.readInteger();
        // fields
        let field;
        for (let i = 0; i < fieldsCount; i++) {
            field = new BinaryField(null, null);
            await field._read(buffer);
            this.setField(field);
        }
        await this._readEnum(buffer);
        // schemas count
        const schemasCount = buffer.readInteger();
        // schemas
        let schema;
        for (let i = 0; i < schemasCount; i++) {
            schema = new BinarySchema();
            await schema._read(buffer);
            this.addSchema(schema);
        }
    }

    async _readEnum(buffer) {
        const BinaryReader = require('./BinaryReader');
        this._isEnum = buffer.readBoolean();
        if (this._isEnum) {
            const valuesCount = buffer.readInteger();
            this._enumValues = new Array(valuesCount);
            for (let i = 0; i < valuesCount; i++) {
                this._enumValues[i] = [await BinaryReader.readObject(buffer), buffer.readInteger()];
            }
        }
    }
}

/** FNV1 hash offset basis. */
const FNV1_OFFSET_BASIS = 0x811C9DC5;
/** FNV1 hash prime. */
const FNV1_PRIME = 0x01000193;

class BinarySchema {
    constructor() {
        this._id = BinarySchema._schemaInitialId();
        this._fieldIds = new Set();
        this._isValid = true;
    }

    get id() {
        return this._id;
    }

    get fieldIds() {
        return [...this._fieldIds];
    }

    finalize() {
        if (!this._isValid) {
            this._id = BinarySchema._schemaInitialId();
            for (let fieldId of this._fieldIds) {
                this._id = BinarySchema._updateSchemaId(this._id, fieldId);
            }
            this._isValid = true;
        }
    }

    clone() {
        const result = new BinarySchema();
        result._id = this._id;
        result._fieldIds = new Set(this._fieldIds);
        result._isValid = this._isValid;
        return result;
    }

    addField(fieldId) {
        if (!this.hasField(fieldId)) {
            this._fieldIds.add(fieldId);
            if (this._isValid) {
                this._id = BinarySchema._updateSchemaId(this._id, fieldId);
            }
        }
    }

    removeField(fieldId) {
        if (this._fieldIds.delete(fieldId)) {
            this._isValid = false;
        }
    }

    hasField(fieldId) {
        return this._fieldIds.has(fieldId);
    }

    static _schemaInitialId() {
        return FNV1_OFFSET_BASIS | 0;
    }

    static _updateSchemaId(schemaId, fieldId) {
        schemaId = schemaId ^ (fieldId & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId |= 0;
        schemaId = schemaId ^ ((fieldId >> 8) & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId |= 0;
        schemaId = schemaId ^ ((fieldId >> 16) & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId |= 0;
        schemaId = schemaId ^ ((fieldId >> 24) & 0xFF);
        schemaId = schemaId * FNV1_PRIME;
        schemaId |= 0;

        return schemaId;
    }

    async _write(buffer) {
        this.finalize();
        // schema id
        buffer.writeInteger(this._id);
        // fields count
        buffer.writeInteger(this._fieldIds.size);
        // field ids
        for (let fieldId of this._fieldIds) {
            buffer.writeInteger(fieldId);
        }
    }

    async _read(buffer) {
        // schema id
        this._id = buffer.readInteger();
        // fields count
        const fieldsCount = buffer.readInteger();
        // field ids
        for (let i = 0; i < fieldsCount; i++) {
            this._fieldIds.add(buffer.readInteger());
        }
    }
}

class BinaryField {
    constructor(name, typeCode) {
        this._name = name;
        this._id = BinaryField._calculateId(name);
        this._typeCode = typeCode;
    }

    get id() {
        return this._id;
    }

    get name() {
        return this._name;
    }

    get typeCode() {
        return this._typeCode;
    }

    static _calculateId(name) {
        return BinaryUtils.hashCodeLowerCase(name);
    }

    async _write(buffer) {
        // field name
        await BinaryWriter.writeString(buffer, this._name);
        // type code
        buffer.writeInteger(this._typeCode);
        // field id
        buffer.writeInteger(this._id);
    }

    async _read(buffer) {
        const BinaryReader = require('./BinaryReader');
        // field name
        this._name = await BinaryReader.readObject(buffer);
        // type code
        this._typeCode = buffer.readInteger();
        // field id
        this._id = buffer.readInteger();
    }
}

class BinaryTypeBuilder {

    static fromTypeName(typeName) {
        let result = new BinaryTypeBuilder();
        result._init(typeName);
        return result;
    }

    static async fromTypeId(typeId, schemaId, hasSchema) {
        let result = new BinaryTypeBuilder();
        if (hasSchema) {
            let type = await BinaryTypeStorage.getEntity().getType(typeId, schemaId);
            if (type) {
                result._type = type;
                result._schema = type.getSchema(schemaId);
                if (!result._schema) {
                    throw Errors.IgniteClientError.serializationError(
                        false, Util.format('schema id "%d" specified for complex object of type "%s" not found',
                            schemaId, type.name));
                }
                result._fromStorage = true;
                return result;
            }
        }
        result._init(null);
        result._type._id = typeId;
        return result;
    }

    static fromObject(jsObject, complexObjectType = null) {
        if (complexObjectType) {
            return BinaryTypeBuilder.fromComplexObjectType(complexObjectType, jsObject);
        }
        else {
            const result = new BinaryTypeBuilder();
            result._fromComplexObjectType(new ComplexObjectType(jsObject), jsObject);
            return result;
        }
    }

    static fromComplexObjectType(complexObjectType, jsObject) {
        let result = new BinaryTypeBuilder();
        const typeInfo = BinaryTypeStorage.getEntity().getByComplexObjectType(complexObjectType);
        if (typeInfo) {
            result._type = typeInfo[0];
            result._schema = typeInfo[1];
            result._fromStorage = true;
        }
        else {
            result._fromComplexObjectType(complexObjectType, jsObject);
            BinaryTypeStorage.getEntity().setByComplexObjectType(complexObjectType, result._type, result._schema);
        }
        return result;        
    }

    getTypeId() {
        return this._type.id;
    }

    getTypeName() {
        return this._type.name;
    }

    getSchemaId() {
        return this._schema.id;
    }

    getFields() {
        return this._type.fields;
    }

    getField(fieldId) {
        return this._type._fields.get(fieldId);
    }

    setField(fieldName, fieldTypeCode = null) {
        const fieldId = BinaryField._calculateId(fieldName);
        if (!this._type.hasField(fieldId) || !this._schema.hasField(fieldId) ||
            this._type.getField(fieldId).typeCode !== fieldTypeCode) {
            this._beforeModify();
            this._type.setField(new BinaryField(fieldName, fieldTypeCode));
            this._schema.addField(fieldId);
        }
    }

    removeField(fieldName) {
        const fieldId = BinaryField._calculateId(fieldName);
        if (this._type.hasField(fieldId)) {
            this._beforeModify();
            this._type.removeField(fieldId);
            this._schema.removeField(fieldId);
        }
    }

    async finalize() {
        this._schema.finalize();
        await BinaryTypeStorage.getEntity().addType(this._type, this._schema);
    }

    constructor() {
        this._type = null;
        this._schema = null;
        this._fromStorage = false;
    }

    _fromComplexObjectType(complexObjectType, jsObject) {
        this._init(complexObjectType._typeName);
        if (complexObjectType._template) {
            this._setFields(complexObjectType, complexObjectType._template, jsObject);
        }
    }

    _init(typeName) {
        this._type = new BinaryType(typeName);
        this._schema = new BinarySchema();
    }

    _beforeModify() {
        if (this._fromStorage) {
            this._type = this._type.clone();
            this._schema = this._schema.clone();
            this._fromStorage = false;
        }
    }

    _setFields(complexObjectType, objectTemplate, jsObject) {
        let fieldType;
        for (let fieldName of BinaryUtils.getJsObjectFieldNames(objectTemplate)) {
            fieldType = complexObjectType._getFieldType(fieldName);
            if (!fieldType && jsObject[fieldName]) {
                fieldType = BinaryUtils.calcObjectType(jsObject[fieldName]);
            }
            this.setField(fieldName, BinaryUtils.getTypeCode(fieldType));
        }
    }
}

module.exports = BinaryType;
module.exports.BinaryField = BinaryField;
module.exports.BinaryTypeBuilder = BinaryTypeBuilder;
