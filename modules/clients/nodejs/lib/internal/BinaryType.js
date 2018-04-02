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

const ComplexObjectType = require('../ObjectType').ComplexObjectType;
const BinaryField = require('./BinaryField');
const BinarySchema = require('./BinarySchema');
const BinaryUtils = require('./BinaryUtils');

class BinaryType {
    constructor(name) {
        this._name = name;
        this._id = BinaryType._calculateId(name);
        this._fields = new Map();
        this._schemas = new Map();
        this._objectConstructor = null;
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

    static _calculateId(name) {
        return BinaryUtils.hashCode(name);
    }

    static _fromObject(object, complexObjectType) {
        if (!complexObjectType) {
            complexObjectType = new ComplexObjectType();
        }
        const result = new BinaryType(complexObjectType._typeName);
        result._objectConstructor = complexObjectType._getObjectConstructor();
        const schema = new BinarySchema();
        if (object) {
            result._addFields(schema, object, complexObjectType);
        }
        if (complexObjectType._objectTemplate) {
            result._addFields(schema, complexObjectType._objectTemplate, complexObjectType);
        }
        for (let [fieldName, fieldType] of complexObjectType._getFields()) {
            result._addField(schema, fieldName, fieldType);
        }
        result._schemas.set(schema.id, schema);
        return result;
    }

    static _fromObjectType(complexObjectType) {
        return BinaryType._fromObject(null, complexObjectType);
    }

    _getSchemas() {
        return [...this._schemas.values()];
    }

    _addFields(schema, objectTemplate, complexObjectType) {
        let fieldValue;
        let fieldType;
        for (let fieldName in objectTemplate) {
            fieldValue = objectTemplate[fieldName];
            fieldType = complexObjectType._getFieldType(fieldName);
            if (!fieldType && BinaryUtils.calcObjectTypeCode(fieldValue) === BinaryUtils.TYPE_CODE.COMPLEX_OBJECT) {
                fieldType = new ComplexObjectType(fieldValue);
            }
            this._addField(schema, fieldName, fieldType);
        }
    }

    _addField(schema, fieldName, fieldType = null) {
        if (!this._fields.has(BinaryField._calculateId(fieldName))) {
            const field = new BinaryField(fieldName, fieldType);
            this._fields.set(field.id, field);
            schema._addFieldId(field.id);
        }
    }

    _getObjectConstructor() {
        return this._objectConstructor;
    }
}

module.exports = BinaryType;
