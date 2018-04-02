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

const BinaryUtils = require('./BinaryUtils');

/** FNV1 hash offset basis. */
const FNV1_OFFSET_BASIS = 0x811C9DC5;
/** FNV1 hash prime. */
const FNV1_PRIME = 0x01000193;

class BinarySchema {
    constructor() {
        this._id = BinarySchema._schemaInitialId();
        this._fieldIds = new Array();
    }

    get id() {
        return this._id;
    }

    get fieldIds() {
        return this._fieldIds;
    }

    _addFieldId(fieldId) {
        this._fieldIds.push(fieldId);
        this._id = BinarySchema._updateSchemaId(this._id, fieldId);
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
}

module.exports = BinarySchema;
