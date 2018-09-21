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
const BinaryUtils = require('./BinaryUtils');
const Util = require('util');

class BinaryTypeStorage {

    constructor(communicator) {
        this._communicator = communicator;
        this._types = new Map();
    }

    static getByComplexObjectType(complexObjectType) {
        return BinaryTypeStorage.complexObjectTypes.get(complexObjectType);
    }

    static setByComplexObjectType(complexObjectType, type, schema) {
        if (!BinaryTypeStorage.complexObjectTypes.has(complexObjectType)) {
            BinaryTypeStorage.complexObjectTypes.set(complexObjectType, [type, schema]);
        }
    }

    static get complexObjectTypes() {
        if (!BinaryTypeStorage._complexObjectTypes) {
            BinaryTypeStorage._complexObjectTypes = new Map();
        }
        return BinaryTypeStorage._complexObjectTypes;
    }

    async addType(binaryType, binarySchema) {
        const typeId = binaryType.id;
        const schemaId = binarySchema.id;
        let storageType = this._types.get(typeId);
        if (!storageType || !storageType.hasSchema(schemaId)) {
            binaryType.addSchema(binarySchema);
            if (!storageType) {
                this._types.set(typeId, binaryType);
                storageType = binaryType;
            }
            else {
                storageType.merge(binaryType, binarySchema);
            }
            await this._putBinaryType(binaryType);
        }
    }

    async getType(typeId, schemaId = null) {
        let storageType = this._types.get(typeId);
        if (!storageType || schemaId && !storageType.hasSchema(schemaId)) {
            storageType = await this._getBinaryType(typeId);
            if (storageType) {
                this._types.set(storageType.id, storageType);
            }
        }
        return storageType;
    }

    /** Private methods */

    async _getBinaryType(typeId) {
        const BinaryType = require('./BinaryType');
        let binaryType = new BinaryType(null);
        binaryType._id = typeId;
        await this._communicator.send(
            BinaryUtils.OPERATION.GET_BINARY_TYPE,
            async (payload) => {
                payload.writeInteger(typeId);
            },
            async (payload) => {
                const exist = payload.readBoolean();
                if (exist) {
                    await binaryType._read(payload);
                }
                else {
                    binaryType = null;
                }
            });
        return binaryType;
    }

    async _putBinaryType(binaryType) {
        if (!binaryType.isValid()) {
            throw Errors.IgniteClientError.serializationError(
                true, Util.format('type "%d" can not be registered', binaryType.id));
        }
        await this._communicator.send(
            BinaryUtils.OPERATION.PUT_BINARY_TYPE,
            async (payload) => {
                await binaryType._write(payload);
            });
    }
}

module.exports = BinaryTypeStorage;
