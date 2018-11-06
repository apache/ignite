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

class BinaryTypeStorage {

    static getEntity() {
        if (!BinaryTypeStorage._entity) {
            throw Errors.IgniteClientError.internalError();
        }
        return BinaryTypeStorage._entity;
    }

    static createEntity(socket) {
        BinaryTypeStorage._entity = new BinaryTypeStorage(socket);
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

    getByComplexObjectType(complexObjectType) {
        return this._complexObjectTypes.get(complexObjectType);
    }

    setByComplexObjectType(complexObjectType, type, schema) {
        if (!this._complexObjectTypes.has(complexObjectType)) {
            this._complexObjectTypes.set(complexObjectType, [type, schema]);
        }
    }

    /** Private methods */

    constructor(socket) {
        this._socket = socket;
        this._types = new Map();
        this._complexObjectTypes = new Map();
    }

    async _getBinaryType(typeId) {
        const BinaryType = require('./BinaryType');
        let binaryType = new BinaryType(null);
        binaryType._id = typeId;
        await this._socket.send(
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
        await this._socket.send(
            BinaryUtils.OPERATION.PUT_BINARY_TYPE,
            async (payload) => {
                await binaryType._write(payload);
            });
    }
}

module.exports = BinaryTypeStorage;
