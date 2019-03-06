/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
