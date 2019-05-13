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
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const Util = require("util");
const internal_1 = require("../internal");
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
    addType(binaryType, binarySchema) {
        return __awaiter(this, void 0, void 0, function* () {
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
                yield this._putBinaryType(binaryType);
            }
        });
    }
    getType(typeId, schemaId = null) {
        return __awaiter(this, void 0, void 0, function* () {
            let storageType = this._types.get(typeId);
            if (!storageType || schemaId && !storageType.hasSchema(schemaId)) {
                storageType = yield this._getBinaryType(typeId);
                if (storageType) {
                    this._types.set(storageType.id, storageType);
                }
            }
            return storageType;
        });
    }
    /** Private methods */
    _getBinaryType(typeId) {
        return __awaiter(this, void 0, void 0, function* () {
            let binaryType = new internal_1.BinaryType(null);
            binaryType._id = typeId;
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.GET_BINARY_TYPE, (payload) => __awaiter(this, void 0, void 0, function* () {
                payload.writeInteger(typeId);
            }), (payload) => __awaiter(this, void 0, void 0, function* () {
                const exist = payload.readBoolean();
                if (exist) {
                    yield binaryType._read(payload);
                }
                else {
                    binaryType = null;
                }
            }));
            return binaryType;
        });
    }
    _putBinaryType(binaryType) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!binaryType.isValid()) {
                throw internal_1.Errors.IgniteClientError.serializationError(true, Util.format('type "%d" can not be registered', binaryType.id));
            }
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.PUT_BINARY_TYPE, (payload) => __awaiter(this, void 0, void 0, function* () {
                yield binaryType._write(payload);
            }));
        });
    }
}
exports.BinaryTypeStorage = BinaryTypeStorage;
//# sourceMappingURL=BinaryTypeStorage.js.map