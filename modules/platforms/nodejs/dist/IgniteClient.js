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
/*
import { CacheClient } from './CacheClient';
import { IgniteClientConfiguration } from './IgniteClientConfiguration';
import { CacheConfiguration } from './CacheConfiguration';
import { BinaryUtils } from './internal/BinaryUtils';
import { BinaryCommunicator } from './internal/BinaryCommunicator';
import { ArgumentChecker } from './internal/ArgumentChecker';
import { Logger } from './internal/Logger';
import { ClientFailoverSocket } from './internal/ClientFailoverSocket';
*/
const internal_1 = require("./internal");
/**
 * State of Ignite client.
 *
 * @typedef IgniteClient.STATE
 * @enum
 * @readonly
 * @property DISCONNECTED The client is not connected to any Ignite node,
 *     operations with the Ignite server are not allowed.
 *     This is initial state after a client instance creation.
 *     If connect() method is called, the client moves to CONNECTING state.
 * @property CONNECTING The client tries to connect to an Ignite node,
 *     operations with the Ignite server are not allowed.
 *     If disconnect() method is called, the client moves to DISCONNECTED state.
 *     If not possible to connect to any Ignite node, the client moves to DISCONNECTED state.
 *     If connection to an Ignite node is successful, the client moves to CONNECTED state.
 * @property CONNECTED The client is connected to an Ignite node,
 *     all operations with the Ignite server are allowed.
 *     If connection with the Ignite node is lost, the client moves to CONNECTING state.
 *     If disconnect() method is called, the client moves to DISCONNECTED state.
 */
const STATE = Object.freeze({
    DISCONNECTED: 0,
    CONNECTING: 1,
    CONNECTED: 2
});
/**
 * Class representing Ignite client.
 *
 */
class IgniteClient {
    /**
     * Public constructor.
     *
     * @param {IgniteClient.onStateChanged} [onStateChanged] -
     * callback called everytime when the client has moved to a new state {@link IgniteClient.STATE}.
     *
     * @return {IgniteClient} - new IgniteClient instance.
     */
    constructor(onStateChanged = null) {
        this._socket = new internal_1.ClientFailoverSocket(onStateChanged);
        this._communicator = new internal_1.BinaryCommunicator(this._socket);
    }
    static get STATE() {
        return STATE;
    }
    /**
     * onStateChanged callback.
     * @callback IgniteClient.onStateChanged
     * @param {IgniteClient.STATE} state - the new state of the client.
     * @param {string} reason - the reason why the state has been changed.
     */
    /**
     * Connects the client.
     *
     * Should be called from DISCONNECTED state only.
     * Moves the client to CONNECTING state.
     *
     * @async
     *
     * @param {IgniteClientConfiguration} config - the client configuration.
     *
     * @throws {IllegalStateError} if the client is not in DISCONNECTED {@link IgniteClient.STATE}.
     * @throws {IgniteClientError} if other error.
     */
    connect(config) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(config, 'config');
            internal_1.ArgumentChecker.hasType(config, 'config', false, internal_1.IgniteClientConfiguration);
            yield this._socket.connect(config);
        });
    }
    /**
     * Disconnects the client.
     *
     * Moves the client to DISCONNECTED state from any other state.
     * Does nothing if the client already disconnected.
     */
    disconnect() {
        if (this._socket) {
            this._socket.disconnect();
        }
    }
    /**
     * Creates new cache with the provided name and optional configuration.
     *
     * @async
     *
     * @param {string} name - cache name.
     * @param {CacheConfiguration} [cacheConfig] - cache configuration.
     *
     * @return {Promise<CacheClient>} - new cache client instance for the created cache.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {OperationError} if cache with the provided name already exists.
     * @throws {IgniteClientError} if other error.
     */
    createCache(name, cacheConfig = null) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(name, 'name');
            internal_1.ArgumentChecker.hasType(cacheConfig, 'cacheConfig', false, internal_1.CacheConfiguration);
            yield this._communicator.send(cacheConfig ?
                internal_1.BinaryUtils.OPERATION.CACHE_CREATE_WITH_CONFIGURATION :
                internal_1.BinaryUtils.OPERATION.CACHE_CREATE_WITH_NAME, (payload) => __awaiter(this, void 0, void 0, function* () {
                yield this._writeCacheNameOrConfig(payload, name, cacheConfig);
            }));
            return this._getCache(name, cacheConfig);
        });
    }
    /**
     * Gets existing cache with the provided name
     * or creates new one with the provided name and optional configuration.
     *
     * @async
     *
     * @param {string} name - cache name.
     * @param {CacheConfiguration} [cacheConfig] - cache configuration (ignored if cache
     *   with the provided name already exists).
     *
     * @return {Promise<CacheClient>} - new cache client instance for the existing or created cache.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {IgniteClientError} if other error.
     */
    getOrCreateCache(name, cacheConfig = null) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(name, 'name');
            internal_1.ArgumentChecker.hasType(cacheConfig, 'cacheConfig', false, internal_1.CacheConfiguration);
            yield this._communicator.send(cacheConfig ?
                internal_1.BinaryUtils.OPERATION.CACHE_GET_OR_CREATE_WITH_CONFIGURATION :
                internal_1.BinaryUtils.OPERATION.CACHE_GET_OR_CREATE_WITH_NAME, (payload) => __awaiter(this, void 0, void 0, function* () {
                yield this._writeCacheNameOrConfig(payload, name, cacheConfig);
            }));
            return this._getCache(name, cacheConfig);
        });
    }
    /**
     * Gets cache client instance of cache with the provided name.
     * The method does not check if the cache with the provided name exists.
     *
     * @param {string} name - cache name.
     *
     * @return {CacheClient} - new cache client instance.
     *
     * @throws {IgniteClientError} if error.
     */
    getCache(name) {
        internal_1.ArgumentChecker.notEmpty(name, 'name');
        return this._getCache(name);
    }
    /**
     * Destroys cache with the provided name.
     *
     * @async
     *
     * @param {string} name - cache name.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {OperationError} if cache with the provided name does not exist.
     * @throws {IgniteClientError} if other error.
     */
    destroyCache(name) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(name, 'name');
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_DESTROY, (payload) => __awaiter(this, void 0, void 0, function* () {
                payload.writeInteger(internal_1.CacheClient._calculateId(name));
            }));
        });
    }
    /**
     * Returns configuration of cache with the provided name.
     *
     * @async
     *
     * @param {string} name - cache name.
     *
     * @return {Promise<CacheConfiguration>} - cache configuration
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {OperationError} if cache with the provided name does not exist.
     * @throws {IgniteClientError} if other error.
     */
    getCacheConfiguration(name) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(name, 'name');
            let config;
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_GET_CONFIGURATION, (payload) => __awaiter(this, void 0, void 0, function* () {
                payload.writeInteger(internal_1.CacheClient._calculateId(name));
                payload.writeByte(0);
            }), (payload) => __awaiter(this, void 0, void 0, function* () {
                config = new internal_1.CacheConfiguration();
                yield config._read(this._communicator, payload);
            }));
            return config;
        });
    }
    /**
     * Gets existing cache names.
     *
     * @async
     *
     * @return {Promise<Array<string>>} - array with the existing cache names.
     *     The array is empty if no caches exist.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {IgniteClientError} if other error.
     */
    cacheNames() {
        return __awaiter(this, void 0, void 0, function* () {
            let names;
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_GET_NAMES, null, (payload) => __awaiter(this, void 0, void 0, function* () {
                names = yield this._communicator.readStringArray(payload);
            }));
            return names;
        });
    }
    /**
     * Enables/disables the library debug output (including errors logging).
     * Disabled by default.
     *
     * @param {boolean} value - true to enable, false to disable
     */
    setDebug(value) {
        internal_1.Logger.debug = value;
    }
    /** Private methods */
    /**
     * @ignore
     */
    _getCache(name, cacheConfig = null) {
        return new internal_1.CacheClient(name, cacheConfig, this._communicator);
    }
    /**
     * @ignore
     */
    _writeCacheNameOrConfig(buffer, name, cacheConfig) {
        return __awaiter(this, void 0, void 0, function* () {
            if (cacheConfig) {
                yield cacheConfig._write(this._communicator, buffer, name);
            }
            else {
                internal_1.BinaryCommunicator.writeString(buffer, name);
            }
        });
    }
}
exports.IgniteClient = IgniteClient;
//# sourceMappingURL=IgniteClient.js.map