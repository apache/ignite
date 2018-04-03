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

const CacheClient = require('./CacheClient');
const IgniteClientConfiguration = require('./IgniteClientConfiguration');
const CacheConfiguration = require('./CacheConfiguration');
const BinaryUtils = require('./internal/BinaryUtils');
const BinaryWriter = require('./internal/BinaryWriter');
const BinaryReader = require('./internal/BinaryReader');
const ArgumentChecker = require('./internal/ArgumentChecker');
const Logger = require('./internal/Logger');

/**
 * State of Ignite client.
 *
 * DISCONNECTED - the client is not connected to any Ignite node,
 * operations with the Ignite server are not allowed.
 * This is initial state after a client instance creation.
 * If connect() method is called, the client moves to CONNECTING state.
 *
 * CONNECTING - the client tries to connect to an Ignite node,
 * operations with the Ignite server are not allowed.
 * If disconnect() method is called, the client moves to DISCONNECTED state.
 * If not possible to connect to any Ignite node, the client moves to DISCONNECTED state.
 * If connection to an Ignite node is successful, the client moves to CONNECTED state.
 *
 * CONNECTED - the client is connected to an Ignite node,
 * all operations with the Ignite server are allowed.
 * If connection with the Ignite node is lost, the client moves to CONNECTING state.
 * If disconnect() method is called, the client moves to DISCONNECTED state.
 *
 * @typedef IgniteClient.STATE
 * @enum
 * @readonly
 * @property DISCONNECTED
 * @property CONNECTING
 * @property CONNECTED
 */
const STATE = Object.freeze({
    DISCONNECTED : 0,
    CONNECTING : 1,
    CONNECTED : 2
});

/**
 * Class representing Ignite client.
 * 
 */
class IgniteClient {

    /**
     * Public constructor.
     *
     * @param {IgniteClient.onStateChanged} [onStateChanged] - (optional)
     * callback called everytime when the client has moved to a new state {@link IgniteClient.STATE}.
     *
     * @return {IgniteClient} - new IgniteClient instance.
     */
    constructor(onStateChanged = null) {
        const ClientFailoverSocket = require('./internal/ClientFailoverSocket');
        this._socket = new ClientFailoverSocket(onStateChanged);
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
    async connect(config) {
        ArgumentChecker.notEmpty(config, 'config');
        ArgumentChecker.hasType(config, 'config', IgniteClientConfiguration);
        await this._socket.connect(config);
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
     * @param {CacheConfiguration} [cacheConfig] - optional cache configuration.
     *
     * @return {Promise<CacheClient>} - new cache client instance for the created cache.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {OperationError} if cache with the provided name already exists.
     * @throws {IgniteClientError} if other error.
     */
    async createCache(name, cacheConfig = null) {
        ArgumentChecker.notEmpty(name, 'name');
        ArgumentChecker.hasType(cacheConfig, 'cacheConfig', CacheConfiguration);

        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_CREATE_WITH_NAME,
            (payload) => {
                BinaryWriter.writeString(payload, name);
            });
        return this._getCache(name, cacheConfig);
    }

    /**
     * Gets existing cache with the provided name
     * or creates new one with the provided name and optional configuration.
     *
     * @async
     *
     * @param {string} name - cache name.
     * @param {CacheConfiguration} [cacheConfig] - optional cache configuration (ignored if cache
     *   with the provided name already exists).
     *
     * @return {Promise<CacheClient>} - new cache client instance for the existing or created cache.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {IgniteClientError} if other error.
     */
    async getOrCreateCache(name, cacheConfig = null) {
        ArgumentChecker.notEmpty(name, 'name');
        ArgumentChecker.hasType(cacheConfig, 'cacheConfig', CacheConfiguration);

        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_GET_OR_CREATE_WITH_NAME,
            (payload) => {
                BinaryWriter.writeString(payload, name);
            });
        return this._getCache(name, cacheConfig);
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
        ArgumentChecker.notEmpty(name, 'name');
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
    async destroyCache(name) {
        ArgumentChecker.notEmpty(name, 'name');
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_DESTROY,
            (payload) => {
                payload.writeInteger(CacheClient._calculateId(name));
            });
    }

    /**
     * Gets existing cache names.
     *
     * @async
     *
     * @return {Array<string>} - array with the existing cache names.
     *     The array is empty if no caches exist.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {IgniteClientError} if other error.
     */
    async cacheNames() {
        let names;
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_GET_NAMES,
            null,
            (payload) => {
                names = BinaryReader.readStringArray(payload);
            });
        return names;
    }

    /**
     * Enables/disables the library debug output (including errors logging).
     * Disabled by default.
     *
     * @param {boolean} value - true to enable, false to disable
     */
    setDebug(value) {
        Logger.debug = value;
    }

    /** Private methods */

    /**
     * @ignore
     */
    _getCache(name, cacheConfig = null) {
        return new CacheClient(name, cacheConfig, this._socket);
    }
}

module.exports = IgniteClient;
