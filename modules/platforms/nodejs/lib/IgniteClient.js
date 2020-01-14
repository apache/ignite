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
const BinaryCommunicator = require('./internal/BinaryCommunicator');
const ArgumentChecker = require('./internal/ArgumentChecker');
const Logger = require('./internal/Logger');

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
     * @param {IgniteClient.onStateChanged} [onStateChanged] -
     * callback called everytime when the client has moved to a new state {@link IgniteClient.STATE}.
     *
     * @return {IgniteClient} - new IgniteClient instance.
     */
    constructor(onStateChanged = null) {
        const ClientFailoverSocket = require('./internal/ClientFailoverSocket');
        this._socket = new ClientFailoverSocket(onStateChanged);
        this._communicator = new BinaryCommunicator(this._socket);
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
        ArgumentChecker.hasType(config, 'config', false, IgniteClientConfiguration);
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
     * @param {CacheConfiguration} [cacheConfig] - cache configuration.
     *
     * @return {Promise<CacheClient>} - new cache client instance for the created cache.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {OperationError} if cache with the provided name already exists.
     * @throws {IgniteClientError} if other error.
     */
    async createCache(name, cacheConfig = null) {
        ArgumentChecker.notEmpty(name, 'name');
        ArgumentChecker.hasType(cacheConfig, 'cacheConfig', false, CacheConfiguration);

        await this._communicator.send(
            cacheConfig ?
                BinaryUtils.OPERATION.CACHE_CREATE_WITH_CONFIGURATION :
                BinaryUtils.OPERATION.CACHE_CREATE_WITH_NAME,
            async (payload) => {
                await this._writeCacheNameOrConfig(payload, name, cacheConfig);
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
     * @param {CacheConfiguration} [cacheConfig] - cache configuration (ignored if cache
     *   with the provided name already exists).
     *
     * @return {Promise<CacheClient>} - new cache client instance for the existing or created cache.
     *
     * @throws {IllegalStateError} if the client is not in CONNECTED {@link IgniteClient.STATE}.
     * @throws {IgniteClientError} if other error.
     */
    async getOrCreateCache(name, cacheConfig = null) {
        ArgumentChecker.notEmpty(name, 'name');
        ArgumentChecker.hasType(cacheConfig, 'cacheConfig', false, CacheConfiguration);
        await this._communicator.send(
            cacheConfig ?
                BinaryUtils.OPERATION.CACHE_GET_OR_CREATE_WITH_CONFIGURATION :
                BinaryUtils.OPERATION.CACHE_GET_OR_CREATE_WITH_NAME,
            async (payload) => {
                await this._writeCacheNameOrConfig(payload, name, cacheConfig);
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
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_DESTROY,
            async (payload) => {
                payload.writeInteger(CacheClient._calculateId(name));
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
    async getCacheConfiguration(name) {
        ArgumentChecker.notEmpty(name, 'name');
        let config;
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_GET_CONFIGURATION,
            async (payload) => {
                payload.writeInteger(CacheClient._calculateId(name));
                payload.writeByte(0);
            },
            async (payload) => {
                config = new CacheConfiguration();
                await config._read(this._communicator, payload);
            });
        return config;
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
    async cacheNames() {
        let names;
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_GET_NAMES,
            null,
            async (payload) => {
                names = await this._communicator.readStringArray(payload);
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
        return new CacheClient(name, cacheConfig, this._communicator);
    }

    /**
     * @ignore
     */
    async _writeCacheNameOrConfig(buffer, name, cacheConfig) {
        if (cacheConfig) {
            await cacheConfig._write(this._communicator, buffer, name);
        }
        else {
            BinaryCommunicator.writeString(buffer, name);
        }
    }
}

module.exports = IgniteClient;
