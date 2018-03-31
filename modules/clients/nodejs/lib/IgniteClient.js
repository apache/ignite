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
const ClientFailoverSocket = require('./internal/ClientFailoverSocket');
const BinaryUtils = require('./internal/BinaryUtils');
const BinaryWriter = require('./internal/BinaryWriter');
const BinaryReader = require('./internal/BinaryReader');
const ArgumentChecker = require('./internal/ArgumentChecker');
const Logger = require('./internal/Logger');

/**
 * Class representing Ignite client.
 * 
 * The client can be in one of the two states - "disconnected" or "connected".
 *
 * "Disconnected" state: initial state after a client instance creation,
 * connect() method is allowed only.
 *
 * "Connected" state: all operations are allowed except connect() method.
 *
 * The client goes to the "disconnected" state when it cannot establish a connection
 * to no one of the server nodes specified in the {@link IgniteClientConfiguration}
 * or when disconnect() method is called.
 *
 * When the client goes to the "disconnected" state the callback
 * specified in the connect() method is called.
 */
class IgniteClient {

    /**
     * The default constructor.
     *
     * @return {IgniteClient} - new IgniteClient instance.
     */
    constructor() {
        this._socket = new ClientFailoverSocket();
    }

    /**
     * IgniteClient.connect() method callback.
     * @callback IgniteClient.onDisconnect
     * @param {IgniteClientError} error - the reason of disconnection,
     * null when disconnected by disconnect() method.
     */

    /**
     * Connects the client.
     *
     * @async
     *
     * @param {IgniteClientConfiguration} config - the client configuration.
     * @param {IgniteClient.onDisconnect} onDisconnect - the callback called when the client is disconnected.
     *
     * @throws {IllegalStateError} if the client already connected.
     * @throws {IgniteClientError} if other error.
     */
    async connect(config, onDisconnect) {
        ArgumentChecker.notEmpty(config, 'config');
        ArgumentChecker.hasType(config, 'config', IgniteClientConfiguration);
        await this._socket.connect(config, onDisconnect);
    }

    /**
     * Disconnects the client.
     *
     * Triggers the callback specified in the connect() method.
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
     * @throws {IllegalStateError} if the client is disconnected.
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
     * @throws {IllegalStateError} if the client is disconnected.
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
     * @throws {IllegalStateError} if the client is disconnected.
     * @throws {OperationError} if cache with the provided name does not exist.
     * @throws {IgniteClientError} if other error.
     */
    async destroyCache(name) {
        ArgumentChecker.notEmpty(name, 'name');
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_DESTROY,
            (payload) => {
                payload.writeInteger(BinaryUtils.hashCode(name));
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
     * @throws {IllegalStateError} if the client is disconnected.
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
