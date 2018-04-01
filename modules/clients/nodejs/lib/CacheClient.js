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

const BinaryUtils = require('./internal/BinaryUtils');
const BinaryReader = require('./internal/BinaryReader');
const BinaryWriter = require('./internal/BinaryWriter');
const ArgumentChecker = require('./internal/ArgumentChecker');

/**
 * Class representing and providing access to Ignite cache.
 *
 * The class has no public constructor. An instance of this class should be obtained
 * via the methods of {@link IgniteClient} objects.
 * One instance of this class provides access to one Ignite cache which is specified
 * during the instance obtaining and cannot be changed after that.
 *
 * There are two groups of methods in the cache client:
 *
 *   - methods to configure a cache client
 *   - methods to operate with the cache itself
 *
 * The cache client can operate in two modes - "non-binary" (default) and "binary" - TBD.
 *
 * @hideconstructor
 */
class CacheClient {
    /* Methods to configure a cache client */

    /**
     * Specifies a type of the cache key.
     *
     * The cache client assumes that keys in all further operations with the cache
     * will have the specified type.
     * Eg. the cache client will convert keys provided as input parameters of the methods
     * to the specified object type before sending them to a server.
     *
     * After the cache client creation a type of the cache key is not specified (null).
     *
     * If the type is not specified then during operations the cache client
     * will do automatic mapping between some of the JavaScript types and object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {ObjectType | integer} type - type of the keys in the cache:
     *   - either an instance of object type
     *   - or a type code (means object type with this type code and with default subtype, if applicable)
     *   - or null (means the type is not specified).
     *
     * @return {CacheClient} - the same instance of the cache client.
     *
     * @throws {IgniteClientError} if error.
     */
    setKeyType(type) {
        this._keyType = BinaryUtils.getObjectType(type, 'type');
        return this;
    }

    /**
     * Specifies a type of the cache value.
     *
     * The cache client assumes that values in all further operations with the cache
     * will have the specified type.
     * Eg. the cache client will convert values provided as input parameters of the methods
     * to the specified object type before sending them to a server.
     *
     * After the cache client creation a type of the cache value is not specified (null).
     *
     * If the type is not specified then during operations the cache client
     * will do automatic mapping between some of the JavaScript types and object types -
     * according to the mapping table defined in the description of the {@link ObjectType} class.
     *
     * @param {ObjectType | integer} type - type of the values in the cache:
     *   - either an instance of object type
     *   - or a type code (means object type with this type code and with default subtype, if applicable)
     *   - or null (means the type is not specified).
     *
     * @return {CacheClient} - the same instance of the cache client.
     *
     * @throws {IgniteClientError} if error.
     */
    setValueType(type) {
        this._valueType = BinaryUtils.getObjectType(type, 'type');
        return this;
    }

    /**
     * Switches between "non-binary" and "binary" modes of the cache client.
     *
     * Does nothing if already in the requested mode.
     *
     * TBD
     *
     * @param {boolean} [on=true] - If true or not specified: switch on "binary" mode.
     *   If false: switch off "binary" mode.
     *
     * @return {CacheClient} - the same instance of the cache client.
     *
     * @throws {IgniteClientError} if other error. TBD
     */
    setBinaryMode(on = true) {
        this._binaryMode = on;
        return this;
    }

    /* Methods to operate with the cache */

    /**
     * Retrieves a value mapped to the specified key from the cache.
     *
     * @async
     *
     * @param {*} key - the cache key.
     *
     * @return {Promise<*>} - the value associated with the specified key, or null, if it does not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    async get(key) {
        ArgumentChecker.notNull(key, "key");
        let value = null;
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_GET,
            (payload) => {
                this._writeCacheInfo(payload);
                BinaryWriter.writeObject(payload, key, this._keyType);
            },
            (payload) => {
                value = BinaryReader.readObject(payload, this._valueType);
            });
        return value;
    }

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * Overwrites the existing value, if any.
     *
     * @async
     *
     * @param {*} key - the cache key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @throws {IgniteClientError} if error.
     */
    async put(key, value) {
        ArgumentChecker.notNull(key, "key");
        ArgumentChecker.notNull(value, "value");
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_PUT,
            (payload) => {
                this._writeCacheInfo(payload);
                BinaryWriter.writeObject(payload, key, this._keyType);
                BinaryWriter.writeObject(payload, value, this._valueType);
            });
    }

    /**
     * Removes all the entities (keys and values) from the cache.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    async removeAll() {
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_REMOVE_ALL,
            (payload) => {
                this._writeCacheInfo(payload);
            });
    }

    /** Private methods */

    /**
     * @ignore
     */
    constructor(name, config, socket) {
        this._name = name;
        this._cacheId = BinaryUtils.hashCode(this._name);
        this._config = config;
        this._keyType = null;
        this._valueType = null;
        this._binaryMode = false;
        this._socket = socket;
    }

    /**
     * @ignore
     */
    _writeCacheInfo(payload) {
        payload.writeInteger(this._cacheId);
        payload.writeByte(this._binaryMode ? 1 : 0);
    }
}

module.exports = CacheClient;
