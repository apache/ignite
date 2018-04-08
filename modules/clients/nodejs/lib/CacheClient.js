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
const SqlQuery = require('./Query').SqlQuery;
const SqlFieldsQuery = require('./Query').SqlFieldsQuery;

/**
 * ???
 * @typedef CacheClient.PEEK_MODE
 * @enum
 * @readonly
 * @property ALL 0
 * @property NEAR 1
 * @property PRIMARY 2
 * @property BACKUP 3
 */
const PEEK_MODE = Object.freeze({
    ALL : 0,
    NEAR : 1,
    PRIMARY : 2,
    BACKUP : 3
});

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

    static get PEEK_MODE() {
        return PEEK_MODE;
    }

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
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} type - type of the keys in the cache:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (means the type is not specified).
     *
     * @return {CacheClient} - the same instance of the cache client.
     *
     * @throws {IgniteClientError} if error.
     */
    setKeyType(type) {
        this._keyType = BinaryUtils.getObjectType(type, 'type');
        this._binaryModeKeyType = new BinaryUtils.BinaryObjectType(this._keyType);
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
     * @param {ObjectType.PRIMITIVE_TYPE | CompositeType} type - type of the values in the cache:
     *   - either a type code of primitive (simple) type
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (means the type is not specified).
     *
     * @return {CacheClient} - the same instance of the cache client.
     *
     * @throws {IgniteClientError} if error.
     */
    setValueType(type) {
        this._valueType = BinaryUtils.getObjectType(type, 'type');
        this._binaryModeValueType = new BinaryUtils.BinaryObjectType(this._valueType);
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
        return await this._putKeyGetValue(BinaryUtils.OPERATION.CACHE_GET, key);
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
        await this._putKeyValue(BinaryUtils.OPERATION.CACHE_PUT, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {Array<*>} keys - the cache keys.
     *
     * @return {Promise<Array<CacheEntry>>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async getAll(keys) {
        ArgumentChecker.notEmpty(keys, 'keys');
        let result = null;
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_GET_ALL,
            (payload) => {
                this._writeCacheInfo(payload);
                this._writeKeys(payload, keys);
            },
            (payload) => {
                const resultCount = payload.readInteger();
                result = new Array(resultCount);
                for (let i = 0; i < resultCount; i++) {
                    result[i] = new CacheEntry(
                        BinaryReader.readObject(payload, this._getKeyType()),
                        BinaryReader.readObject(payload, this._getValueType()));
                }
            });
        return result;
    }

    /**
     * ???
     *
     * @async
     *
     * @param {Array<CacheEntry>} entries - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async putAll(entries) {
        ArgumentChecker.notEmpty(entries, 'entries');
        ArgumentChecker.hasType(entries, 'entries', true, CacheEntry);
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_PUT_ALL,
            (payload) => {
                this._writeCacheInfo(payload);
                payload.writeInteger(entries.length);
                for (let entry of entries) {
                    this._writeKeyValue(payload, entry.getKey(), entry.getValue());
                }
            });
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async containsKey(key) {
        return await this._putKeyGetBoolean(BinaryUtils.OPERATION.CACHE_CONTAINS_KEY, key);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {Array<*>} keys - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async containsKeys(keys) {
        return await this._putKeysGetBoolean(BinaryUtils.OPERATION.CACHE_CONTAINS_KEYS, key);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     *
     * @return {Promise<*>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndPut(key, value) {
        return await this._putKeyValueGetValue(BinaryUtils.OPERATION.CACHE_GET_AND_PUT, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     *
     * @return {Promise<*>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndReplace(key, value) {
        return await this._putKeyValueGetValue(BinaryUtils.OPERATION.CACHE_GET_AND_REPLACE, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     *
     * @return {Promise<*>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndRemove(key) {
        return await this._putKeyGetValue(BinaryUtils.OPERATION.CACHE_GET_AND_REMOVE, key);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async putIfAbsent(key, value) {
        return await this._putKeyValueGetBoolean(BinaryUtils.OPERATION.CACHE_PUT_IF_ABSENT, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     *
     * @return {Promise<*>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndPutIfAbsent(key, value) {
        return await this._putKeyValueGetValue(BinaryUtils.OPERATION.CACHE_GET_AND_PUT_IF_ABSENT, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async replace(key, value) {
        return await this._putKeyValueGetBoolean(BinaryUtils.OPERATION.CACHE_REPLACE, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     * @param {*} newValue - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async replaceIfEquals(key, value, newValue) {
        ArgumentChecker.notNull(key, 'key');
        ArgumentChecker.notNull(value, 'value');
        ArgumentChecker.notNull(newValue, 'newValue');
        let result;
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_REPLACE_IF_EQUALS,
            (payload) => {
                this._writeCacheInfo(payload);
                this._writeKeyValue(payload, key, value);
                BinaryWriter.writeObject(payload, newValue, this._getValueType());
            },
            (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }

    /**
     * ???
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    async clear() {
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_CLEAR,
            (payload) => {
                this._writeCacheInfo(payload);
            });
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async clearKey(key) {
        await this._putKey(BinaryUtils.OPERATION.CACHE_CLEAR_KEY, key);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {Array<*>} keys - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async clearKeys(keys) {
        await this._putKeys(BinaryUtils.OPERATION.CACHE_CLEAR_KEYS, keys);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async removeKey(key) {
        return await this._putKeyGetBoolean(BinaryUtils.OPERATION.CACHE_REMOVE_KEY, key);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {*} key - ???
     * @param {*} value - ???
     *
     * @return {Promise<boolean>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async removeIfEquals(key, value) {
        return await this._putKeyValueGetBoolean(BinaryUtils.OPERATION.CACHE_REMOVE_IF_EQUALS, key, value);
    }

    /**
     * ???
     *
     * @async
     *
     * @param {Array<*>} keys - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async removeKeys(keys) {
        await this._putKeys(BinaryUtils.OPERATION.CACHE_REMOVE_KEYS, keys);
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

    /**
     * ???
     *
     * @async
     *
     * @param {...CacheClient.PEEK_MODE} [peekModes] - ???
     *
     * @return {Promise<number>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async getSize(...peekModes) {
        ArgumentChecker.hasValueFrom(peekModes, 'peekModes', true, CacheClient.PEEK_MODE);
        let result;
        await this._socket.send(
            BinaryUtils.OPERATION.CACHE_GET_SIZE,
            (payload) => {
                this._writeCacheInfo(payload);
                payload.writeInteger(peekModes.length);
                for (let mode of peekModes) {
                    payload.writeByte(mode);
                }
            },
            (payload) => {
                result = payload.readLong().toNumber();
            });
        return result;
    }

    /**
     * ???
     *
     * @async
     *
     * @param {SqlQuery | SqlFieldsQuery} query - ???
     *
     * @return {Promise<Cursor>} - ???
     *
     * @throws {IgniteClientError} if error.
     */
    async query(query) {
        ArgumentChecker.notNull(query, 'query');
        ArgumentChecker.hasType(query, 'query', false, SqlQuery, SqlFieldsQuery);

        let value = null;
        await this._socket.send(
            query._operation,
            (payload) => {
                this._writeCacheInfo(payload);
                query._write(payload);
            },
            (payload) => {
                value = query._getCursor(this._socket, payload, this._keyType, this._valueType);
            });
        return value;
    }

    /** Private methods */

    /**
     * @ignore
     */
    constructor(name, config, socket) {
        this._name = name;
        this._cacheId = CacheClient._calculateId(this._name);
        this._config = config;
        this._keyType = null;
        this._valueType = null;
        this._binaryModeKeyType = new BinaryUtils.BinaryObjectType();
        this._binaryModeValueType = new BinaryUtils.BinaryObjectType();
        this._binaryMode = false;
        this._socket = socket;
    }

    /**
     * @ignore
     */
    static _calculateId(name) {
        return BinaryUtils.hashCode(name);
    }

    /**
     * @ignore
     */
    _writeCacheInfo(payload) {
        payload.writeInteger(this._cacheId);
        payload.writeByte(this._binaryMode ? 1 : 0);
    }

    /**
     * @ignore
     */
    _writeKeyValue(payload, key, value) {
        BinaryWriter.writeObject(payload, key, this._getKeyType());
        BinaryWriter.writeObject(payload, value, this._getValueType());
    }

    /**
     * @ignore
     */
    _writeKeys(payload, keys) {
        payload.writeInteger(keys.length);
        for (let key of keys) {
            BinaryWriter.writeObject(payload, key, this._getKeyType());
        }
    }

    /**
     * @ignore
     */
    _getKeyType() {
        return this._binaryMode ? this._binaryModeKeyType : this._keyType;
    }

    /**
     * @ignore
     */
    _getValueType() {
        return this._binaryMode ? this._binaryModeValueType : this._valueType;
    }

    /**
     * @ignore
     */
    async _putKeyValue(operation, key, value, payloadReader = null) {
        ArgumentChecker.notNull(key, 'key');
        ArgumentChecker.notNull(value, 'value');
        await this._socket.send(
            operation,
            (payload) => {
                this._writeCacheInfo(payload);
                this._writeKeyValue(payload, key, value);
            },
            payloadReader);
    }

    /**
     * @ignore
     */
    async _putKeyValueGetValue(operation, key, value) {
        let value = null;
        await this._putKeyValue(
            operation, key, value,
            (payload) => {
                value = BinaryReader.readObject(payload, this._getValueType());
            });
        return value;
    }

    /**
     * @ignore
     */
    async _putKeyValueGetBoolean(operation, key, value) {
        let result;
        await this._putKeyValue(
            operation, key, value,
            (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }

    /**
     * @ignore
     */
    async _putKey(operation, key, payloadReader = null) {
        ArgumentChecker.notNull(key, 'key');
        await this._socket.send(
            operation,
            (payload) => {
                this._writeCacheInfo(payload);
                BinaryWriter.writeObject(payload, key, this._getKeyType());
            },
            payloadReader);
    }

    /**
     * @ignore
     */
    async _putKeyGetValue(operation, key) {
        let value = null;
        await this._putKey(
            operation, key,
            (payload) => {
                value = BinaryReader.readObject(payload, this._getValueType());
            });
        return value;
    }

    /**
     * @ignore
     */
    async _putKeyGetBoolean(operation, key) {
        let result;
        await this._putKey(
            operation, key,
            (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }

    /**
     * @ignore
     */
    async _putKeys(operation, keys, payloadReader = null) {
        ArgumentChecker.notEmpty(keys, 'keys');
        await this._socket.send(
            operation,
            (payload) => {
                this._writeCacheInfo(payload);
                this._writeKeys(payload, keys);
            },
            payloadReader);
    }

    /**
     * @ignore
     */
    async _putKeysGetBoolean(operation, keys) {
        let result;
        await this._putKeys(
            operation, keys,
            (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }
}

/**
 * A cache entry (key-value pair).
 */
class CacheEntry {

    /**
     * ???
     */
    constructor(key, value) {
        this._key = key;
        this._value = value;
    }

    /**
     * Returns the key corresponding to this entry.
     *
     * @return {*} - the key corresponding to this entry.
     */
    getKey() {
        return this._key;
    }

    /**
     * Returns the value corresponding to this entry.
     *
     * @return {*} - the value corresponding to this entry.
     */
    getValue() {
        return this._value;
    }
}

module.exports = CacheClient;
module.exports.CacheEntry = CacheEntry;
