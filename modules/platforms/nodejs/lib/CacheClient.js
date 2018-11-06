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
const ArgumentChecker = require('./internal/ArgumentChecker');
const SqlQuery = require('./Query').SqlQuery;
const SqlFieldsQuery = require('./Query').SqlFieldsQuery;
const ScanQuery = require('./Query').ScanQuery;

/**
 * Peek modes
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
 * There are three groups of methods in the cache client:
 *   - methods to configure the cache client
 *   - methods to operate with the cache using Key-Value Queries
 *   - methods to operate with the cache using SQL and Scan Queries
 *
 * @hideconstructor
 */
class CacheClient {

    static get PEEK_MODE() {
        return PEEK_MODE;
    }

    /* Methods to configure the cache client */

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
        BinaryUtils.checkObjectType(type, 'type');
        this._keyType = type;
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
        BinaryUtils.checkObjectType(type, 'type');
        this._valueType = type;
        return this;
    }

    /* Methods to operate with the cache using Key-Value Queries */

    /**
     * Retrieves a value associated with the specified key from the cache.
     *
     * @async
     *
     * @param {*} key - key.
     *
     * @return {Promise<*>} - value associated with the specified key, or null if it does not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    async get(key) {
        return await this._writeKeyReadValueOp(BinaryUtils.OPERATION.CACHE_GET, key);
    }

    /**
     * Retrieves entries associated with the specified keys from the cache.
     *
     * @async
     *
     * @param {Array<*>} keys - keys.
     *
     * @return {Promise<Array<CacheEntry>>} - the retrieved entries (key-value pairs).
     *   Entries with the keys which do not exist in the cache are not included into the array.
     *
     * @throws {IgniteClientError} if error.
     */
    async getAll(keys) {
        ArgumentChecker.notEmpty(keys, 'keys');
        ArgumentChecker.hasType(keys, 'keys', false, Array);
        let result = null;
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_GET_ALL,
            async (payload) => {
                this._writeCacheInfo(payload);
                await this._writeKeys(payload, keys);
            },
            async (payload) => {
                const resultCount = payload.readInteger();
                result = new Array(resultCount);
                for (let i = 0; i < resultCount; i++) {
                    result[i] = new CacheEntry(
                        await this._communicator.readObject(payload, this._getKeyType()),
                        await this._communicator.readObject(payload, this._getValueType()));
                }
            });
        return result;
    }

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * Overwrites the previous value if the key exists in the cache,
     * otherwise creates new entry (key-value pair).
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @throws {IgniteClientError} if error.
     */
    async put(key, value) {
        await this._writeKeyValueOp(BinaryUtils.OPERATION.CACHE_PUT, key, value);
    }

    /**
     * Associates the specified values with the specified keys in the cache.
     *
     * Overwrites the previous value if a key exists in the cache,
     * otherwise creates new entry (key-value pair).
     *
     * @async
     *
     * @param {Array<CacheEntry>} entries - entries (key-value pairs) to be put into the cache.
     *
     * @throws {IgniteClientError} if error.
     */
    async putAll(entries) {
        ArgumentChecker.notEmpty(entries, 'entries');
        ArgumentChecker.hasType(entries, 'entries', true, CacheEntry);
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_PUT_ALL,
            async (payload) => {
                this._writeCacheInfo(payload);
                payload.writeInteger(entries.length);
                for (let entry of entries) {
                    await this._writeKeyValue(payload, entry.getKey(), entry.getValue());
                }
            });
    }

    /**
     * Checks if the specified key exists in the cache.
     *
     * @async
     *
     * @param {*} key - key to check.
     *
     * @return {Promise<boolean>} - true if the key exists, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    async containsKey(key) {
        return await this._writeKeyReadBooleanOp(BinaryUtils.OPERATION.CACHE_CONTAINS_KEY, key);
    }

    /**
     * Checks if all the specified keys exist in the cache.
     *
     * @async
     *
     * @param {Array<*>} keys - keys to check.
     *
     * @return {Promise<boolean>} - true if all the keys exist,
     *   false if at least one of the keys does not exist in the cache.
     *
     * @throws {IgniteClientError} if error.
     */
    async containsKeys(keys) {
        return await this._writeKeysReadBooleanOp(BinaryUtils.OPERATION.CACHE_CONTAINS_KEYS, keys);
    }

    /**
     * Associates the specified value with the specified key in the cache
     * and returns the previous associated value, if any.
     *
     * Overwrites the previous value if the key exists in the cache,
     * otherwise creates new entry (key-value pair).
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @return {Promise<*>} - the previous value associated with the specified key, or null if it did not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndPut(key, value) {
        return await this._writeKeyValueReadValueOp(BinaryUtils.OPERATION.CACHE_GET_AND_PUT, key, value);
    }

    /**
     * Associates the specified value with the specified key in the cache
     * and returns the previous associated value, if the key exists in the cache.
     * Otherwise does nothing and returns null.
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @return {Promise<*>} - the previous value associated with the specified key, or null if it did not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndReplace(key, value) {
        return await this._writeKeyValueReadValueOp(BinaryUtils.OPERATION.CACHE_GET_AND_REPLACE, key, value);
    }

    /**
     * Removes the cache entry with the specified key
     * and returns the last associated value, if any.
     *
     * @async
     *
     * @param {*} key - key of the entry to be removed.
     *
     * @return {Promise<*>} - the last value associated with the specified key, or null if it did not exist.
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndRemove(key) {
        return await this._writeKeyReadValueOp(BinaryUtils.OPERATION.CACHE_GET_AND_REMOVE, key);
    }

    /**
     * Creates new entry (key-value pair) if the specified key does not exist in the cache.
     * Otherwise does nothing.
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @return {Promise<boolean>} - true if the operation has been done, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    async putIfAbsent(key, value) {
        return await this._writeKeyValueReadBooleanOp(BinaryUtils.OPERATION.CACHE_PUT_IF_ABSENT, key, value);
    }

    /**
     * Creates new entry (key-value pair) if the specified key does not exist in the cache.
     * Otherwise returns the current value associated with the existing key.
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @return {Promise<*>} - the current value associated with the key if it already exists in the cache,
     *   null if the new entry is created.
     *
     * @throws {IgniteClientError} if error.
     */
    async getAndPutIfAbsent(key, value) {
        return await this._writeKeyValueReadValueOp(BinaryUtils.OPERATION.CACHE_GET_AND_PUT_IF_ABSENT, key, value);
    }

    /**
     * Associates the specified value with the specified key, if the key exists in the cache.
     * Otherwise does nothing.
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be associated with the specified key.
     *
     * @return {Promise<boolean>} - true if the operation has been done, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    async replace(key, value) {
        return await this._writeKeyValueReadBooleanOp(BinaryUtils.OPERATION.CACHE_REPLACE, key, value);
    }

    /**
     * Associates the new value with the specified key, if the key exists in the cache
     * and the current value equals to the provided one.
     * Otherwise does nothing.
     *
     * @async
     *
     * @param {*} key - key.
     * @param {*} value - value to be compared with the current value associated with the specified key.
     * @param {*} newValue - new value to be associated with the specified key.
     *
     * @return {Promise<boolean>} - true if the operation has been done, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    async replaceIfEquals(key, value, newValue) {
        ArgumentChecker.notNull(key, 'key');
        ArgumentChecker.notNull(value, 'value');
        ArgumentChecker.notNull(newValue, 'newValue');
        let result;
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_REPLACE_IF_EQUALS,
            async (payload) => {
                this._writeCacheInfo(payload);
                await this._writeKeyValue(payload, key, value);
                await this._communicator.writeObject(payload, newValue, this._getValueType());
            },
            async (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }

    /**
     * Removes all entries from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    async clear() {
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_CLEAR,
            async (payload) => {
                this._writeCacheInfo(payload);
            });
    }

    /**
     * Removes entry with the specified key from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @param {*} key - key to be removed.
     *
     * @throws {IgniteClientError} if error.
     */
    async clearKey(key) {
        await this._writeKeyOp(BinaryUtils.OPERATION.CACHE_CLEAR_KEY, key);
    }

    /**
     * Removes entries with the specified keys from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @param {Array<*>} keys - keys to be removed.
     *
     * @throws {IgniteClientError} if error.
     */
    async clearKeys(keys) {
        await this._writeKeysOp(BinaryUtils.OPERATION.CACHE_CLEAR_KEYS, keys);
    }

    /**
     * Removes entry with the specified key from the cache, notifying listeners and cache writers.
     *
     * @async
     *
     * @param {*} key - key to be removed.
     *
     * @return {Promise<boolean>} - true if the operation has been done, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    async removeKey(key) {
        return await this._writeKeyReadBooleanOp(BinaryUtils.OPERATION.CACHE_REMOVE_KEY, key);
    }

    /**
     * Removes entry with the specified key from the cache, if the current value equals to the provided one.
     * Notifies listeners and cache writers.
     *
     * @async
     *
     * @param {*} key - key to be removed.
     * @param {*} value - value to be compared with the current value associated with the specified key.
     *
     * @return {Promise<boolean>} - true if the operation has been done, false otherwise.
     *
     * @throws {IgniteClientError} if error.
     */
    async removeIfEquals(key, value) {
        return await this._writeKeyValueReadBooleanOp(BinaryUtils.OPERATION.CACHE_REMOVE_IF_EQUALS, key, value);
    }

    /**
     * Removes entries with the specified keys from the cache, notifying listeners and cache writers.
     *
     * @async
     *
     * @param {Array<*>} keys - keys to be removed.
     *
     * @throws {IgniteClientError} if error.
     */
    async removeKeys(keys) {
        await this._writeKeysOp(BinaryUtils.OPERATION.CACHE_REMOVE_KEYS, keys);
    }

    /**
     * Removes all entries from the cache, notifying listeners and cache writers.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    async removeAll() {
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_REMOVE_ALL,
            async (payload) => {
                this._writeCacheInfo(payload);
            });
    }

    /**
     * Returns the number of the entries in the cache.
     *
     * @async
     *
     * @param {...CacheClient.PEEK_MODE} [peekModes] - peek modes.
     *
     * @return {Promise<number>} - the number of the entries in the cache.
     *
     * @throws {IgniteClientError} if error.
     */
    async getSize(...peekModes) {
        ArgumentChecker.hasValueFrom(peekModes, 'peekModes', true, CacheClient.PEEK_MODE);
        let result;
        await this._communicator.send(
            BinaryUtils.OPERATION.CACHE_GET_SIZE,
            async (payload) => {
                this._writeCacheInfo(payload);
                payload.writeInteger(peekModes.length);
                for (let mode of peekModes) {
                    payload.writeByte(mode);
                }
            },
            async (payload) => {
                result = payload.readLong().toNumber();
            });
        return result;
    }

    /* Methods to operate with the cache using SQL and Scan Queries */

    /**
     * Starts an SQL or Scan query operation.
     *
     * @async
     *
     * @param {SqlQuery | SqlFieldsQuery | ScanQuery} query - query to be executed.
     *
     * @return {Promise<Cursor>} - cursor to obtain the results of the query operation:
     *   - {@link SqlFieldsCursor} in case of {@link SqlFieldsQuery} query
     *   - {@link Cursor} in case of other types of query
     *
     * @throws {IgniteClientError} if error.
     */
    async query(query) {
        ArgumentChecker.notNull(query, 'query');
        ArgumentChecker.hasType(query, 'query', false, SqlQuery, SqlFieldsQuery, ScanQuery);

        let value = null;
        await this._communicator.send(
            query._operation,
            async (payload) => {
                this._writeCacheInfo(payload);
                await query._write(this._communicator, payload);
            },
            async (payload) => {
                value = await query._getCursor(this._communicator, payload, this._keyType, this._valueType);
            });
        return value;
    }

    /** Private methods */

    /**
     * @ignore
     */
    constructor(name, config, communicator) {
        this._name = name;
        this._cacheId = CacheClient._calculateId(this._name);
        this._config = config;
        this._keyType = null;
        this._valueType = null;
        this._communicator = communicator;
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
        payload.writeByte(0);
    }

    /**
     * @ignore
     */
    async _writeKeyValue(payload, key, value) {
        await this._communicator.writeObject(payload, key, this._getKeyType());
        await this._communicator.writeObject(payload, value, this._getValueType());
    }

    /**
     * @ignore
     */
    async _writeKeys(payload, keys) {
        payload.writeInteger(keys.length);
        for (let key of keys) {
            await this._communicator.writeObject(payload, key, this._getKeyType());
        }
    }

    /**
     * @ignore
     */
    _getKeyType() {
        return this._keyType;
    }

    /**
     * @ignore
     */
    _getValueType() {
        return this._valueType;
    }

    /**
     * @ignore
     */
    async _writeKeyValueOp(operation, key, value, payloadReader = null) {
        ArgumentChecker.notNull(key, 'key');
        ArgumentChecker.notNull(value, 'value');
        await this._communicator.send(
            operation,
            async (payload) => {
                this._writeCacheInfo(payload);
                await this._writeKeyValue(payload, key, value);
            },
            payloadReader);
    }

    /**
     * @ignore
     */
    async _writeKeyValueReadValueOp(operation, key, value) {
        let result = null;
        await this._writeKeyValueOp(
            operation, key, value,
            async (payload) => {
                result = await this._communicator.readObject(payload, this._getValueType());
            });
        return result;
    }

    /**
     * @ignore
     */
    async _writeKeyValueReadBooleanOp(operation, key, value) {
        let result = false;
        await this._writeKeyValueOp(
            operation, key, value,
            async (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }

    /**
     * @ignore
     */
    async _writeKeyOp(operation, key, payloadReader = null) {
        ArgumentChecker.notNull(key, 'key');
        await this._communicator.send(
            operation,
            async (payload) => {
                this._writeCacheInfo(payload);
                await this._communicator.writeObject(payload, key, this._getKeyType());
            },
            payloadReader);
    }

    /**
     * @ignore
     */
    async _writeKeyReadValueOp(operation, key) {
        let value = null;
        await this._writeKeyOp(
            operation, key,
            async (payload) => {
                value = await this._communicator.readObject(payload, this._getValueType());
            });
        return value;
    }

    /**
     * @ignore
     */
    async _writeKeyReadBooleanOp(operation, key) {
        let result = false;
        await this._writeKeyOp(
            operation, key,
            async (payload) => {
                result = payload.readBoolean();
            });
        return result;
    }

    /**
     * @ignore
     */
    async _writeKeysOp(operation, keys, payloadReader = null) {
        ArgumentChecker.notEmpty(keys, 'keys');
        ArgumentChecker.hasType(keys, 'keys', false, Array);
        await this._communicator.send(
            operation,
            async (payload) => {
                this._writeCacheInfo(payload);
                await this._writeKeys(payload, keys);
            },
            payloadReader);
    }

    /**
     * @ignore
     */
    async _writeKeysReadBooleanOp(operation, keys) {
        let result = false;
        await this._writeKeysOp(
            operation, keys,
            async (payload) => {
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
     * Public constructor.
     *
     * @param {*} key - key corresponding to this entry.
     * @param {*} value - value associated with the key.
     *
     * @return {CacheEntry} - new CacheEntry instance     
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
