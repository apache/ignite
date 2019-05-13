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
const internal_1 = require("./internal");
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
    ALL: 0,
    NEAR: 1,
    PRIMARY: 2,
    BACKUP: 3
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
        internal_1.BinaryUtils.checkObjectType(type, 'type');
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
        internal_1.BinaryUtils.checkObjectType(type, 'type');
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
    get(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyReadValueOp(internal_1.BinaryUtils.OPERATION.CACHE_GET, key);
        });
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
    getAll(keys) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(keys, 'keys');
            internal_1.ArgumentChecker.hasType(keys, 'keys', false, Array);
            let result = null;
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_GET_ALL, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                yield this._writeKeys(payload, keys);
            }), (payload) => __awaiter(this, void 0, void 0, function* () {
                const resultCount = payload.readInteger();
                result = new Array(resultCount);
                for (let i = 0; i < resultCount; i++) {
                    result[i] = new CacheEntry(yield this._communicator.readObject(payload, this._getKeyType()), yield this._communicator.readObject(payload, this._getValueType()));
                }
            }));
            return result;
        });
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
    put(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._writeKeyValueOp(internal_1.BinaryUtils.OPERATION.CACHE_PUT, key, value);
        });
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
    putAll(entries) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(entries, 'entries');
            internal_1.ArgumentChecker.hasType(entries, 'entries', true, CacheEntry);
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_PUT_ALL, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                payload.writeInteger(entries.length);
                for (let entry of entries) {
                    yield this._writeKeyValue(payload, entry.getKey(), entry.getValue());
                }
            }));
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
    containsKey(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyReadBooleanOp(internal_1.BinaryUtils.OPERATION.CACHE_CONTAINS_KEY, key);
        });
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
    containsKeys(keys) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeysReadBooleanOp(internal_1.BinaryUtils.OPERATION.CACHE_CONTAINS_KEYS, keys);
        });
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
    getAndPut(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyValueReadValueOp(internal_1.BinaryUtils.OPERATION.CACHE_GET_AND_PUT, key, value);
        });
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
    getAndReplace(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyValueReadValueOp(internal_1.BinaryUtils.OPERATION.CACHE_GET_AND_REPLACE, key, value);
        });
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
    getAndRemove(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyReadValueOp(internal_1.BinaryUtils.OPERATION.CACHE_GET_AND_REMOVE, key);
        });
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
    putIfAbsent(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyValueReadBooleanOp(internal_1.BinaryUtils.OPERATION.CACHE_PUT_IF_ABSENT, key, value);
        });
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
    getAndPutIfAbsent(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyValueReadValueOp(internal_1.BinaryUtils.OPERATION.CACHE_GET_AND_PUT_IF_ABSENT, key, value);
        });
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
    replace(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyValueReadBooleanOp(internal_1.BinaryUtils.OPERATION.CACHE_REPLACE, key, value);
        });
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
    replaceIfEquals(key, value, newValue) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notNull(key, 'key');
            internal_1.ArgumentChecker.notNull(value, 'value');
            internal_1.ArgumentChecker.notNull(newValue, 'newValue');
            let result;
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_REPLACE_IF_EQUALS, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                yield this._writeKeyValue(payload, key, value);
                yield this._communicator.writeObject(payload, newValue, this._getValueType());
            }), (payload) => __awaiter(this, void 0, void 0, function* () {
                result = payload.readBoolean();
            }));
            return result;
        });
    }
    /**
     * Removes all entries from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    clear() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_CLEAR, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
            }));
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
    clearKey(key) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._writeKeyOp(internal_1.BinaryUtils.OPERATION.CACHE_CLEAR_KEY, key);
        });
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
    clearKeys(keys) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._writeKeysOp(internal_1.BinaryUtils.OPERATION.CACHE_CLEAR_KEYS, keys);
        });
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
    removeKey(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyReadBooleanOp(internal_1.BinaryUtils.OPERATION.CACHE_REMOVE_KEY, key);
        });
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
    removeIfEquals(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            return yield this._writeKeyValueReadBooleanOp(internal_1.BinaryUtils.OPERATION.CACHE_REMOVE_IF_EQUALS, key, value);
        });
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
    removeKeys(keys) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._writeKeysOp(internal_1.BinaryUtils.OPERATION.CACHE_REMOVE_KEYS, keys);
        });
    }
    /**
     * Removes all entries from the cache, notifying listeners and cache writers.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    removeAll() {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_REMOVE_ALL, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
            }));
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
    getSize(...peekModes) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.hasValueFrom(peekModes, 'peekModes', true, CacheClient.PEEK_MODE);
            let result;
            yield this._communicator.send(internal_1.BinaryUtils.OPERATION.CACHE_GET_SIZE, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                payload.writeInteger(peekModes.length);
                for (let mode of peekModes) {
                    payload.writeByte(mode);
                }
            }), (payload) => __awaiter(this, void 0, void 0, function* () {
                result = payload.readLong().toNumber();
            }));
            return result;
        });
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
    query(query) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notNull(query, 'query');
            internal_1.ArgumentChecker.hasType(query, 'query', false, internal_1.SqlQuery, internal_1.SqlFieldsQuery, internal_1.ScanQuery);
            let value = null;
            yield this._communicator.send(query._operation, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                yield query._write(this._communicator, payload);
            }), (payload) => __awaiter(this, void 0, void 0, function* () {
                value = yield query._getCursor(this._communicator, payload, this._keyType, this._valueType);
            }));
            return value;
        });
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
        return internal_1.BinaryUtils.hashCode(name);
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
    _writeKeyValue(payload, key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this._communicator.writeObject(payload, key, this._getKeyType());
            yield this._communicator.writeObject(payload, value, this._getValueType());
        });
    }
    /**
     * @ignore
     */
    _writeKeys(payload, keys) {
        return __awaiter(this, void 0, void 0, function* () {
            payload.writeInteger(keys.length);
            for (let key of keys) {
                yield this._communicator.writeObject(payload, key, this._getKeyType());
            }
        });
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
    _writeKeyValueOp(operation, key, value, payloadReader = null) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notNull(key, 'key');
            internal_1.ArgumentChecker.notNull(value, 'value');
            yield this._communicator.send(operation, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                yield this._writeKeyValue(payload, key, value);
            }), payloadReader);
        });
    }
    /**
     * @ignore
     */
    _writeKeyValueReadValueOp(operation, key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            let result = null;
            yield this._writeKeyValueOp(operation, key, value, (payload) => __awaiter(this, void 0, void 0, function* () {
                result = yield this._communicator.readObject(payload, this._getValueType());
            }));
            return result;
        });
    }
    /**
     * @ignore
     */
    _writeKeyValueReadBooleanOp(operation, key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            let result = false;
            yield this._writeKeyValueOp(operation, key, value, (payload) => __awaiter(this, void 0, void 0, function* () {
                result = payload.readBoolean();
            }));
            return result;
        });
    }
    /**
     * @ignore
     */
    _writeKeyOp(operation, key, payloadReader = null) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notNull(key, 'key');
            yield this._communicator.send(operation, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                yield this._communicator.writeObject(payload, key, this._getKeyType());
            }), payloadReader);
        });
    }
    /**
     * @ignore
     */
    _writeKeyReadValueOp(operation, key) {
        return __awaiter(this, void 0, void 0, function* () {
            let value = null;
            yield this._writeKeyOp(operation, key, (payload) => __awaiter(this, void 0, void 0, function* () {
                value = yield this._communicator.readObject(payload, this._getValueType());
            }));
            return value;
        });
    }
    /**
     * @ignore
     */
    _writeKeyReadBooleanOp(operation, key) {
        return __awaiter(this, void 0, void 0, function* () {
            let result = false;
            yield this._writeKeyOp(operation, key, (payload) => __awaiter(this, void 0, void 0, function* () {
                result = payload.readBoolean();
            }));
            return result;
        });
    }
    /**
     * @ignore
     */
    _writeKeysOp(operation, keys, payloadReader = null) {
        return __awaiter(this, void 0, void 0, function* () {
            internal_1.ArgumentChecker.notEmpty(keys, 'keys');
            internal_1.ArgumentChecker.hasType(keys, 'keys', false, Array);
            yield this._communicator.send(operation, (payload) => __awaiter(this, void 0, void 0, function* () {
                this._writeCacheInfo(payload);
                yield this._writeKeys(payload, keys);
            }), payloadReader);
        });
    }
    /**
     * @ignore
     */
    _writeKeysReadBooleanOp(operation, keys) {
        return __awaiter(this, void 0, void 0, function* () {
            let result = false;
            yield this._writeKeysOp(operation, keys, (payload) => __awaiter(this, void 0, void 0, function* () {
                result = payload.readBoolean();
            }));
            return result;
        });
    }
}
exports.CacheClient = CacheClient;
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
exports.CacheEntry = CacheEntry;
//# sourceMappingURL=CacheClient.js.map