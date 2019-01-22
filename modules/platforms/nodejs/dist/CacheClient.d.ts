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
export declare class CacheClient {
    private _keyType;
    private _valueType;
    private _communicator;
    private _name;
    private _cacheId;
    private _config;
    static readonly PEEK_MODE: Readonly<{
        ALL: number;
        NEAR: number;
        PRIMARY: number;
        BACKUP: number;
    }>;
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
    setKeyType(type: any): this;
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
    setValueType(type: any): this;
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
    get(key: any): Promise<any>;
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
    getAll(keys: any): Promise<any>;
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
    put(key: any, value: any): Promise<void>;
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
    putAll(entries: any): Promise<void>;
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
    containsKey(key: any): Promise<boolean>;
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
    containsKeys(keys: any): Promise<boolean>;
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
    getAndPut(key: any, value: any): Promise<any>;
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
    getAndReplace(key: any, value: any): Promise<any>;
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
    getAndRemove(key: any): Promise<any>;
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
    putIfAbsent(key: any, value: any): Promise<boolean>;
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
    getAndPutIfAbsent(key: any, value: any): Promise<any>;
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
    replace(key: any, value: any): Promise<boolean>;
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
    replaceIfEquals(key: any, value: any, newValue: any): Promise<any>;
    /**
     * Removes all entries from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    clear(): Promise<void>;
    /**
     * Removes entry with the specified key from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @param {*} key - key to be removed.
     *
     * @throws {IgniteClientError} if error.
     */
    clearKey(key: any): Promise<void>;
    /**
     * Removes entries with the specified keys from the cache, without notifying listeners and cache writers.
     *
     * @async
     *
     * @param {Array<*>} keys - keys to be removed.
     *
     * @throws {IgniteClientError} if error.
     */
    clearKeys(keys: any): Promise<void>;
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
    removeKey(key: any): Promise<boolean>;
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
    removeIfEquals(key: any, value: any): Promise<boolean>;
    /**
     * Removes entries with the specified keys from the cache, notifying listeners and cache writers.
     *
     * @async
     *
     * @param {Array<*>} keys - keys to be removed.
     *
     * @throws {IgniteClientError} if error.
     */
    removeKeys(keys: any): Promise<void>;
    /**
     * Removes all entries from the cache, notifying listeners and cache writers.
     *
     * @async
     *
     * @throws {IgniteClientError} if error.
     */
    removeAll(): Promise<void>;
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
    getSize(...peekModes: any[]): Promise<any>;
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
    query(query: any): Promise<any>;
    /** Private methods */
    /**
     * @ignore
     */
    constructor(name: any, config: any, communicator: any);
    /**
     * @ignore
     */
    static _calculateId(name: any): number;
    /**
     * @ignore
     */
    _writeCacheInfo(payload: any): void;
    /**
     * @ignore
     */
    _writeKeyValue(payload: any, key: any, value: any): Promise<void>;
    /**
     * @ignore
     */
    _writeKeys(payload: any, keys: any): Promise<void>;
    /**
     * @ignore
     */
    _getKeyType(): any;
    /**
     * @ignore
     */
    _getValueType(): any;
    /**
     * @ignore
     */
    _writeKeyValueOp(operation: any, key: any, value: any, payloadReader?: any): Promise<void>;
    /**
     * @ignore
     */
    _writeKeyValueReadValueOp(operation: any, key: any, value: any): Promise<any>;
    /**
     * @ignore
     */
    _writeKeyValueReadBooleanOp(operation: any, key: any, value: any): Promise<boolean>;
    /**
     * @ignore
     */
    _writeKeyOp(operation: any, key: any, payloadReader?: any): Promise<void>;
    /**
     * @ignore
     */
    _writeKeyReadValueOp(operation: any, key: any): Promise<any>;
    /**
     * @ignore
     */
    _writeKeyReadBooleanOp(operation: any, key: any): Promise<boolean>;
    /**
     * @ignore
     */
    _writeKeysOp(operation: any, keys: any, payloadReader?: any): Promise<void>;
    /**
     * @ignore
     */
    _writeKeysReadBooleanOp(operation: any, keys: any): Promise<boolean>;
}
/**
 * A cache entry (key-value pair).
 */
export declare class CacheEntry {
    private _key;
    private _value;
    /**
     * Public constructor.
     *
     * @param {*} key - key corresponding to this entry.
     * @param {*} value - value associated with the key.
     *
     * @return {CacheEntry} - new CacheEntry instance
     */
    constructor(key: any, value: any);
    /**
     * Returns the key corresponding to this entry.
     *
     * @return {*} - the key corresponding to this entry.
     */
    getKey(): any;
    /**
     * Returns the value corresponding to this entry.
     *
     * @return {*} - the value corresponding to this entry.
     */
    getValue(): any;
}
