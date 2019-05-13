import { CacheClient } from './internal';
/**
 * Class representing Ignite client.
 *
 */
export declare class IgniteClient {
    private _socket;
    private _communicator;
    /**
     * Public constructor.
     *
     * @param {IgniteClient.onStateChanged} [onStateChanged] -
     * callback called everytime when the client has moved to a new state {@link IgniteClient.STATE}.
     *
     * @return {IgniteClient} - new IgniteClient instance.
     */
    constructor(onStateChanged?: any);
    static readonly STATE: Readonly<{
        DISCONNECTED: number;
        CONNECTING: number;
        CONNECTED: number;
    }>;
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
    connect(config: any): Promise<void>;
    /**
     * Disconnects the client.
     *
     * Moves the client to DISCONNECTED state from any other state.
     * Does nothing if the client already disconnected.
     */
    disconnect(): void;
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
    createCache(name: any, cacheConfig?: any): Promise<CacheClient>;
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
    getOrCreateCache(name: any, cacheConfig?: any): Promise<CacheClient>;
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
    getCache(name: any): CacheClient;
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
    destroyCache(name: any): Promise<void>;
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
    getCacheConfiguration(name: any): Promise<any>;
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
    cacheNames(): Promise<any>;
    /**
     * Enables/disables the library debug output (including errors logging).
     * Disabled by default.
     *
     * @param {boolean} value - true to enable, false to disable
     */
    setDebug(value: any): void;
    /** Private methods */
    /**
     * @ignore
     */
    _getCache(name: any, cacheConfig?: any): CacheClient;
    /**
     * @ignore
     */
    _writeCacheNameOrConfig(buffer: any, name: any, cacheConfig: any): Promise<void>;
}
