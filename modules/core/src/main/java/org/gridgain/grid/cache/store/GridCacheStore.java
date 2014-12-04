/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.store;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.jdbc.*;
import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 * API for cache persistent storage for read-through and write-through behavior.
 * Persistent store is configured via {@link GridCacheConfiguration#getStore()}
 * configuration property. If not provided, values will be only kept in cache memory
 * or swap storage without ever being persisted to a persistent storage.
 * <p>
 * {@link GridCacheStoreAdapter} provides default implementation for bulk operations,
 * such as {@link #loadAll(GridCacheTx, Collection, org.apache.ignite.lang.IgniteBiInClosure)},
 * {@link #putAll(GridCacheTx, Map)}, and {@link #removeAll(GridCacheTx, Collection)}
 * by sequentially calling corresponding {@link #load(GridCacheTx, Object)},
 * {@link #put(GridCacheTx, Object, Object)}, and {@link #remove(GridCacheTx, Object)}
 * operations. Use this adapter whenever such behaviour is acceptable. However
 * in many cases it maybe more preferable to take advantage of database batch update
 * functionality, and therefore default adapter implementation may not be the best option.
 * <p>
 * Provided implementations may be used for test purposes:
 * <ul>
 *     <li>{@gglink org.gridgain.grid.cache.store.hibernate.GridCacheHibernateBlobStore}</li>
 *     <li>{@link GridCacheJdbcBlobStore}</li>
 * </ul>
 * <p>
 * All transactional operations of this API are provided with ongoing {@link GridCacheTx},
 * if any. As transaction is {@link GridMetadataAware}, you can attach any metadata to
 * it, e.g. to recognize if several operations belong to the same transaction or not.
 * Here is an example of how attach a JDBC connection as transaction metadata:
 * <pre name="code" class="java">
 * Connection conn = tx.meta("some.name");
 *
 * if (conn == null) {
 *     conn = ...; // Get JDBC connection.
 *
 *     // Store connection in transaction metadata, so it can be accessed
 *     // for other operations on the same transaction.
 *     tx.addMeta("some.name", conn);
 * }
 * </pre>
 * <h1 class="header">Working With Portable Objects</h1>
 * When portables are enabled for cache by setting {@link GridCacheConfiguration#isPortableEnabled()} to
 * {@code true}), all portable keys and values are converted to instances of {@link org.apache.ignite.portables.GridPortableObject}.
 * Therefore, all cache store methods will take parameters in portable format. To avoid class
 * cast exceptions, store must have signature compatible with portables. E.g., if you use {@link Integer}
 * as a key and {@code Value} class as a value (which will be converted to portable format), cache store
 * signature should be the following:
 * <pre>
 * public class PortableCacheStore implements GridCacheStore&lt;Integer, GridPortableObject&gt; {
 *     public void put(@Nullable GridCacheTx tx, Integer key, GridPortableObject val) throws GridException {
 *         ...
 *     }
 *
 *     ...
 * }
 * </pre>
 * Note that only portable classes are converted to {@link org.apache.ignite.portables.GridPortableObject} format. Following
 * types are stored in cache without changes and therefore should not affect cache store signature:
 * <ul>
 *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
 *     <li>Arrays of primitives (byte[], int[], ...)</li>
 *     <li>{@link String} and array of {@link String}s</li>
 *     <li>{@link UUID} and array of {@link UUID}s</li>
 *     <li>{@link Date} and array of {@link Date}s</li>
 *     <li>{@link Timestamp} and array of {@link Timestamp}s</li>
 *     <li>Enums and array of enums</li>
 *     <li>
 *         Maps, collections and array of objects (but objects inside
 *         them will still be converted if they are portable)
 *     </li>
 * </ul>
 *
 * @see org.apache.ignite.IgnitePortables
 */
public interface GridCacheStore<K, V> {
    /**
     * Loads value for the key from underlying persistent storage.
     *
     * @param tx Cache transaction.
     * @param key Key to load.
     * @return Loaded value or {@code null} if value was not found.
     * @throws GridException If load failed.
     */
    @Nullable public V load(@Nullable GridCacheTx tx, K key) throws GridException;

    /**
     * Loads all values from underlying persistent storage. Note that keys are not
     * passed, so it is up to implementation to figure out what to load. This method
     * is called whenever {@link GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)}
     * method is invoked which is usually to preload the cache from persistent storage.
     * <p>
     * This method is optional, and cache implementation does not depend on this
     * method to do anything. Default implementation of this method in
     * {@link GridCacheStoreAdapter} does nothing.
     * <p>
     * For every loaded value method {@link org.apache.ignite.lang.IgniteBiInClosure#apply(Object, Object)}
     * should be called on the passed in closure. The closure will then make sure
     * that the loaded value is stored in cache.
     *
     * @param clo Closure for loaded values.
     * @param args Arguments passes into
     *      {@link GridCache#loadCache(org.apache.ignite.lang.IgniteBiPredicate, long, Object...)} method.
     * @throws GridException If loading failed.
     */
    public void loadCache(IgniteBiInClosure<K, V> clo, @Nullable Object... args) throws GridException;

    /**
     * Loads all values for given keys and passes every value to the provided closure.
     * <p>
     * For every loaded value method {@link org.apache.ignite.lang.IgniteInClosure#apply(Object)} should be called on
     * the passed in closure. The closure will then make sure that the loaded value is stored
     * in cache.
     *
     * @param tx Cache transaction.
     * @param keys Collection of keys to load.
     * @param c Closure to call for every loaded element.
     * @throws GridException If load failed.
     */
    public void loadAll(@Nullable GridCacheTx tx, Collection<? extends K> keys, IgniteBiInClosure<K, V> c)
        throws GridException;

    /**
     * Stores a given value in persistent storage. Note that if write-behind is configured for a
     * particular cache, transaction object passed in the cache store will be always {@code null}.
     *
     * @param tx Cache transaction, if write-behind is not enabled, {@code null} otherwise.
     * @param key Key to put.
     * @param val Value to put.
     * @throws GridException If put failed.
     */
    public void put(@Nullable GridCacheTx tx, K key, V val) throws GridException;

    /**
     * Stores given key value pairs in persistent storage. Note that if write-behind is configured
     * for a particular cache, transaction object passed in the cache store will be always {@code null}.
     *
     * @param tx Cache transaction, if write-behind is not enabled, {@code null} otherwise.
     * @param map Values to store.
     * @throws GridException If store failed.
     */
    public void putAll(@Nullable GridCacheTx tx, Map<? extends K, ? extends V> map) throws GridException;

    /**
     * Removes the value identified by given key from persistent storage. Note that  if write-behind is
     * configured for a particular cache, transaction object passed in the cache store will be always
     * {@code null}.
     *
     * @param tx Cache transaction, if write-behind is not enabled, {@code null} otherwise.
     * @param key Key to remove.
     * @throws GridException If remove failed.
     */
    public void remove(@Nullable GridCacheTx tx, K key) throws GridException;

    /**
     * Removes all vales identified by given keys from persistent storage. Note that if write-behind
     * is configured for a particular cache, transaction object passed in the cache store will be
     * always {@code null}.
     *
     * @param tx Cache transaction, if write-behind is not enabled, {@code null} otherwise.
     * @param keys Keys to remove.
     * @throws GridException If remove failed.
     */
    public void removeAll(@Nullable GridCacheTx tx, Collection<? extends K> keys) throws GridException;

    /**
     * Tells store to commit or rollback a transaction depending on the value of the {@code 'commit'}
     * parameter.
     *
     * @param tx Cache transaction being ended.
     * @param commit {@code True} if transaction should commit, {@code false} for rollback.
     * @throws GridException If commit or rollback failed. Note that commit failure in some cases
     *      may bring cache transaction into {@link GridCacheTxState#UNKNOWN} which will
     *      consequently cause all transacted entries to be invalidated.
     */
    public void txEnd(GridCacheTx tx, boolean commit) throws GridException;
}
