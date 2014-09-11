/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.dr.cache.sender.*;
import org.gridgain.grid.kernal.processors.cache.dr.*;
import org.gridgain.grid.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Internal projection interface.
 */
public interface GridCacheProjectionEx<K, V> extends GridCacheProjection<K, V> {
    /**
     * Creates projection for specified subject ID.
     *
     * @param subjId Client ID.
     * @return Internal projection.
     */
    GridCacheProjectionEx<K, V> forSubjectId(UUID subjId);

    /**
     * Gets predicate on which this projection is based on or {@code null}
     * if predicate is not defined.
     *
     * @return Filter on which this projection is based on.
     */
    @Nullable public GridPredicate<GridCacheEntry<K, V>> predicate();

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Cached entry. If not provided, equivalent to {GridCacheProjection#put}.
     * @param ttl Optional time-to-live. If negative, leaves ttl value unchanged.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws GridException If failed.
     */
    @Nullable public V put(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException;

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Optional cached entry.
     * @param ttl Optional time-to-live value. If negative, leaves ttl value unchanged.
     * @param filter Optional filter.
     * @return Put operation future.
     */
    public GridFuture<V> putAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter);

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Cached entry. If not provided, equivalent to {GridCacheProjection#put}.
     * @param ttl Optional time-to-live. If negative, leaves ttl value unchanged.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws GridException If failed.
     */
    public boolean putx(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException;

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key.
     * @param val Value.
     * @param entry Cached entry. If not provided, equivalent to {GridCacheProjection#put}.
     * @param ttl Optional time-to-live. If negative, leave ttl value unchanged.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public GridFuture<Boolean> putxAsync(K key, V val, @Nullable GridCacheEntryEx<K, V> entry, long ttl,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter);

    /**
     * Store DR data.
     *
     * @param drMap DR map.
     * @throws GridException If put operation failed.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public void putAllDr(Map<? extends K, GridCacheDrInfo<V>> drMap) throws GridException;

    /**
     * Store DR data asynchronously.
     *
     * @param drMap DR map.
     * @return Future.
     * @throws GridException If put operation failed.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public GridFuture<?> putAllDrAsync(Map<? extends K, GridCacheDrInfo<V>> drMap) throws GridException;

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key.
     * @param transformer Transformer closure.
     * @param entry Cached entry.
     * @param ttl Optional time-to-lve.
     * @return Transform operation future.
     */
    public GridFuture<?> transformAsync(K key, GridClosure<V, V> transformer, @Nullable GridCacheEntryEx<K, V> entry,
        long ttl);

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Cached entry. If not provided, equivalent to {GridCacheProjection#put}.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws GridException If failed.
     */
    @Nullable public V remove(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException;

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Optional cached entry.
     * @param filter Optional filter.
     * @return Put operation future.
     */
    public GridFuture<V> removeAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter);

    /**
     * Removes DR data.
     *
     * @param drMap DR map.
     * @throws GridException If remove failed.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public void removeAllDr(Map<? extends K, GridCacheVersion> drMap) throws GridException;

    /**
     * Removes DR data asynchronously.
     *
     * @param drMap DR map.
     * @return Future.
     * @throws GridException If remove failed.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public GridFuture<?> removeAllDrAsync(Map<? extends K, GridCacheVersion> drMap) throws GridException;

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Cached entry. If not provided, equivalent to {GridCacheProjection#put}.
     * @param filter Optional filter.
     * @return Previous value.
     * @throws GridException If failed.
     */
    public boolean removex(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException;

    /**
     * Internal method that is called from {@link GridCacheEntryImpl}.
     *
     * @param key Key to remove.
     * @param entry Cached entry. If not provided, equivalent to {GridCacheProjection#put}.
     * @param filter Optional filter.
     * @return Putx operation future.
     */
    public GridFuture<Boolean> removexAsync(K key, @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter);

    /**
     * Asynchronously stores given key-value pair in cache only if only if the previous value is equal to the
     * {@code 'oldVal'} passed in.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link GridCacheStore}
     * via {@link GridCacheStore#put(GridCacheTx, Object, Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link GridCacheFlag#LOCAL}, {@link GridCacheFlag#READ}.
     *
     * @param key Key to store in cache.
     * @param oldVal Old value to match.
     * @param newVal Value to be associated with the given key.
     * @return Future for the replace operation. The future will return object containing actual old value and success
     *      flag.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public GridFuture<GridCacheReturn<V>> replacexAsync(K key, V oldVal, V newVal);

    /**
     * Stores given key-value pair in cache only if only if the previous value is equal to the
     * {@code 'oldVal'} passed in.
     * <p>
     * This method will return {@code true} if value is stored in cache and {@code false} otherwise.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link GridCacheStore}
     * via {@link GridCacheStore#put(GridCacheTx, Object, Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link GridCacheFlag#LOCAL}, {@link GridCacheFlag#READ}.
     *
     * @param key Key to store in cache.
     * @param oldVal Old value to match.
     * @param newVal Value to be associated with the given key.
     * @return Object containing actual old value and success flag.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws GridException If replace operation failed.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public GridCacheReturn<V> replacex(K key, V oldVal, V newVal) throws GridException;

    /**
     * Removes given key mapping from cache if one exists and value is equal to the passed in value.
     * <p>
     * If write-through is enabled, the value will be removed from {@link GridCacheStore}
     * via {@link GridCacheStore#remove(GridCacheTx, Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link GridCacheFlag#LOCAL}, {@link GridCacheFlag#READ}.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @param val Value to match against currently cached value.
     * @return Object containing actual old value and success flag.
     * @throws NullPointerException if the key or value is {@code null}.
     * @throws GridException If remove failed.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public GridCacheReturn<V> removex(K key, V val) throws GridException;

    /**
     * Asynchronously removes given key mapping from cache if one exists and value is equal to the passed in value.
     * <p>
     * This method will return {@code true} if remove did occur, which means that all optionally
     * provided filters have passed and there was something to remove, {@code false} otherwise.
     * <p>
     * If write-through is enabled, the value will be removed from {@link GridCacheStore}
     * via {@link GridCacheStore#remove(GridCacheTx, Object)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     * <h2 class="header">Cache Flags</h2>
     * This method is not available if any of the following flags are set on projection:
     * {@link GridCacheFlag#LOCAL}, {@link GridCacheFlag#READ}.
     *
     * @param key Key whose mapping is to be removed from cache.
     * @param val Value to match against currently cached value.
     * @return Future for the remove operation. The future will return object containing actual old value and success
     *      flag.
     * @throws NullPointerException if the key or value is {@code null}.
     * @throws GridCacheFlagException If projection flags validation failed.
     */
    public GridFuture<GridCacheReturn<V>> removexAsync(K key, V val);

    /**
     * @param key Key to retrieve the value for.
     * @param entry Cached entry when called from entry wrapper.
     * @param filter Filter to check prior to getting the value. Note that filter check
     *      together with getting the value is an atomic operation.
     * @return Value.
     * @throws GridException If failed.
     */
    @Nullable public V get(K key, @Nullable GridCacheEntryEx<K, V> entry, boolean deserializePortable,
        @Nullable GridPredicate<GridCacheEntry<K, V>>... filter) throws GridException;

    /**
     * Gets value from cache. Will go to primary node even if this is a backup.
     *
     * @param key Key to get value for.
     * @return Value.
     * @throws GridException If failed.
     */
    @Nullable public V getForcePrimary(K key) throws GridException;

    /**
     * Asynchronously gets value from cache. Will go to primary node even if this is a backup.
     *
     * @param key Key to get value for.
     * @return Future with result.
     */
    public GridFuture<V> getForcePrimaryAsync(K key);

    /**
     * Gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys Keys to get values for.
     * @return Value.
     * @throws GridException If failed.
     */
    @Nullable public Map<K, V> getAllOutTx(List<K> keys) throws GridException;

    /**
     * Asynchronously gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys Keys to get values for.
     * @return Future with result.
     */
    public GridFuture<Map<K, V>> getAllOutTxAsync(List<K> keys);

    /**
     * Checks whether this cache is GGFS data cache.
     *
     * @return {@code True} in case this cache is GGFS data cache.
     */
    public boolean isGgfsDataCache();

    /**
     * Get current amount of used GGFS space in bytes.
     *
     * @return Amount of used GGFS space in bytes.
     */
    public long ggfsDataSpaceUsed();

    /**
     * Get maximum space available for GGFS.
     *
     * @return Amount of space available for GGFS in bytes.
     */
    public long ggfsDataSpaceMax();

    /**
     * Checks whether this cache is Mongo data cache.
     *
     * @return {@code True} if this cache is mongo data cache.
     */
    public boolean isMongoDataCache();

    /**
     * Checks whether this cache is Mongo meta cache.
     *
     * @return {@code True} if this cache is mongo meta cache.
     */
    public boolean isMongoMetaCache();

    /**
     * Checks whether this cache is DR system cache.
     *
     * @return {@code True} if this cache is DR system cache.
     */
    public boolean isDrSystemCache();

    /**
     * Starts full state transfer.
     *
     * @param dataCenterIds  Data center IDs for which full state transfer was requested.
     * @return Future that will be completed when all replication batches are sent.
     */
    public GridFuture<?> drStateTransfer(Collection<Byte> dataCenterIds);

    /**
     * List currently active state transfers.
     *
     * @return Collection of currently active state transfers.
     */
    public Collection<GridDrStateTransferDescriptor> drListStateTransfers();

    /**
     * Pauses data center replication for this cache.
     */
    public void drPause();

    /**
     * Resumes data center replication for this cache.
     */
    public void drResume();

    /**
     * Get DR pause state.
     *
     * @return DR pause state.
     */
    @Nullable public GridDrStatus drPauseState();

    /**
     * Gets entry set containing internal entries.
     *
     * @param filter Filter.
     * @return Entry set.
     */
    public Set<GridCacheEntry<K, V>> entrySetx(GridPredicate<GridCacheEntry<K, V>>... filter);

    /**
     * Gets set of primary entries containing internal entries.
     *
     * @param filter Optional filter.
     * @return Primary entry set.
     */
    public Set<GridCacheEntry<K, V>> primaryEntrySetx(GridPredicate<GridCacheEntry<K, V>>... filter);

    /**
     * Whether "keepPortable" enabled.
     *
     * @return {@code True} if keep portable is enabled.
     */
    public boolean isKeepPortableEnabled();
}
