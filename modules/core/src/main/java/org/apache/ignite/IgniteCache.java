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

package org.apache.ignite;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Configuration;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SpiQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteAsyncSupport;
import org.apache.ignite.lang.IgniteAsyncSupported;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.TransactionException;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.Nullable;

/**
 * Main entry point for all <b>Data Grid APIs.</b> You can get a named cache by calling {@link Ignite#cache(String)}
 * method.
 * <h1 class="header">Functionality</h1>
 * This API extends {@link javax.cache.Cache} API which contains {@code JCache (JSR107)} cache functionality
 * and documentation. In addition to {@link javax.cache.Cache} functionality this API provides:
 * <ul>
 * <li>Ability to perform basic atomic Map-like operations available on {@code JCache} API.</li>
 * <li>Ability to bulk load cache via {@link #loadCache(IgniteBiPredicate, Object...)} method.
 * <li>Distributed lock functionality via {@link #lock(Object)} methods.</li>
 * <li>Ability to query cache using Predicate, SQL, and Text queries via {@link #query(Query)} method.</li>
 * <li>Ability to collect cache and query metrics.</li>
 * <li>Ability to force partition rebalancing via {@link #rebalance()} methopd
 *  (in case if delayed rebalancing was configured.)</li>
 * <li>Ability to peek into memory without doing actual {@code get(...)} from cache
 *  via {@link #localPeek(Object, CachePeekMode...)} methods</li>
 * <li>Ability to evict and promote entries from on-heap to off-heap or swap and back.</li>
 * <li>Ability to atomically collocate compute and data via {@link #invoke(Object, CacheEntryProcessor, Object...)}
 *  methods.</li>
 * </ul>
 * <h1 class="header">Transactions</h1>
 * Cache API supports transactions. You can group and set of cache methods within a transaction
 * to provide ACID-compliant behavior. See {@link IgniteTransactions} for more information.
 * <br>
 * Methods which can be used inside transaction (put, get...) throw TransactionException.
 * See {@link TransactionException} for more information.
 *
 * @param <K> Cache key type.
 * @param <V> Cache value type.
 */
public interface IgniteCache<K, V> extends javax.cache.Cache<K, V>, IgniteAsyncSupport {
    /** {@inheritDoc} */
    @Deprecated
    @Override public IgniteCache<K, V> withAsync();

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz);

    /**
     * Returns cache with the specified expired policy set. This policy will be used for each operation
     * invoked on the returned cache.
     * <p>
     * This method does not modify existing cache instance.
     *
     * @param plc Expire policy to use.
     * @return Cache instance with the specified expiry policy set.
     */
    public IgniteCache<K, V> withExpiryPolicy(ExpiryPolicy plc);

    /**
     * @return Cache with read-through write-through behavior disabled.
     */
    public IgniteCache<K, V> withSkipStore();

    /**
     * @return Cache with no-retries behavior enabled.
     */
    public IgniteCache<K, V> withNoRetries();

    /**
     * Gets an instance of {@code IgniteCache} that will be allowed to execute cache operations (read, write)
     * regardless of partition loss policy.
     *
     * @return Cache without partition loss protection.
     */
    public IgniteCache<K, V> withPartitionRecover();

    /**
     * Returns cache that will operate with binary objects.
     * <p>
     * Cache returned by this method will not be forced to deserialize binary objects,
     * so keys and values will be returned from cache API methods without changes. Therefore,
     * signature of the cache can contain only following types:
     * <ul>
     *     <li><code>org.apache.ignite.binary.BinaryObject</code> for binary classes</li>
     *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
     *     <li>Arrays of primitives (byte[], int[], ...)</li>
     *     <li>{@link String} and array of {@link String}s</li>
     *     <li>{@link UUID} and array of {@link UUID}s</li>
     *     <li>{@link Date} and array of {@link Date}s</li>
     *     <li>{@link Timestamp} and array of {@link Timestamp}s</li>
     *     <li>Enums and array of enums</li>
     *     <li>
     *         Maps, collections and array of objects (but objects inside
     *         them will still be converted if they are binary)
     *     </li>
     * </ul>
     * <p>
     * For example, if you use {@link Integer} as a key and {@code Value} class as a value
     * (which will be stored in binary format), you should acquire following projection
     * to avoid deserialization:
     * <pre>
     * IgniteCache<Integer, BinaryObject> prj = cache.withKeepBinary();
     *
     * // Value is not deserialized and returned in binary format.
     * BinaryObject po = prj.get(1);
     * </pre>
     * <p>
     * Note that this method makes sense only if cache is working in binary mode
     * if default marshaller is used.
     * If not, this method is no-op and will return current cache.
     *
     * @return New cache instance for binary objects.
     */
    public <K1, V1> IgniteCache<K1, V1> withKeepBinary();

    /**
     * Executes {@link #localLoadCache(IgniteBiPredicate, Object...)} on all cache nodes.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values loaded from storage before they are put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @throws CacheException If loading failed.
     */
    @IgniteAsyncSupported
    public void loadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException;

    /**
     * Asynchronously executes {@link #localLoadCache(IgniteBiPredicate, Object...)} on all cache nodes.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values loaded from storage before they are put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @return a Future representing pending completion of the cache loading.
     * @throws CacheException If loading failed.
     */
    public IgniteFuture<Void> loadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws CacheException;

    /**
     * Delegates to {@link CacheStore#loadCache(IgniteBiInClosure,Object...)} method
     * to load state from the underlying persistent storage. The loaded values
     * will then be given to the optionally passed in predicate, and, if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all loaded values will be stored in cache.
     * <p>
     * Note that this method does not receive keys as a parameter, so it is up to
     * {@link CacheStore} implementation to provide all the data to be loaded.
     * <p>
     * This method is not transactional and may end up loading a stale value into
     * cache if another thread has updated the value immediately after it has been
     * loaded. It is mostly useful when pre-loading the cache from underlying
     * data store before start, or for read-only caches.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @throws CacheException If loading failed.
     */
    @IgniteAsyncSupported
    public void localLoadCache(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args) throws CacheException;

    /**
     * Asynchronously loads state from the underlying persistent storage by delegating
     * to {@link CacheStore#loadCache(IgniteBiInClosure,Object...)} method. The loaded values
     * will then be given to the optionally passed in predicate, and, if the predicate returns
     * {@code true}, will be stored in cache. If predicate is {@code null}, then
     * all loaded values will be stored in cache.
     * <p>
     * Note that this method does not receive keys as a parameter, so it is up to
     * {@link CacheStore} implementation to provide all the data to be loaded.
     * <p>
     * This method is not transactional and may end up loading a stale value into
     * cache if another thread has updated the value immediately after it has been
     * loaded. It is mostly useful when pre-loading the cache from underlying
     * data store before start, or for read-only caches.
     *
     * @param p Optional predicate (may be {@code null}). If provided, will be used to
     *      filter values to be put into cache.
     * @param args Optional user arguments to be passed into
     *      {@link CacheStore#loadCache(IgniteBiInClosure, Object...)} method.
     * @return a Future representing pending completion of the cache loading.
     * @throws CacheException If loading failed.
     */
    public IgniteFuture<Void> localLoadCacheAsync(@Nullable IgniteBiPredicate<K, V> p, @Nullable Object... args)
        throws CacheException;

    /**
     * Stores given key-value pair in cache only if cache had no previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned.
     * In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, {@link CacheStore#load(Object)} method will be used.
     * <p>
     * If the returned value is not needed, method {@link #putIfAbsent(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via {@link CacheStore#write(javax.cache.Cache.Entry)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return Previously contained value regardless of whether put happened or not ({@code null} if there was no
     *      previous value).
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws CacheException If put operation failed.
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    public V getAndPutIfAbsent(K key, V val) throws CacheException, TransactionException;

    /**
     * Asynchronously stores given key-value pair in cache only if cache had no previous mapping for it. If cache
     * previously contained value for the given key, then this value is returned.
     * In case of {@link CacheMode#PARTITIONED} or {@link CacheMode#REPLICATED} caches,
     * the value will be loaded from the primary node, which in its turn may load the value
     * from the swap storage, and consecutively, if it's not in swap,
     * from the underlying persistent storage. If value has to be loaded from persistent
     * storage, {@link CacheStore#load(Object)} method will be used.
     * <p>
     * If the returned value is not needed, method {@link #putIfAbsentAsync(Object, Object)} should
     * always be used instead of this one to avoid the overhead associated with returning of the
     * previous value.
     * <p>
     * If write-through is enabled, the stored value will be persisted to {@link CacheStore}
     * via {@link CacheStore#write(javax.cache.Cache.Entry)} method.
     * <h2 class="header">Transactions</h2>
     * This method is transactional and will enlist the entry into ongoing transaction
     * if there is one.
     *
     * @param key Key to store in cache.
     * @param val Value to be associated with the given key.
     * @return a Future representing pending completion of the operation.
     * @throws NullPointerException If either key or value are {@code null}.
     * @throws CacheException If put operation failed.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<V> getAndPutIfAbsentAsync(K key, V val) throws CacheException, TransactionException;

    /**
     * Creates a {@link Lock} instance associated with passed key.
     * This method does not acquire lock immediately, you have to call appropriate method on returned instance.
     * Returned lock does not support {@link Lock#newCondition()} method,
     * other methods defined in {@link Lock} are supported.
     *
     * @param key Key for lock.
     * @return New lock instance associated with passed key.
     * @see Lock#lock()
     * @see Lock#tryLock(long, TimeUnit)
     */
    public Lock lock(K key);

    /**
     * Creates a {@link Lock} instance associated with passed keys.
     * This method does not acquire lock immediately, you have to call appropriate method on returned instance.
     * Returned lock does not support {@link Lock#newCondition()} method,
     * other methods defined in {@link Lock} are supported.
     *
     * @param keys Keys for lock.
     * @return New lock instance associated with passed key.
     * @see Lock#lock()
     * @see Lock#tryLock(long, TimeUnit)
     */
    public Lock lockAll(Collection<? extends K> keys);

    /**
     * Checks if specified key is locked.
     * <p>
     * This is a local in-VM operation and does not involve any network trips
     * or access to persistent storage in any way.
     *
     * @param key Key to check.
     * @param byCurrThread If {@code true} method will check that current thread owns a lock on this key, other vise
     *     will check that any thread on any node owns a lock on this key.
     * @return {@code True} if lock is owned by some node.
     */
    public boolean isLocalLocked(K key, boolean byCurrThread);

    /**
     * Queries cache. Accepts any subclass of {@link Query} interface.
     * See also {@link #query(SqlFieldsQuery)}.
     *
     * @param qry Query.
     * @return Cursor.
     * @see ScanQuery
     * @see SqlQuery
     * @see SqlFieldsQuery
     * @see TextQuery
     * @see SpiQuery
     *
     */
    public <R> QueryCursor<R> query(Query<R> qry);

    /**
     * Queries cache. Accepts {@link SqlFieldsQuery} class.
     *
     * @param qry SqlFieldsQuery.
     * @return Cursor.
     * @see SqlFieldsQuery
     */
    public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry);

    /**
     * Queries the cache transforming the entries on the server nodes. Can be used, for example,
     * to avoid network overhead in case only one field out of the large is required by client.
     * <p>
     * Currently transformers are supported ONLY for {@link ScanQuery}. Passing any other
     * subclass of {@link Query} interface to this method will end up with
     * {@link UnsupportedOperationException}.
     *
     * @param qry Query.
     * @param transformer Transformer.
     * @return Cursor.
     */
    public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer);

    /**
     * Allows for iteration over local cache entries.
     *
     * @param peekModes Peek modes.
     * @return Iterable over local cache entries.
     * @throws CacheException If failed.
     */
    public Iterable<Entry<K, V>> localEntries(CachePeekMode... peekModes) throws CacheException;

    /**
     * Gets query metrics.
     *
     * @return Metrics.
     */
    public QueryMetrics queryMetrics();

    /**
     * Reset query metrics.
     */
    public void resetQueryMetrics();

    /**
     * Gets query detail metrics.
     * Query detail metrics could be enabled via {@link CacheConfiguration#setQueryDetailMetricsSize(int)} method.
     *
     * @return Metrics.
     */
    public Collection<? extends QueryDetailMetrics> queryDetailMetrics();

    /**
     * Reset query detail metrics.
     */
    public void resetQueryDetailMetrics();

    /**
     * Attempts to evict all entries associated with keys. Note,
     * that entry will be evicted only if it's not used (not
     * participating in any locks or transactions).
     *
     * @param keys Keys to evict.
     */
    public void localEvict(Collection<? extends K> keys);

    /**
     * Peeks at in-memory cached value using default optional peek mode.
     * <p>
     * This method will not load value from any persistent store or from a remote node.
     * <h2 class="header">Transactions</h2>
     * This method does not participate in any transactions.
     *
     * @param key Entry key.
     * @param peekModes Peek modes.
     * @return Peeked value, or {@code null} if not found.
     * @throws NullPointerException If key is {@code null}.
     */
    public V localPeek(K key, CachePeekMode... peekModes);

    /**
     * Gets the number of all entries cached across all nodes. By default, if {@code peekModes} value isn't defined,
     * only size of primary copies across all nodes will be returned. This behavior is identical to calling
     * this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size across all nodes.
     * @throws CacheException On error.
     */
    @IgniteAsyncSupported
    public int size(CachePeekMode... peekModes) throws CacheException;

    /**
     * Asynchronously gets the number of all entries cached across all nodes. By default,
     * if {@code peekModes} value isn't defined, only size of primary copies across all nodes will be returned.
     * This behavior is identical to calling this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return a Future representing pending completion of the operation.
     * @throws CacheException On error.
     */
    public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException;

    /**
     * Gets the number of all entries cached across all nodes as a long value. By default, if {@code peekModes} value
     * isn't defined, only size of primary copies across all nodes will be returned. This behavior is identical to
     * calling this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size across all nodes.
     * @throws CacheException On error.
     */
    @IgniteAsyncSupported
    public long sizeLong(CachePeekMode... peekModes) throws CacheException;

    /**
     * Asynchronously gets the number of all entries cached across all nodes as a long value. By default,
     * if {@code peekModes} value isn't defined, only size of primary copies across all nodes will be returned.
     * This behavior is identical to calling this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return a Future representing pending completion of the operation.
     * @throws CacheException On error.
     */
    public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException;

    /**
     * Gets the number of all entries cached in a partition as a long value. By default, if {@code peekModes} value
     * isn't defined, only size of primary copies across all nodes will be returned. This behavior is identical to
     * calling this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their partition cache sizes.
     *
     * @param partition partition.
     * @param peekModes Optional peek modes. If not provided, then total partition cache size is returned.
     * @return Partition cache size across all nodes.
     * @throws CacheException On error.
     */
    @IgniteAsyncSupported
    public long sizeLong(int partition, CachePeekMode... peekModes) throws CacheException;

    /**
     * Asynchronously gets the number of all entries cached in a partition as a long value. By default, if {@code peekModes} value
     * isn't defined, only size of primary copies across all nodes will be returned. This behavior is identical to
     * calling this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their partition cache sizes.
     *
     * @param partition partition.
     * @param peekModes Optional peek modes. If not provided, then total partition cache size is returned.
     * @return a Future representing pending completion of the operation.
     * @throws CacheException On error.
     */
    public IgniteFuture<Long> sizeLongAsync(int partition, CachePeekMode... peekModes) throws CacheException;

    /**
     * Gets the number of all entries cached on this node. By default, if {@code peekModes} value isn't defined,
     * only size of primary copies will be returned. This behavior is identical to calling this method with
     * {@link CachePeekMode#PRIMARY} peek mode.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size on this node.
     */
    public int localSize(CachePeekMode... peekModes);

    /**
     * Gets the number of all entries cached on this node as a long value. By default, if {@code peekModes} value isn't
     * defined, only size of primary copies will be returned. This behavior is identical to calling this method with
     * {@link CachePeekMode#PRIMARY} peek mode.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size on this node.
     */
    public long localSizeLong(CachePeekMode... peekModes);

    /**
     * Gets the number of all entries cached on this node for the partition as a long value. By default, if {@code peekModes} value isn't
     * defined, only size of primary copies will be returned. This behavior is identical to calling this method with
     * {@link CachePeekMode#PRIMARY} peek mode.
     *
     * @param partition partition.
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size on this node.
     */
    public long localSizeLong(int partition, CachePeekMode... peekModes);

    /**
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @return The map of {@link EntryProcessorResult}s of the processing per key,
     * if any, defined by the {@link EntryProcessor} implementation.  No mappings
     * will be returned for {@link EntryProcessor}s that return a
     * <code>null</code> value for a key.
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map,
        Object... args) throws TransactionException;

    /**
     * Asynchronously version of the {@link #invokeAll(Set, EntryProcessor, Object...)} method.
     *
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @return a Future representing pending completion of the operation. See more about future result
     * at the {@link #invokeAll(Map, Object...)}.
     * @throws TransactionException If operation within transaction is failed.
     */
    public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(
        Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public V get(K key) throws TransactionException;

    /**
     * Asynchronously gets an entry from the cache.
     * <p>
     * If the cache is configured to use read-through, and a future result would be null
     * because the entry is missing from the cache, the Cache's {@link CacheLoader}
     * is called in an attempt to load the entry.
     *
     * @param key Key.
     * @return a Future representing pending completion of the operation.
     */
    public IgniteFuture<V> getAsync(K key);

    /**
     * Gets an entry from the cache.
     * <p>
     * If the cache is configured to use read-through, and get would return null
     * because the entry is missing from the cache, the Cache's {@link CacheLoader}
     * is called in an attempt to load the entry.
     *
     * @param key The key whose associated value is to be returned.
     * @return The element, or null, if it does not exist.
     * @throws IllegalStateException If the cache is {@link #isClosed()}.
     * @throws NullPointerException If the key is {@code null}.
     * @throws CacheException If there is a problem fetching the value.
     * @throws ClassCastException If the implementation is configured to perform
     * runtime-type-checking, and the key or value types are incompatible with those that have been
     * configured for the {@link Cache}.
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    public CacheEntry<K, V> getEntry(K key) throws TransactionException;

    /**
     * Asynchronously gets an entry from the cache.
     * <p>
     * If the cache is configured to use read-through, and a future result would be null
     * because the entry is missing from the cache, the Cache's {@link CacheLoader}
     * is called in an attempt to load the entry.
     *
     * @param key The key whose associated value is to be returned.
     * @return a Future representing pending completion of the operation.
     * @throws IllegalStateException If the cache is {@link #isClosed()}.
     * @throws NullPointerException If the key is {@code null}.
     * @throws CacheException If there is a problem fetching the value.
     * @throws ClassCastException If the implementation is configured to perform
     * runtime-type-checking, and the key or value types are incompatible with those that have been
     * configured for the {@link Cache}.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<CacheEntry<K, V>> getEntryAsync(K key) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public Map<K, V> getAll(Set<? extends K> keys) throws TransactionException;

    /**
     * Asynchronously gets a collection of entries from the {@link Cache}, returning them as
     * {@link Map} of the values associated with the set of keys requested.
     * <p>
     * If the cache is configured read-through, and a future result for a key would
     * be null because an entry is missing from the cache, the Cache's
     * {@link CacheLoader} is called in an attempt to load the entry. If an
     * entry cannot be loaded for a given key, the key will not be present in
     * the returned Map.
     *
     * @param keys Keys set.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Map<K, V>> getAllAsync(Set<? extends K> keys) throws TransactionException;

    /**
     * Gets a collection of entries from the {@link Cache}.
     * <p>
     * If the cache is configured read-through, and a get for a key would
     * return null because an entry is missing from the cache, the Cache's
     * {@link CacheLoader} is called in an attempt to load the entry. If an
     * entry cannot be loaded for a given key, the key will not be present in
     * the returned Collection.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return A collection of entries that were found for the given keys. Entries not found
     *         in the cache are not in the returned collection.
     * @throws NullPointerException If keys is null or if keys contains a {@code null}.
     * @throws IllegalStateException If the cache is {@link #isClosed()}.
     * @throws CacheException If there is a problem fetching the values.
     * @throws ClassCastException If the implementation is configured to perform
     * runtime-type-checking, and the key or value types are incompatible with those that have been
     * configured for the {@link Cache}.
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    public Collection<CacheEntry<K, V>> getEntries(Set<? extends K> keys) throws TransactionException;

    /**
     * Asynchronously gets a collection of entries from the {@link Cache}.
     * <p>
     * If the cache is configured read-through, and a future result for a key would
     * be null because an entry is missing from the cache, the Cache's
     * {@link CacheLoader} is called in an attempt to load the entry. If an
     * entry cannot be loaded for a given key, the key will not be present in
     * the returned Collection.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return a Future representing pending completion of the operation.
     * @throws NullPointerException If keys is null or if keys contains a {@code null}.
     * @throws IllegalStateException If the cache is {@link #isClosed()}.
     * @throws CacheException If there is a problem fetching the values.
     * @throws ClassCastException If the implementation is configured to perform
     * runtime-type-checking, and the key or value types are incompatible with those that have been
     * configured for the {@link Cache}.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Collection<CacheEntry<K, V>>> getEntriesAsync(Set<? extends K> keys) throws TransactionException;

    /**
     * Gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return A map of entries that were found for the given keys.
     */
    @IgniteAsyncSupported
    public Map<K, V> getAllOutTx(Set<? extends K> keys);

    /**
     * Asynchronously gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return a Future representing pending completion of the operation.
     */
    public IgniteFuture<Map<K, V>> getAllOutTxAsync(Set<? extends K> keys);

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public boolean containsKey(K key) throws TransactionException;

    /**
     * Asynchronously determines if the {@link Cache} contains an entry for the specified key.
     * <p>
     * More formally, future result is <tt>true</tt> if and only if this cache contains a
     * mapping for a key <tt>k</tt> such that <tt>key.equals(k)</tt>.
     * (There can be at most one such mapping.)
     *
     * @param key Key.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Boolean> containsKeyAsync(K key) throws TransactionException;

    /**
     * Determines if the {@link Cache} contains entries for the specified keys.
     *
     * @param keys Key whose presence in this cache is to be tested.
     * @return {@code True} if this cache contains a mapping for the specified keys.
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    public boolean containsKeys(Set<? extends K> keys) throws TransactionException;

    /**
     * Asynchronously determines if the {@link Cache} contains entries for the specified keys.
     *
     * @param keys Key whose presence in this cache is to be tested.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Boolean> containsKeysAsync(Set<? extends K> keys) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public void put(K key, V val) throws TransactionException;

    /**
     * Asynchronously associates the specified value with the specified key in the cache.
     * <p>
     * If the {@link Cache} previously contained a mapping for the key, the old
     * value is replaced by the specified value.  (A cache <tt>c</tt> is said to
     * contain a mapping for a key <tt>k</tt> if and only if {@link
     * #containsKey(Object) c.containsKey(k)} would return <tt>true</tt>.)
     *
     * @param key Key.
     * @param val Value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Void> putAsync(K key, V val) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public V getAndPut(K key, V val) throws TransactionException;

    /**
     * Asynchronously associates the specified value with the specified key in this cache,
     * returning an existing value if one existed as the future result.
     * <p>
     * If the cache previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A cache
     * <tt>c</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) c.containsKey(k)} would return
     * <tt>true</tt>.)
     * <p>
     * The previous value is returned as the future result, or future result is null if there was no value associated
     * with the key previously.
     *
     * @param key Key.
     * @param val Value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<V> getAndPutAsync(K key, V val) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public void putAll(Map<? extends K, ? extends V> map) throws TransactionException;

    /**
     * Asynchronously copies all of the entries from the specified map to the {@link Cache}.
     * <p>
     * The effect of this call is equivalent to that of calling
     * {@link #putAsync(Object, Object)}  putAsync(k, v)} on this cache once for each mapping
     * from key <tt>k</tt> to value <tt>v</tt> in the specified map.
     * <p>
     * The order in which the individual puts occur is undefined.
     * <p>
     * The behavior of this operation is undefined if entries in the cache
     * corresponding to entries in the map are modified or removed while this
     * operation is in progress. or if map is modified while the operation is in
     * progress.
     * <p>
     * In Default Consistency mode, individual puts occur atomically but not
     * the entire putAll.  Listeners may observe individual updates.
     *
     * @param map Map containing keys and values to put into the cache.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public boolean putIfAbsent(K key, V val) throws TransactionException;

    /**
     * Asynchronously associates the specified key with the given value if it is
     * not already associated with a value.
     *
     * @param key Key.
     * @param val Value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionTimeoutException If operation performs within transaction and timeout occurred.
     * @throws TransactionRollbackException If operation performs within transaction that automatically rolled back.
     * @throws TransactionHeuristicException If operation performs within transaction that entered an unknown state.
     */
    public IgniteFuture<Boolean> putIfAbsentAsync(K key, V val);

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public boolean remove(K key) throws TransactionException;

    /**
     * Asynchronously removes the mapping for a key from this cache if it is present.
     * <p>
     * More formally, if this cache contains a mapping from key <tt>k</tt> to
     * value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping is removed.
     * (The cache can contain at most one such mapping.)
     *
     * <p>A future result is <tt>true</tt> if this cache previously associated the key,
     * or <tt>false</tt> if the cache contained no mapping for the key.
     * <p>
     * The cache will not contain a mapping for the specified key once the
     * returned future is completed.
     *
     * @param key Key.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Boolean> removeAsync(K key) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public boolean remove(K key, V oldVal) throws TransactionException;

    /**
     * Asynchronously removes the mapping for a key only if currently mapped to the
     * given value.
     *
     * @param key Key.
     * @param oldVal Old value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Boolean> removeAsync(K key, V oldVal) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public V getAndRemove(K key) throws TransactionException;

    /**
     * Asynchronously removes the entry for a key only if currently mapped to some
     * value.
     *
     * @param key Key.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<V> getAndRemoveAsync(K key) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public boolean replace(K key, V oldVal, V newVal) throws TransactionException;

    /**
     * Asynchronous version of the {@link #replace(Object, Object, Object)}.
     *
     * @param key Key.
     * @param oldVal Old value.
     * @param newVal New value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Boolean> replaceAsync(K key, V oldVal, V newVal) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public boolean replace(K key, V val) throws TransactionException;

    /**
     * Asynchronously replaces the entry for a key only if currently mapped to a
     * given value.
     *
     * @param key Key.
     * @param val Value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Boolean> replaceAsync(K key, V val) throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public V getAndReplace(K key, V val) throws TransactionException;

    /**
     * Asynchronously replaces the value for a given key if and only if there is a
     * value currently mapped by the key.
     *
     * @param key Key.
     * @param val Value.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionTimeoutException If operation performs within transaction and timeout occurred.
     * @throws TransactionRollbackException If operation performs within transaction that automatically rolled back.
     * @throws TransactionHeuristicException If operation performs within transaction that entered an unknown state.
     */
    public IgniteFuture<V> getAndReplaceAsync(K key, V val);

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public void removeAll(Set<? extends K> keys) throws TransactionException;

    /**
     * Asynchronously removes entries for the specified keys.
     * <p>
     * The order in which the individual entries are removed is undefined.
     * <p>
     * For every entry in the key set, the following are called:
     * <ul>
     *   <li>any registered {@link CacheEntryRemovedListener}s</li>
     *   <li>if the cache is a write-through cache, the {@link CacheWriter}</li>
     * </ul>
     * If the key set is empty, the {@link CacheWriter} is not called.
     *
     * @param keys Keys set.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public IgniteFuture<Void> removeAllAsync(Set<? extends K> keys) throws TransactionException;

    /**
     * Removes all of the mappings from this cache.
     * <p>
     * The order that the individual entries are removed is undefined.
     * <p>
     * For every mapping that exists the following are called:
     * <ul>
     *   <li>any registered {@link CacheEntryRemovedListener}s</li>
     *   <li>if the cache is a write-through cache, the {@link CacheWriter}</li>
     * </ul>
     * If the cache is empty, the {@link CacheWriter} is not called.
     * <p>
     * This operation is not transactional. It calls broadcast closure that
     * deletes all primary keys from remote nodes.
     * <p>
     * This is potentially an expensive operation as listeners are invoked.
     * Use {@link #clear()} to avoid this.
     *
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @throws CacheException        if there is a problem during the remove
     * @see #clear()
     * @see CacheWriter#deleteAll
     */
    @IgniteAsyncSupported
    @Override public void removeAll();

    /**
     * Asynchronously removes all of the mappings from this cache.
     * <p>
     * The order that the individual entries are removed is undefined.
     * <p>
     * For every mapping that exists the following are called:
     * <ul>
     *   <li>any registered {@link CacheEntryRemovedListener}s</li>
     *   <li>if the cache is a write-through cache, the {@link CacheWriter}</li>
     * </ul>
     * If the cache is empty, the {@link CacheWriter} is not called.
     * <p>
     * This is potentially an expensive operation as listeners are invoked.
     * Use {@link #clearAsync()} to avoid this.
     *
     * @return a Future representing pending completion of the operation.
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @throws CacheException        if there is a problem during the remove
     * @see #clearAsync()
     * @see CacheWriter#deleteAll
     */
    public IgniteFuture<Void> removeAllAsync();

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void clear();

    /**
     * Asynchronously clears the contents of the cache, without notifying listeners or
     * {@link CacheWriter}s.
     *
     * @return a Future representing pending completion of the operation.
     */
    public IgniteFuture<Void> clearAsync();

    /**
     * Clears entry from the cache and swap storage, without notifying listeners or
     * {@link CacheWriter}s. Entry is cleared only if it is not currently locked,
     * and is not participating in a transaction.
     *
     * @param key Key to clear.
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @throws CacheException        if there is a problem during the clear
     */
    @IgniteAsyncSupported
    public void clear(K key);

    /**
     * Asynchronously clears entry from the cache and swap storage, without notifying listeners or
     * {@link CacheWriter}s. Entry is cleared only if it is not currently locked,
     * and is not participating in a transaction.
     *
     * @param key Key to clear.
     * @return a Future representing pending completion of the operation.
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @throws CacheException        if there is a problem during the clear
     */
    public IgniteFuture<Void> clearAsync(K key);

    /**
     * Clears entries from the cache and swap storage, without notifying listeners or
     * {@link CacheWriter}s. Entry is cleared only if it is not currently locked,
     * and is not participating in a transaction.
     *
     * @param keys Keys to clear.
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @throws CacheException        if there is a problem during the clear
     */
    @IgniteAsyncSupported
    public void clearAll(Set<? extends K> keys);

    /**
     * Asynchronously clears entries from the cache and swap storage, without notifying listeners or
     * {@link CacheWriter}s. Entry is cleared only if it is not currently locked,
     * and is not participating in a transaction.
     *
     * @param keys Keys to clear.
     * @return a Future representing pending completion of the operation.
     * @throws IllegalStateException if the cache is {@link #isClosed()}
     * @throws CacheException        if there is a problem during the clear
     */
    public IgniteFuture<Void> clearAllAsync(Set<? extends K> keys);

    /**
     * Clears entry from the cache and swap storage, without notifying listeners or
     * {@link CacheWriter}s. Entry is cleared only if it is not currently locked,
     * and is not participating in a transaction.
     * <p/>
     * Note that this operation is local as it merely clears
     * an entry from local cache, it does not remove entries from
     * remote caches.
     *
     * @param key Key to clear.
     */
    public void localClear(K key);

    /**
     * Clears entries from the cache and swap storage, without notifying listeners or
     * {@link CacheWriter}s. Entry is cleared only if it is not currently locked,
     * and is not participating in a transaction.
     * <p/>
     * Note that this operation is local as it merely clears
     * an entry from local cache, it does not remove entries from
     * remote caches.
     *
     * @param keys Keys to clear.
     */
    public void localClearAll(Set<? extends K> keys);

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws TransactionException;

    /**
     * Asynchronously invokes an {@link EntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     *
     * @param key The key to the entry.
     * @param entryProcessor The {@link EntryProcessor} to invoke.
     * @param arguments Additional arguments to pass to the {@link EntryProcessor}.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public <T> IgniteFuture<T> invokeAsync(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
		    throws TransactionException;

    /**
     * Invokes an {@link CacheEntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     * <p>
     * An instance of entry processor must be stateless as it may be invoked multiple times on primary and
     * backup nodes in the cache. It is guaranteed that the value passed to the entry processor will be always
     * the same.
     *
     * @param key The key to the entry.
     * @param entryProcessor The {@link CacheEntryProcessor} to invoke.
     * @param arguments Additional arguments to pass to the {@link CacheEntryProcessor}.
     * @return The result of the processing, if any, defined by the {@link CacheEntryProcessor} implementation.
     * @throws NullPointerException If key or {@link CacheEntryProcessor} is null
     * @throws IllegalStateException If the cache is {@link #isClosed()}
     * @throws ClassCastException If the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}.
     * @throws EntryProcessorException If an exception is thrown by the {@link
     *                                 CacheEntryProcessor}, a Caching Implementation
     *                                 must wrap any {@link Exception} thrown
     *                                 wrapped in an {@link EntryProcessorException}.
     * @throws TransactionException If operation within transaction is failed.
     * @see CacheEntryProcessor
     */
    @IgniteAsyncSupported
    public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... arguments)
		    throws TransactionException;

    /**
     * Asynchronously invokes an {@link CacheEntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     * <p>
     * An instance of entry processor must be stateless as it may be invoked multiple times on primary and
     * backup nodes in the cache. It is guaranteed that the value passed to the entry processor will be always
     * the same.
     *
     * @param key The key to the entry.
     * @param entryProcessor The {@link CacheEntryProcessor} to invoke.
     * @param arguments Additional arguments to pass to the {@link CacheEntryProcessor}.
     * @return a Future representing pending completion of the operation.
     * @throws NullPointerException If key or {@link CacheEntryProcessor} is null
     * @throws IllegalStateException If the cache is {@link #isClosed()}
     * @throws ClassCastException If the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}.
     * @throws EntryProcessorException If an exception is thrown by the {@link
     *                                 CacheEntryProcessor}, a Caching Implementation
     *                                 must wrap any {@link Exception} thrown
     *                                 wrapped in an {@link EntryProcessorException}.
     * @throws TransactionException If operation within transaction is failed.
     * @see CacheEntryProcessor
     */
    public <T> IgniteFuture<T> invokeAsync(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... arguments)
		    throws TransactionException;

    /**
     * {@inheritDoc}
     * @throws TransactionException If operation within transaction is failed.
     */
    @IgniteAsyncSupported
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException;

    /**
     * Asynchronously invokes an {@link EntryProcessor} against the set of {@link javax.cache.Cache.Entry}s
     * specified by the set of keys.
     * <p>
     * If an {@link javax.cache.Cache.Entry} does not exist for the specified key, an attempt is made
     * to load it (if a loader is configured) or a surrogate {@link javax.cache.Cache.Entry},
     * consisting of the key and a value of null is provided.
     * <p>
     * The order that the entries for the keys are processed is undefined.
     * Implementations may choose to process the entries in any order, including
     * concurrently.  Furthermore there is no guarantee implementations will
     * use the same {@link EntryProcessor} instance to process each entry, as
     * the case may be in a non-local cache topology.
     * <p>
     * The result of executing the {@link EntryProcessor} is returned in the future as a
     * {@link Map} of {@link EntryProcessorResult}s, one result per key.  Should the
     * {@link EntryProcessor} or Caching implementation throw an exception, the
     * exception is wrapped and re-thrown when a call to
     * {@link javax.cache.processor.EntryProcessorResult#get()} is made.

     *
     * @param keys The set of keys.
     * @param entryProcessor The {@link EntryProcessor} to invoke.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @return a Future representing pending completion of the operation.
     * @throws TransactionException If operation within transaction is failed.
     */
    public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException;


    /**
     * Invokes an {@link CacheEntryProcessor} against the set of {@link javax.cache.Cache.Entry}s
     * specified by the set of keys.
     * <p>
     * If an {@link javax.cache.Cache.Entry} does not exist for the specified key, an attempt is made
     * to load it (if a loader is configured) or a surrogate {@link javax.cache.Cache.Entry},
     * consisting of the key and a value of null is provided.
     * <p>
     * The order that the entries for the keys are processed is undefined.
     * Implementations may choose to process the entries in any order, including
     * concurrently.  Furthermore there is no guarantee implementations will
     * use the same {@link CacheEntryProcessor} instance to process each entry, as
     * the case may be in a non-local cache topology.
     * <p>
     * The result of executing the {@link CacheEntryProcessor} is returned as a
     * {@link Map} of {@link EntryProcessorResult}s, one result per key.  Should the
     * {@link CacheEntryProcessor} or Caching implementation throw an exception, the
     * exception is wrapped and re-thrown when a call to
     * {@link javax.cache.processor.EntryProcessorResult#get()} is made.
     * <p>
     * An instance of entry processor must be stateless as it may be invoked multiple times on primary and
     * backup nodes in the cache. It is guaranteed that the value passed to the entry processor will be always
     * the same.
     *
     * @param keys The set of keys for entries to process.
     * @param entryProcessor The {@link CacheEntryProcessor} to invoke.
     * @param args Additional arguments to pass to the {@link CacheEntryProcessor}.
     * @return The map of {@link EntryProcessorResult}s of the processing per key,
     * if any, defined by the {@link CacheEntryProcessor} implementation.  No mappings
     * will be returned for {@link CacheEntryProcessor}s that return a
     * <code>null</code> value for a key.
     * @throws NullPointerException If keys or {@link CacheEntryProcessor} are {#code null}.
     * @throws IllegalStateException If the cache is {@link #isClosed()}.
     * @throws ClassCastException If the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}.
     * @throws TransactionException If operation within transaction is failed.
     * @see CacheEntryProcessor
     */
    @IgniteAsyncSupported
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException;

    /**
     * Asynchronously invokes an {@link CacheEntryProcessor} against the set of {@link javax.cache.Cache.Entry}s
     * specified by the set of keys.
     * <p>
     * If an {@link javax.cache.Cache.Entry} does not exist for the specified key, an attempt is made
     * to load it (if a loader is configured) or a surrogate {@link javax.cache.Cache.Entry},
     * consisting of the key and a value of null is provided.
     * <p>
     * The order that the entries for the keys are processed is undefined.
     * Implementations may choose to process the entries in any order, including
     * concurrently.  Furthermore there is no guarantee implementations will
     * use the same {@link CacheEntryProcessor} instance to process each entry, as
     * the case may be in a non-local cache topology.
     * <p>
     * The result of executing the {@link CacheEntryProcessor} is returned in the future as a
     * {@link Map} of {@link EntryProcessorResult}s, one result per key.  Should the
     * {@link CacheEntryProcessor} or Caching implementation throw an exception, the
     * exception is wrapped and re-thrown when a call to
     * {@link javax.cache.processor.EntryProcessorResult#get()} is made.
     * <p>
     * An instance of entry processor must be stateless as it may be invoked multiple times on primary and
     * backup nodes in the cache. It is guaranteed that the value passed to the entry processor will be always
     * the same.
     *
     * @param keys The set of keys for entries to process.
     * @param entryProcessor The {@link CacheEntryProcessor} to invoke.
     * @param args Additional arguments to pass to the {@link CacheEntryProcessor}.
     * @return a Future representing pending completion of the operation.
     * @throws NullPointerException If keys or {@link CacheEntryProcessor} are {#code null}.
     * @throws IllegalStateException If the cache is {@link #isClosed()}.
     * @throws ClassCastException If the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}.
     * @throws TransactionException If operation within transaction is failed.
     * @see CacheEntryProcessor
     */
    public <T> IgniteFuture<Map<K, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args) throws TransactionException;

    /**
     * Closes this cache instance.
     * <p>
     * For local cache equivalent to {@link #destroy()}.
     * For distributed caches, if called on clients, stops client cache, if called on a server node,
     * just closes this cache instance and does not destroy cache data.
     * <p>
     * After cache instance is closed another {@code IgniteCache} instance for the same
     * cache can be created using {@link Ignite#cache(String)} method.
     */
    @Override public void close();

    /**
     * Completely deletes the cache with all its data from the system on all cluster nodes.
     */
    public void destroy();

    /**
     * This cache node to re-balance its partitions. This method is usually used when
     * {@link CacheConfiguration#getRebalanceDelay()} configuration parameter has non-zero value.
     * When many nodes are started or stopped almost concurrently, it is more efficient to delay
     * rebalancing until the node topology is stable to make sure that no redundant re-partitioning
     * happens.
     * <p>
     * In case of{@link CacheMode#PARTITIONED} caches, for better efficiency user should
     * usually make sure that new nodes get placed on the same place of consistent hash ring as
     * the left nodes, and that nodes are restarted before
     * {@link CacheConfiguration#getRebalanceDelay() rebalanceDelay} expires. To place nodes
     * on the same place in consistent hash ring, use
     * {@link IgniteConfiguration#setConsistentId(Serializable)} to make sure that
     * a node maps to the same hash ID if re-started.
     * <p>
     * See {@link CacheConfiguration#getRebalanceDelay()} for more information on how to configure
     * rebalance re-partition delay.
     * <p>
     * @return Future that will be completed when rebalancing is finished. Future.get() returns true
     *      when rebalance was successfully finished.
     */
    public IgniteFuture<?> rebalance();

    /**
     * Returns future that will be completed when all indexes for this cache are ready to use.
     *
     * @return Future.
     */
    public IgniteFuture<?> indexReadyFuture();

    /**
     * Gets whole cluster snapshot metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetrics metrics();

    /**
     * Gets cluster group snapshot metrics for caches in cluster group.
     *
     * @param grp Cluster group.
     * @return Cache metrics.
     */
    public CacheMetrics metrics(ClusterGroup grp);

    /**
     * Gets local snapshot metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetrics localMetrics();

    /**
     * Gets whole cluster MxBean for this cache.
     *
     * @return MxBean.
     */
    public CacheMetricsMXBean mxBean();

    /**
     * Gets local MxBean for this cache.
     *
     * @return MxBean.
     */
    public CacheMetricsMXBean localMxBean();

    /**
     * Gets a collection of lost partition IDs.
     *
     * @return Lost paritions.
     */
    public Collection<Integer> lostPartitions();
}
