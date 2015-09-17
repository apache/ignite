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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Configuration;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheWriter;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SpiQuery;
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
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
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
 * <h1 class="header">Asynchronous Mode</h1>
 * Cache API supports asynchronous mode via {@link IgniteAsyncSupport} functionality. To turn on
 * asynchronous mode invoke {@link #withAsync()} method. Once asynchronous mode is enabled,
 * all methods with {@link IgniteAsyncSupported @IgniteAsyncSupported} annotation will be executed
 * asynchronously.
 *
 * @param <K> Cache key type.
 * @param <V> Cache value type.
 */
public interface IgniteCache<K, V> extends javax.cache.Cache<K, V>, IgniteAsyncSupport {
    /** {@inheritDoc} */
    @Override public IgniteCache<K, V> withAsync();

    /** {@inheritDoc} */
    @Override public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz);

    /**
     * Gets a random entry out of cache. In the worst cache scenario this method
     * has complexity of <pre>O(S * N/64)</pre> where {@code N} is the size of internal hash
     * table and {@code S} is the number of hash table buckets to sample, which is {@code 5}
     * by default. However, if the table is pretty dense, with density factor of {@code N/64},
     * which is true for near fully populated caches, this method will generally perform significantly
     * faster with complexity of O(S) where {@code S = 5}.
     *
     * @return Random entry, or {@code null} if cache is empty.
     */
    public Entry<K, V> randomEntry();

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
     */
    @IgniteAsyncSupported
    public V getAndPutIfAbsent(K key, V val) throws CacheException;

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
     *
     * @param qry Query.
     * @return Cursor.
     * @see ScanQuery
     * @see SqlQuery
     * @see TextQuery
     * @see SpiQuery
     */
    public <R> QueryCursor<R> query(Query<R> qry);

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
     * Attempts to evict all entries associated with keys. Note,
     * that entry will be evicted only if it's not used (not
     * participating in any locks or transactions).
     *
     * @param keys Keys to evict.
     */
    public void localEvict(Collection<? extends K> keys);

    /**
     * Peeks at in-memory cached value using default optinal peek mode.
     * <p>
     * This method will not load value from any persistent store or from a remote node.
     * <h2 class="header">Transactions</h2>
     * This method does not participate in any transactions.
     *
     * @param key Entry key.
     * @return Peeked value, or {@code null} if not found.
     * @throws NullPointerException If key is {@code null}.
     */
    public V localPeek(K key, CachePeekMode... peekModes);

    /**
     * This method unswaps cache entries by given keys, if any, from swap storage
     * into memory.
     * <h2 class="header">Transactions</h2>
     * This method is not transactional.
     *
     * @param keys Keys to promote entries for.
     * @throws CacheException If promote failed.
     */
    public void localPromote(Set<? extends K> keys) throws CacheException;

    /**
     * Gets the number of all entries cached across all nodes. By default, if {@code peekModes} value isn't defined,
     * only size of primary copies across all nodes will be returned. This behavior is identical to calling
     * this method with {@link CachePeekMode#PRIMARY} peek mode.
     * <p>
     * NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
     *
     * @param peekModes Optional peek modes. If not provided, then total cache size is returned.
     * @return Cache size across all nodes.
     */
    @IgniteAsyncSupported
    public int size(CachePeekMode... peekModes) throws CacheException;

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
     * @param map Map containing keys and entry processors to be applied to values.
     * @param args Additional arguments to pass to the {@link EntryProcessor}.
     * @return The map of {@link EntryProcessorResult}s of the processing per key,
     * if any, defined by the {@link EntryProcessor} implementation.  No mappings
     * will be returned for {@link EntryProcessor}s that return a
     * <code>null</code> value for a key.
     */
    @IgniteAsyncSupported
    <T> Map<K, EntryProcessorResult<T>> invokeAll(Map<? extends K, ? extends EntryProcessor<K, V, T>> map, Object... args);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V get(K key);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public Map<K, V> getAll(Set<? extends K> keys);

    /**
     * Gets values from cache. Will bypass started transaction, if any, i.e. will not enlist entries
     * and will not lock any keys if pessimistic transaction is started by thread.
     *
     * @param keys The keys whose associated values are to be returned.
     * @return A map of entries that were found for the given keys.
     */
    @IgniteAsyncSupported
    public Map<K, V> getAllOutTx(Set<? extends K> keys);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean containsKey(K key);

    /**
     * Determines if the {@link Cache} contains entries for the specified keys.
     *
     * @param keys Key whose presence in this cache is to be tested.
     * @return {@code True} if this cache contains a mapping for the specified keys.
     */
    @IgniteAsyncSupported
    public boolean containsKeys(Set<? extends K> keys);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void put(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V getAndPut(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void putAll(Map<? extends K, ? extends V> map);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean putIfAbsent(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean remove(K key);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean remove(K key, V oldVal);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V getAndRemove(K key);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean replace(K key, V oldVal, V newVal);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public boolean replace(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public V getAndReplace(K key, V val);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void removeAll(Set<? extends K> keys);

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

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public void clear();

    /**
     * Clear entry from the cache and swap storage, without notifying listeners or
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
     * Clear entries from the cache and swap storage, without notifying listeners or
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
     * Clear entry from the cache and swap storage, without notifying listeners or
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
     * Clear entries from the cache and swap storage, without notifying listeners or
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

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments);

    /**
     * Invokes an {@link CacheEntryProcessor} against the {@link javax.cache.Cache.Entry} specified by
     * the provided key. If an {@link javax.cache.Cache.Entry} does not exist for the specified key,
     * an attempt is made to load it (if a loader is configured) or a surrogate
     * {@link javax.cache.Cache.Entry}, consisting of the key with a null value is used instead.
     * This method different
     * <p>
     *
     * @param key            the key to the entry
     * @param entryProcessor the {@link CacheEntryProcessor} to invoke
     * @param arguments      additional arguments to pass to the
     *                       {@link CacheEntryProcessor}
     * @return the result of the processing, if any, defined by the
     *         {@link CacheEntryProcessor} implementation
     * @throws NullPointerException    if key or {@link CacheEntryProcessor} is null
     * @throws IllegalStateException   if the cache is {@link #isClosed()}
     * @throws ClassCastException    if the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}
     * @throws EntryProcessorException if an exception is thrown by the {@link
     *                                 CacheEntryProcessor}, a Caching Implementation
     *                                 must wrap any {@link Exception} thrown
     *                                 wrapped in an {@link EntryProcessorException}.
     * @see CacheEntryProcessor
     */
    @IgniteAsyncSupported
    public <T> T invoke(K key, CacheEntryProcessor<K, V, T> entryProcessor, Object... arguments);

    /** {@inheritDoc} */
    @IgniteAsyncSupported
    @Override public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        EntryProcessor<K, V, T> entryProcessor, Object... args);

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
     *
     * @param keys           the set of keys for entries to process
     * @param entryProcessor the {@link CacheEntryProcessor} to invoke
     * @param args      additional arguments to pass to the
     *                       {@link CacheEntryProcessor}
     * @return the map of {@link EntryProcessorResult}s of the processing per key,
     * if any, defined by the {@link CacheEntryProcessor} implementation.  No mappings
     * will be returned for {@link CacheEntryProcessor}s that return a
     * <code>null</code> value for a key.
     * @throws NullPointerException    if keys or {@link CacheEntryProcessor} are null
     * @throws IllegalStateException   if the cache is {@link #isClosed()}
     * @throws ClassCastException    if the implementation is configured to perform
     *                               runtime-type-checking, and the key or value
     *                               types are incompatible with those that have been
     *                               configured for the {@link Cache}
     * @see CacheEntryProcessor
     */
    @IgniteAsyncSupported
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys,
        CacheEntryProcessor<K, V, T> entryProcessor, Object... args);

    /**
     * Closes this cache instance.
     * <p>
     * For local cache equivalent to {@link #destroy()}.
     * For distributed caches, if called on clients, stops client cache, if called on a server node,
     * just closes this cache instance and does not destroy cache data.
     * <p>
     * After cache instance is closed another {@link IgniteCache} instance for the same
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
     * @return Future that will be completed when rebalancing is finished.
     */
    public IgniteFuture<?> rebalance();

    /**
     * Gets snapshot metrics (statistics) for this cache.
     *
     * @return Cache metrics.
     */
    public CacheMetrics metrics();

    /**
     * Gets snapshot metrics for caches in cluster group.
     *
     * @param grp Cluster group.
     * @return Cache metrics.
     */
    public CacheMetrics metrics(ClusterGroup grp);

    /**
     * Gets MxBean for this cache.
     *
     * @return MxBean.
     */
    public CacheMetricsMXBean mxBean();
}
